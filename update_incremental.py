# update_incremental.py
# Incremental updater for the MySQL DB using timestamped CSV snapshots.
# - Monitors input folders for new CSVs (i.e., files present in the folders)
# - Applies consistent transforms (renames, normalization) used by the main ETL
# - Stages the new data into __stg tables
# - Upserts only changed rows and updates last_updated to the current run timestamp
# - Deletes rows missing from the new CSV (treat CSVs as full snapshots)
# - Archives processed CSVs into archive/<YYYY-MM-DD_HHMM>/... (same structure)
#
# Note on staff keys:
# - Staff uniqueness is assumed on the composite key:
#   (staff_first_name, staff_last_name, phone, email)
# - We match and upsert using this natural key.

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from pathlib import Path
from datetime import datetime, timezone
import shutil

import pandas as pd
from sqlalchemy import create_engine, text, types, inspect


#load user and password from file
with open("db_credentials.txt", "r") as f:
    lines = f.readlines()
    db_user = lines[0].strip()
    db_password = lines[1].strip()

# -------------------- CONNECTION/CONSTANTS --------------------

#use the loaded user and password to connect to the mysql database
engine = create_engine(
    "mysql+pymysql://"+db_user+":"+db_password+"@127.0.0.1:3307/mydb",
    pool_pre_ping=True,
)

VARCHAR_LEN = 191  # safer for utf8mb4 indexed columns in MySQL
BASE_ROOT = Path("data_setup") / "Data CSV"
API_ROOT = Path("csvs_from_api")
ARCHIVE_ROOT = Path("archive")

# -------------------- DATASET CONFIG --------------------

# Source file locations (monitored)
SOURCE_FILES: Dict[str, Path] = {
    "brands": BASE_ROOT / "brands.csv",
    "categories": BASE_ROOT / "categories.csv",
    "products": BASE_ROOT / "products.csv",
    "staff": BASE_ROOT / "staffs.csv",  # file is staffs.csv -> becomes 'staff'
    "stocks": BASE_ROOT / "stocks.csv",
    "stores": BASE_ROOT / "stores.csv",
    "customers": API_ROOT / "customers.csv",
    "order_items": API_ROOT / "order_items.csv",
    "orders": API_ROOT / "orders.csv",
}

# Table names per dataset
TABLES: Dict[str, str] = {
    "brands": "brands",
    "categories": "categories",
    "products": "products",
    "customers": "customers",
    "stores": "stores",
    "staff": "staff",
    "orders": "orders",
    "order_items": "order_items",
    "stocks": "stocks",
}

# Keys used to match rows for upserts/deletes
# Note: staff uses the requested composite natural key.
KEYS: Dict[str, List[str]] = {
    "brands": ["brand_id"],
    "categories": ["category_id"],
    "products": ["product_id"],
    "customers": ["customer_id"],
    "stores": ["store_name"],
    "staff": ["staff_first_name", "staff_last_name", "phone", "email"],
    "orders": ["order_id"],
    "order_items": ["order_id", "item_id"],
    "stocks": ["store_name", "product_id"],
}

# DTypes for staging tables (and reference for column ordering)
DTYPES: Dict[str, Dict[str, Any]] = {
    "brands": {
        "brand_id": types.Integer(),
        "brand_name": types.String(VARCHAR_LEN),
        "last_updated": types.DateTime(),
    },
    "categories": {
        "category_id": types.Integer(),
        "category_name": types.String(VARCHAR_LEN),
        "last_updated": types.DateTime(),
    },
    "products": {
        "product_id": types.Integer(),
        "product_name": types.String(VARCHAR_LEN),
        "brand_id": types.Integer(),
        "category_id": types.Integer(),
        "model_year": types.Integer(),
        "listed_price": types.Float(),
        "last_updated": types.DateTime(),
    },
    "customers": {
        "customer_id": types.Integer(),
        "first_name": types.String(VARCHAR_LEN),
        "last_name": types.String(VARCHAR_LEN),
        "phone": types.String(50),
        "email": types.String(VARCHAR_LEN),
        "street": types.String(VARCHAR_LEN),
        "city": types.String(VARCHAR_LEN),
        "state": types.String(50),
        "zip_code": types.String(20),
        "last_updated": types.DateTime(),
    },
    "stores": {
        "store_name": types.String(VARCHAR_LEN),
        "phone": types.String(50),
        "email": types.String(VARCHAR_LEN),
        "street": types.String(VARCHAR_LEN),
        "city": types.String(VARCHAR_LEN),
        "state": types.String(50),
        "zip_code": types.String(20),
        "last_updated": types.DateTime(),
    },
    "staff": {
        "staff_first_name": types.String(VARCHAR_LEN),
        "staff_last_name": types.String(VARCHAR_LEN),
        "email": types.String(VARCHAR_LEN),
        "phone": types.String(50),
        "active": types.Boolean(),
        "store_name": types.String(VARCHAR_LEN),
        "manager_id": types.Integer(),
        "last_updated": types.DateTime(),
    },
    "orders": {
        "order_id": types.Integer(),
        "customer_id": types.Integer(),
        "order_status": types.String(50),
        "order_date": types.DateTime(),
        "required_date": types.DateTime(),
        "shipped_date": types.DateTime(),
        "store_name": types.String(VARCHAR_LEN),
        "staff_first_name": types.String(VARCHAR_LEN),
        "last_updated": types.DateTime(),
    },
    "order_items": {
        "order_id": types.Integer(),
        "item_id": types.Integer(),
        "product_id": types.Integer(),
        "quantity": types.Integer(),
        "list_price": types.Float(),
        "discount": types.Float(),
        "last_updated": types.DateTime(),
    },
    "stocks": {
        "product_id": types.Integer(),
        "store_name": types.String(VARCHAR_LEN),
        "quantity": types.Integer(),
        "last_updated": types.DateTime(),
    },
}

# -------------------- UTILITIES --------------------


def _q(name: str) -> str:
    return f"`{name}`"


def floor_to_minute_utc(dt: Optional[datetime] = None) -> datetime:
    if dt is None:
        dt = datetime.now(timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.replace(second=0, microsecond=0, tzinfo=None)


def ts_folder_name(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d_%H%M")


def normalize_strings(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    df = df.copy()
    for c in columns:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()
    return df


def archive_inputs(
    source_files: Dict[str, Path],
    run_ts: datetime,
    which_keys: List[str],
    archive_root: Path = ARCHIVE_ROOT,
) -> None:
    """
    Move processed CSVs (for the given keys) into a timestamped folder,
    preserving relative structure.
    """
    ts_dir = archive_root / ts_folder_name(run_ts)
    for key in which_keys:
        src = source_files[key]
        try:
            dest = ts_dir / src
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dest))
            print(f"Archived {src} -> {dest}")
        except FileNotFoundError:
            print(f"Warning: source file not found, skipping: {src}")


# -------------------- TRANSFORMS (dataset-specific) --------------------


def transform_dataset(name: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the same consistent transforms as the main ETL:
    - Column renames
    - Datetime parsing (orders)
    - String normalization
    - Key consistency (store_name, staff_first_name)
    - order_items item_id backfill
    - stocks aggregation for duplicates
    """
    df = df.copy()

    if name == "brands":
        df = normalize_strings(df, ["brand_name"])

    elif name == "categories":
        df = normalize_strings(df, ["category_name"])

    elif name == "products":
        df = normalize_strings(df, ["product_name"])

    elif name == "customers":
        df = normalize_strings(df, ["first_name", "last_name", "email"])

    elif name == "stores":
        # rename name -> store_name
        if "name" in df.columns:
            df = df.rename(columns={"name": "store_name"})
        df = normalize_strings(df, ["store_name", "email"])

    elif name == "staff":
        # file is staffs.csv with 'name' and 'last_name'
        df = df.rename(
            columns={"name": "staff_first_name", "last_name": "staff_last_name"}
        )
        # keep store_name as-is if present
        df = normalize_strings(
            df, ["staff_first_name", "staff_last_name", "email", "store_name"]
        )

    elif name == "orders":
        df = df.rename(
            columns={"store": "store_name", "staff_name": "staff_first_name"}
        )
        # dates are DD/MM/YYYY
        if "order_date" in df.columns:
            df["order_date"] = pd.to_datetime(
                df["order_date"], format="%d/%m/%Y"
            )
        if "required_date" in df.columns:
            df["required_date"] = pd.to_datetime(
                df["required_date"], format="%d/%m/%Y"
            )
        if "shipped_date" in df.columns:
            df["shipped_date"] = pd.to_datetime(
                df["shipped_date"], format="%d/%m/%Y", errors="coerce"
            )
        df = normalize_strings(
            df, ["store_name", "staff_first_name", "order_status"]
        )

    elif name == "order_items":
        # ensure item_id per order
        if "item_id" not in df.columns or df["item_id"].isna().any():
            df["item_id"] = (
                df.sort_values(["order_id", "product_id"])
                .groupby("order_id")
                .cumcount()
                + 1
            )

    elif name == "stocks":
        # normalize potential store column variants
        if "store" in df.columns and "store_name" not in df.columns:
            df = df.rename(columns={"store": "store_name"})
        if "name" in df.columns and "store_name" not in df.columns:
            df = df.rename(columns={"name": "store_name"})
        df = normalize_strings(df, ["store_name"])
        # aggregate duplicates
        if {"store_name", "product_id", "quantity"}.issubset(df.columns):
            df = (
                df.groupby(
                    ["store_name", "product_id"], as_index=False, dropna=False
                )
                .agg({"quantity": "sum"})
                .reset_index(drop=True)
            )

    return df


# -------------------- STAGING AND UPSERT SQL --------------------


def ensure_table_exists(table: str) -> bool:
    insp = inspect(engine)
    return insp.has_table(table_name=table)


def stage_dataframe(
    df: pd.DataFrame, table: str, dtypes: Dict[str, Any]
) -> str:
    """
    Writes df into a staging table `<table>__stg` (replacing it).
    Only columns defined in dtypes are kept (and ordered).
    """
    stg_table = f"{table}__stg"
    cols = list(dtypes.keys())
    # Keep only known columns
    df = df.loc[:, [c for c in cols if c in df.columns]].copy()
    # Ensure all expected columns exist; add missing as NA
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[cols]

    with engine.begin() as conn:
        # replace staging table
        df.to_sql(
            stg_table,
            con=conn,
            if_exists="replace",
            index=False,
            dtype=dtypes,
            method=None,
        )
    return stg_table


def _join_eq(alias_left: str, alias_right: str, cols: Sequence[str]) -> str:
    # Use null-safe equality for safety
    return " AND ".join(
        f"{alias_left}.{_q(c)} <=> {alias_right}.{_q(c)}" for c in cols
    )


def _left_join_eq(alias_left: str, alias_right: str, cols: Sequence[str]) -> str:
    # Use standard equality for LEFT JOIN anti joins (keys should be non-null)
    return " AND ".join(
        f"{alias_left}.{_q(c)} = {alias_right}.{_q(c)}" for c in cols
    )


def upsert_from_stage(
    table: str, stg_table: str, key_cols: List[str], all_cols: List[str]
) -> Tuple[int, int, int]:
    """
    Applies:
    - Update changed rows (based on non-key columns, excluding last_updated)
    - Insert new rows
    - Delete rows missing from stage
    Returns (updated_count, inserted_count, deleted_count).
    """
    compare_cols = [
        c for c in all_cols if c not in key_cols and c != "last_updated"
    ]
    set_cols = compare_cols + ["last_updated"]

    eq_join = _join_eq("b", "s", key_cols)
    eq_left_join = _left_join_eq("b", "s", key_cols)
    diff_pred = " OR ".join([f"NOT (b.{_q(c)} <=> s.{_q(c)})" for c in compare_cols])
    set_clause = ", ".join([f"b.{_q(c)} = s.{_q(c)}" for c in set_cols])
    cols_list = ", ".join(_q(c) for c in all_cols)
    s_cols_list = ", ".join(f"s.{_q(c)}" for c in all_cols)

    updated = inserted = deleted = 0

    with engine.begin() as conn:
        # Update changed rows only
        if compare_cols:
            sql_update = f"""
                UPDATE {_q(table)} b
                JOIN {_q(stg_table)} s
                  ON {eq_join}
                SET {set_clause}
                WHERE {diff_pred}
            """
            res = conn.execute(text(sql_update))
            updated = res.rowcount or 0

        # Insert new rows
        sql_insert = f"""
            INSERT INTO {_q(table)} ({cols_list})
            SELECT {s_cols_list}
            FROM {_q(stg_table)} s
            LEFT JOIN {_q(table)} b
              ON {eq_left_join}
            WHERE { " AND ".join([f"b.{_q(k)} IS NULL" for k in key_cols]) }
        """
        res = conn.execute(text(sql_insert))
        inserted = res.rowcount or 0

        # Delete rows missing in stage (full-snapshot semantics)
        sql_delete = f"""
            DELETE b FROM {_q(table)} b
            LEFT JOIN {_q(stg_table)} s
              ON {eq_left_join}
            WHERE { " AND ".join([f"s.{_q(k)} IS NULL" for k in key_cols]) }
        """
        res = conn.execute(text(sql_delete))
        deleted = res.rowcount or 0

    return updated, inserted, deleted


# -------------------- DRIVER --------------------


def read_new_csvs() -> Dict[str, pd.DataFrame]:
    """
    Read any datasets that currently exist in the monitored folders.
    If a dataset's file is missing, it's skipped.
    """
    dfs: Dict[str, pd.DataFrame] = {}
    for key, path in SOURCE_FILES.items():
        if path.exists():
            dfs[key] = pd.read_csv(path)
    return dfs


def filter_and_align_columns(
    name: str, df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform dataset, align to DTYPES columns, and return DataFrame ready
    for staging (without last_updated; it's added later).
    """
    df = transform_dataset(name, df)
    # Keep only known columns for this dataset
    cols = list(DTYPES[name].keys())
    if "last_updated" in cols:
        cols_wo_ts = [c for c in cols if c != "last_updated"]
    else:
        cols_wo_ts = cols
    # Add any missing expected cols as NA
    for c in cols_wo_ts:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[cols_wo_ts]
    return df


def process_dataset_update(name: str, run_ts: datetime) -> Optional[Tuple[int, int, int]]:
    """
    If a new CSV for `name` is present, stage and apply incremental changes.
    Returns (updated, inserted, deleted) or None if skipped.
    """
    src = SOURCE_FILES[name]
    table = TABLES[name]
    key_cols = KEYS[name]
    dtypes = DTYPES[name]

    if not src.exists():
        return None

    if not ensure_table_exists(table):
        raise RuntimeError(
            f"Base table {table} does not exist. Run the initial ETL first."
        )

    # Read and transform
    df = pd.read_csv(src)
    df = filter_and_align_columns(name, df)

    # Add last_updated (run timestamp floored to minute)
    ts_min = floor_to_minute_utc(run_ts)
    df["last_updated"] = ts_min

    # Stage and upsert
    stg_table = stage_dataframe(df, table, dtypes)
    updated, inserted, deleted = upsert_from_stage(
        table=table, stg_table=stg_table, key_cols=key_cols, all_cols=list(dtypes.keys())
    )

    # Drop stage
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {_q(stg_table)}"))

    print(
        f"{name}: updated={updated}, inserted={inserted}, deleted={deleted}, "
        f"run_ts={ts_min}"
    )
    return updated, inserted, deleted


def main() -> None:
    run_ts = floor_to_minute_utc()

    # Detect which datasets have new CSVs in monitored folders
    present = [k for k, p in SOURCE_FILES.items() if p.exists()]
    if not present:
        print("No new CSVs detected in monitored folders. Nothing to do.")
        return

    print(f"Detected datasets to update: {', '.join(present)}")
    results: Dict[str, Tuple[int, int, int]] = {}

    for name in present:
        try:
            r = process_dataset_update(name, run_ts)
            if r is not None:
                results[name] = r
        except Exception as e:
            print(f"Error updating {name}: {e}")

    # Archive only the processed datasets
    if results:
        archive_inputs(SOURCE_FILES, run_ts, which_keys=list(results.keys()))
    else:
        print("No datasets were updated. Skipping archiving.")


if __name__ == "__main__":
    main()