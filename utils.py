from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import yaml
from sqlalchemy import create_engine, text, types

# -------------------- CONFIG / ENGINE --------------------


def load_config(path: Path | str = "config.yaml") -> Dict[str, Any]:
    """
    Load configuration from a YAML file.
    """
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg


def _read_credentials(creds_path: Path) -> Tuple[str, str]:
    """
    Read DB user and password from a simple two-line file.
    """
    with open(creds_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    user = lines[0].strip()
    password = lines[1].strip()
    return user, password


def get_engine(cfg: Dict[str, Any]):
    """
    Create a SQLAlchemy engine using db settings from config.
    """
    db = cfg["db"]
    user, password = _read_credentials(Path(db["credentials_file"]))
    url = (
        f'{db["driver"]}://{user}:{password}@{db["host"]}:'
        f'{db["port"]}/{db["database"]}'
    )
    engine = create_engine(url, pool_pre_ping=bool(db.get("pool_pre_ping", True)))
    return engine


# -------------------- PATHS / DATASET HELPERS --------------------


def get_data_root(cfg: Dict[str, Any]) -> Path:
    return Path(cfg["paths"]["data_root"]).resolve()


def get_archive_root(cfg: Dict[str, Any]) -> Path:
    return Path(cfg["paths"]["archive_root"]).resolve()


def get_source_paths(cfg: Dict[str, Any]) -> Dict[str, Path]:
    """
    dataset name -> absolute path to CSV source file.
    """
    root = get_data_root(cfg)
    return {name: root / ds["file"] for name, ds in cfg["datasets"].items()}


def get_tables_map(cfg: Dict[str, Any]) -> Dict[str, str]:
    """
    dataset name -> table name.
    """
    return {name: ds["table"] for name, ds in cfg["datasets"].items()}


def get_key_columns(cfg: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    dataset name -> key columns (used for upsert/delete).
    """
    return {name: list(ds["key_columns"]) for name, ds in cfg["datasets"].items()}


def get_load_sequence(cfg: Dict[str, Any]) -> List[str]:
    """
    Ordered dataset processing list to respect FK dependencies.
    """
    return list(cfg.get("load_sequence", cfg["datasets"].keys()))


# -------------------- SQL / DTYPE HELPERS --------------------


def _sqlalchemy_type_from_spec(
    spec: Any, default_varchar_len: int
) -> types.TypeEngine:
    """
    Convert a YAML type spec to a SQLAlchemy types.* instance.
    Accepts:
      - "String" (uses default length)
      - {type: "String", length: 123}
      - "Integer", "Float", "DateTime", "Boolean"
    """
    if isinstance(spec, str):
        tname = spec
        params: Dict[str, Any] = {}
    elif isinstance(spec, dict):
        tname = spec["type"]
        params = {k: v for k, v in spec.items() if k != "type"}
    else:
        raise ValueError(f"Unsupported dtype spec: {spec!r}")

    tname = tname.lower()
    if tname == "string":
        length = int(params.get("length", default_varchar_len))
        return types.String(length)
    if tname == "integer":
        return types.Integer()
    if tname == "float":
        return types.Float()
    if tname == "datetime":
        return types.DateTime()
    if tname == "boolean":
        return types.Boolean()
    raise ValueError(f"Unsupported type name: {tname}")


def build_dataset_dtypes(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build SQLAlchemy dtype mappings for each dataset from config.
    """
    default_len = int(cfg["sql"]["varchar_len"])
    dtypes: Dict[str, Dict[str, Any]] = {}
    for name, ds in cfg["datasets"].items():
        dtypes[name] = {
            col: _sqlalchemy_type_from_spec(tspec, default_len)
            for col, tspec in ds["dtype"].items()
        }
    return dtypes


# -------------------- GENERIC SQL HELPERS --------------------


def _q(name: str) -> str:
    """
    Quote a MySQL identifier with backticks.
    """
    return f"`{name}`"


def _cols(cols: Sequence[str]) -> str:
    return ", ".join(_q(c) for c in cols)


def _constraint_name(prefix: str, table: str, cols: Sequence[str]) -> str:
    """
    Build a deterministic constraint name, truncated for safety.
    """
    base = f"{prefix}_{table}_{'_'.join(cols)}"
    return base[:60]


def load_to_sql(
    engine,
    df: pd.DataFrame,
    table_name: str,
    dtype: Dict[str, Any],
    primary_key: Optional[Sequence[str]] = None,
    unique_constraints: Optional[List[Sequence[str]]] = None,
    foreign_keys: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Replace a table with df, then add constraints (PK, unique, FKs).
    """
    if unique_constraints is None:
        unique_constraints = []
    if foreign_keys is None:
        foreign_keys = []

    # Enforce stable schema and reduce accidental dtype issues
    df = df.drop_duplicates().convert_dtypes().infer_objects()

    with engine.begin() as conn:
        df.to_sql(
            table_name,
            con=conn,
            if_exists="replace",
            index=False,
            dtype=dtype,
            method=None,
        )

        # Unique constraints
        for cols in unique_constraints:
            col_list = list(cols)
            uc_name = _constraint_name("uq", table_name, col_list)
            conn.execute(
                text(
                    f"ALTER TABLE {_q(table_name)} "
                    f"ADD CONSTRAINT {_q(uc_name)} UNIQUE ({_cols(col_list)})"
                )
            )

        # Primary key
        if primary_key:
            pk_cols = list(primary_key)
            conn.execute(
                text(
                    f"ALTER TABLE {_q(table_name)} "
                    f"ADD PRIMARY KEY ({_cols(pk_cols)})"
                )
            )

        # Foreign keys
        for fk in foreign_keys:
            cols = list(fk["columns"])
            ref_table = fk["ref_table"]
            ref_cols = list(fk["ref_columns"])
            on_delete = fk.get("on_delete")
            on_update = fk.get("on_update")

            fk_name = _constraint_name("fk", table_name, cols)
            sql = (
                f"ALTER TABLE {_q(table_name)} "
                f"ADD CONSTRAINT {_q(fk_name)} "
                f"FOREIGN KEY ({_cols(cols)}) "
                f"REFERENCES {_q(ref_table)} ({_cols(ref_cols)})"
            )
            if on_delete:
                sql += f" ON DELETE {on_delete}"
            if on_update:
                sql += f" ON UPDATE {on_update}"
            conn.execute(text(sql))

        # Row count info
        result = conn.execute(text(f"SELECT COUNT(*) FROM {_q(table_name)}"))
        print(f"{table_name} count:", result.scalar())


# -------------------- TRANSFORMS / NORMALIZATION --------------------


def normalize_strings(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """
    Trim/clean selected string columns using pandas' nullable StringDtype.
    """
    df = df.copy()
    for c in columns:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()
    return df


def transform_dataset(name: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply consistent dataset-specific transforms used by both ETL and
    incremental updates: renames, parsing, normalization, key consistency.
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
        # rename name -> store_name (if present)
        if "name" in df.columns:
            df = df.rename(columns={"name": "store_name"})
        df = normalize_strings(df, ["store_name", "email"])

    elif name == "staff":
        # source has 'name' (first name) and 'last_name'
        df = df.rename(
            columns={"name": "staff_first_name", "last_name": "staff_last_name"}
        )
        df = normalize_strings(
            df,
            ["staff_first_name", "staff_last_name", "phone", "email", "store_name"],
        )

    elif name == "orders":
        # normalize store and staff name columns
        df = df.rename(
            columns={"store": "store_name", "staff_name": "staff_first_name"}
        )
        # parse dates as DD/MM/YYYY; shipped_date can be missing
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
        # backfill item_id as 1..n per order if missing or NA
        if "item_id" not in df.columns or df["item_id"].isna().any():
            df = df.sort_values(["order_id", "product_id"])
            df["item_id"] = df.groupby("order_id").cumcount() + 1

    elif name == "stocks":
        # normalize potential store column variants
        if "store" in df.columns and "store_name" not in df.columns:
            df = df.rename(columns={"store": "store_name"})
        if "name" in df.columns and "store_name" not in df.columns:
            df = df.rename(columns={"name": "store_name"})
        df = normalize_strings(df, ["store_name"])
        # aggregate duplicates (sum quantity)
        if {"store_name", "product_id", "quantity"}.issubset(df.columns):
            df = (
                df.groupby(
                    ["store_name", "product_id"], as_index=False, dropna=False
                )
                .agg({"quantity": "sum"})
                .reset_index(drop=True)
            )

    return df


# -------------------- TIME / ARCHIVE HELPERS --------------------


def floor_to_minute_utc(dt: Optional[pd.Timestamp | pd.Timestamp] = None):
    """
    Return a naive datetime in UTC, rounded down to the minute.
    """
    from datetime import datetime, timezone

    if dt is None:
        dt = datetime.now(timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.replace(second=0, microsecond=0, tzinfo=None)


def ts_folder_name(ts) -> str:
    """
    Folder-friendly timestamp like 2025-10-29_1200.
    """
    from datetime import datetime

    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()
    assert hasattr(ts, "strftime")
    return ts.strftime("%Y-%m-%d_%H%M")


def archive_inputs(
    source_paths: Dict[str, Path],
    run_ts,
    which_keys: List[str],
    archive_root: Path,
) -> None:
    """
    Move processed CSVs into archive/<YYYY-MM-DD_HHMM>/filename.csv.
    Only moves CSV files (not directories).
    """
    ts_dir = archive_root / ts_folder_name(run_ts)
    ts_dir.mkdir(parents=True, exist_ok=True)

    for key in which_keys:
        src_path = source_paths[key]
        try:
            dest_path = ts_dir / src_path.name
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            src_path.rename(dest_path)
            print(f"Archived {src_path} -> {dest_path}")
        except FileNotFoundError:
            print(f"Warning: source file not found, skipping: {src_path}")