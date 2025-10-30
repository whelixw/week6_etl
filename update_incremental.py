# Incremental updater for the MySQL DB using timestamped CSV snapshots.
# - Monitors input folder for new CSVs (files present in data folder)
# - Applies consistent transforms used by the main ETL
# - Stages the new data into <table>__stg
# - Upserts only changed rows and updates last_updated to current run timestamp
# - Deletes rows missing from the new CSV (treat CSVs as full snapshots)
# - Archives processed CSVs into archive/<YYYY-MM-DD_HHMM>/filename.csv
#
# Staff natural key used for matching:
#   (staff_first_name, staff_last_name, phone, email)

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import inspect, text

from utils import (
    _q,
    archive_inputs,
    build_dataset_dtypes,
    floor_to_minute_utc,
    get_archive_root,
    get_engine,
    get_load_sequence,
    get_source_paths,
    get_tables_map,
    get_key_columns,
    load_config,
    transform_dataset,
)

# -------------------- STAGING HELPERS --------------------


def ensure_table_exists(engine, table: str) -> bool:
    """
    Check if a base table exists in the target database.
    """
    insp = inspect(engine)
    return insp.has_table(table_name=table)


def stage_dataframe(
    engine, df: pd.DataFrame, table: str, dtypes: Dict[str, Any]
) -> str:
    """
    Write df into a staging table `<table>__stg` (replace if exists).
    Only columns defined in dtypes are kept and ordered; missing added as NA.
    """
    stg_table = f"{table}__stg"
    cols = list(dtypes.keys())

    # Keep only known columns; add missing as NA
    df = df.loc[:, [c for c in cols if c in df.columns]].copy()
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[cols]

    with engine.begin() as conn:
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
    """
    Null-safe equality join (MySQL <=>) for robust comparisons.
    """
    return " AND ".join(
        f"{alias_left}.{_q(c)} <=> {alias_right}.{_q(c)}" for c in cols
    )


def _left_join_eq(alias_left: str, alias_right: str, cols: Sequence[str]) -> str:
    """
    Standard equality (used for anti-joins on non-null key columns).
    """
    return " AND ".join(
        f"{alias_left}.{_q(c)} = {alias_right}.{_q(c)}" for c in cols
    )


def upsert_from_stage(
    engine,
    table: str,
    stg_table: str,
    key_cols: List[str],
    all_cols: List[str],
) -> Tuple[int, int, int]:
    """
    Apply:
      - UPDATE changed rows (compare non-key, non-last_updated cols)
      - INSERT rows present in stage but absent in base
      - DELETE rows missing from stage (full-snapshot semantics)
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
        # Update only changed rows
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

        # Delete missing rows (full snapshot)
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


def process_dataset_update(
    engine,
    name: str,
    cfg: Dict[str, Any],
    dtypes_by_ds: Dict[str, Dict[str, Any]],
    run_ts,
) -> Optional[Tuple[int, int, int]]:
    """
    If a new CSV for `name` is present, stage and apply incremental changes.
    Returns (updated, inserted, deleted) or None if no CSV present.
    """
    src_paths = get_source_paths(cfg)
    if not src_paths[name].exists():
        return None

    table = cfg["datasets"][name]["table"]
    key_cols: List[str] = list(cfg["datasets"][name]["key_columns"])
    dtypes = dtypes_by_ds[name]

    if not ensure_table_exists(engine, table):
        raise RuntimeError(
            f"Base table {table} does not exist. Run the initial ETL first."
        )

    # Read, transform, and add last_updated (run timestamp floored to minute)
    df = pd.read_csv(src_paths[name])
    df = transform_dataset(name, df)
    ts_min = floor_to_minute_utc(run_ts)
    df["last_updated"] = ts_min

    # Stage and upsert
    stg_table = stage_dataframe(engine, df, table, dtypes)
    updated, inserted, deleted = upsert_from_stage(
        engine=engine,
        table=table,
        stg_table=stg_table,
        key_cols=key_cols,
        all_cols=list(dtypes.keys()),
    )

    # Drop staging table
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {_q(stg_table)}"))

    print(
        f"{name}: updated={updated}, inserted={inserted}, deleted={deleted}, "
        f"run_ts={ts_min}"
    )
    return updated, inserted, deleted


def main() -> None:
    cfg = load_config("config.yaml")
    engine = get_engine(cfg)

    # Build dtype maps once
    dtypes_by_ds = build_dataset_dtypes(cfg)

    # Ensure data directory exists
    data_root = Path(cfg["paths"]["data_root"])
    data_root.mkdir(parents=True, exist_ok=True)

    run_ts = floor_to_minute_utc()

    # Detect present CSVs in monitored folder
    src_paths = get_source_paths(cfg)
    present = [name for name, p in src_paths.items() if p.exists()]
    if not present:
        print("No new CSVs detected in data folder. Nothing to do.")
        return

    # Process in dependency-aware order to reduce FK issues
    ordered = [n for n in get_load_sequence(cfg) if n in present]
    print(f"Detected datasets to update (ordered): {', '.join(ordered)}")

    results: Dict[str, Tuple[int, int, int]] = {}
    for name in ordered:
        try:
            r = process_dataset_update(engine, name, cfg, dtypes_by_ds, run_ts)
            if r is not None:
                results[name] = r
        except Exception as e:
            print(f"Error updating {name}: {e}")

    # Archive only processed datasets
    if results:
        archive_inputs(
            source_paths=src_paths,
            run_ts=run_ts,
            which_keys=list(results.keys()),
            archive_root=Path(cfg["paths"]["archive_root"]),
        )
    else:
        print("No datasets were updated. Skipping archiving.")


if __name__ == "__main__":
    main()