from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd

from sqlalchemy import text

from utils import (
    archive_inputs,
    build_dataset_dtypes,
    floor_to_minute_utc,
    get_archive_root,
    get_data_root,
    get_engine,
    get_key_columns,
    get_load_sequence,
    get_source_paths,
    load_config,
    load_to_sql,
    transform_dataset,
)

# -------------------- EXTRACT --------------------


def extract(cfg: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """
    Read all inputs from the configured data folder.
    Returns dataset key -> DataFrame.
    """
    data_root = get_data_root(cfg)
    data_root.mkdir(parents=True, exist_ok=True)

    src_paths = get_source_paths(cfg)
    raw: Dict[str, pd.DataFrame] = {}
    for name, path in src_paths.items():
        raw[name] = pd.read_csv(path)
    return raw


# -------------------- TRANSFORM --------------------


def transform(
    raw: Dict[str, pd.DataFrame],
    run_ts,
) -> Dict[str, pd.DataFrame]:
    """
    Apply standard transforms and add last_updated with minute precision.
    """
    ts_minute = floor_to_minute_utc(run_ts)

    dfs: Dict[str, pd.DataFrame] = {}
    for name, df in raw.items():
        tdf = transform_dataset(name, df)
        tdf["last_updated"] = ts_minute
        dfs[name] = tdf

    return dfs


# -------------------- LOAD --------------------


def load_all(cfg: Dict[str, Any], dfs: Dict[str, pd.DataFrame]) -> None:
    """
    Load tables in dependency order with constraints defined in config.
    """
    engine = get_engine(cfg)
    dtypes_by_ds = build_dataset_dtypes(cfg)
    seq = get_load_sequence(cfg)

    for name in seq:
        ds_cfg = cfg["datasets"][name]
        table = ds_cfg["table"]
        dtype = dtypes_by_ds[name]
        pk = ds_cfg.get("primary_key") or None
        uniques = ds_cfg.get("unique_constraints") or []
        fks = ds_cfg.get("foreign_keys") or []

        print(f"Loading {name} -> {table} ...")
        print(f"cols: {list(dfs[name].columns)}")
        load_to_sql(
            engine=engine,
            df=dfs[name],
            table_name=table,
            dtype=dtype,
            primary_key=pk,
            unique_constraints=uniques,
            foreign_keys=fks,
        )
    print("\nAll data loaded successfully!")


# -------------------- ARCHIVE --------------------


def archive_processed(cfg: Dict[str, Any], run_ts) -> None:
    """
    Archive all CSVs present in config (full snapshot run).
    """
    src_paths = get_source_paths(cfg)
    archive_root = get_archive_root(cfg)
    which = list(src_paths.keys())
    archive_inputs(src_paths, run_ts, which_keys=which, archive_root=archive_root)


# -------------------- MAIN --------------------


if __name__ == "__main__":
    cfg = load_config("config.yaml")

    # Ensure data directory exists (place all CSVs directly under it)
    data_root = get_data_root(cfg)
    data_root.mkdir(parents=True, exist_ok=True)

    run_ts = floor_to_minute_utc()

    raw = extract(cfg)
    dfs = transform(raw, run_ts)
    load_all(cfg, dfs)
    archive_processed(cfg, run_ts)