"""Shared filter for the silver "fixed income" subset of class/subclass tables.

The filter logic — keep rows whose `classificacao_anbima` starts with
"Renda Fixa" — is identical for class and subclass; the only differences are
the input/output table names, the dimension key, and the output column list.
This module centralizes the logic so the two builders are thin wrappers.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import silver_path, write_parquet
from fund_rank.silver._quality_report import write_quality_report

log = get_logger(__name__)

RF_PREFIX = "Renda Fixa"


def filter_rf_subset(
    settings: Settings,
    as_of: date,
    *,
    in_table: str,
    out_table: str,
    distinct_key: str,
    output_columns: list[str],
) -> Path:
    """Filter a silver dimension table to its Renda Fixa subset.

    Args:
        in_table: source table name under silver/ (e.g. "class_funds").
        out_table: output table name under silver/ (e.g. "class_funds_fixed_income").
        distinct_key: column used as uniqueness key for the quality report.
        output_columns: full set of columns expected in the output (for the
            null-table in the quality report; not used to project the data).
    """
    in_path = silver_path(settings, in_table, as_of.isoformat())
    if not in_path.exists():
        raise FileNotFoundError(
            f"silver/{in_table} not found at {in_path}; run build_{in_table} first."
        )

    df = pl.read_parquet(in_path)
    before = df.height
    df_rf = df.filter(
        pl.col("classificacao_anbima")
        .cast(pl.Utf8, strict=False)
        .str.starts_with(RF_PREFIX)
    )
    log.info(
        f"silver.{out_table}.filtered",
        before=before,
        after=df_rf.height,
        excluded=before - df_rf.height,
    )

    out_path = silver_path(settings, out_table, as_of.isoformat())
    write_parquet(df_rf, out_path)
    log.info(f"silver.{out_table}.written", path=str(out_path), rows=df_rf.height)

    write_quality_report(
        df_rf, as_of, settings,
        table_name=out_table,
        distinct_keys=[distinct_key],
        null_columns=output_columns,
    )
    return out_path
