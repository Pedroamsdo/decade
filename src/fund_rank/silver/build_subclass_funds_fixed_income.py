"""silver/subclass_funds_fixed_income — RF subset of subclass_funds.

Filters subclass_funds to rows whose `classificacao_anbima` starts with
"Renda Fixa". Excludes Previdência RF (different ANBIMA category).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import silver_path, write_parquet

log = get_logger(__name__)

RF_PREFIX = "Renda Fixa"


def run(settings: Settings, as_of: date) -> Path:
    in_path = silver_path(settings, "subclass_funds", as_of.isoformat())
    if not in_path.exists():
        raise FileNotFoundError(
            f"silver/subclass_funds not found at {in_path}; run build_subclass_funds first."
        )

    df = pl.read_parquet(in_path)
    before = df.height
    df_rf = df.filter(
        pl.col("classificacao_anbima")
        .cast(pl.Utf8, strict=False)
        .str.starts_with(RF_PREFIX)
    )
    log.info(
        "silver.subclass_funds_fixed_income.filtered",
        before=before,
        after=df_rf.height,
        excluded=before - df_rf.height,
    )

    out_path = silver_path(settings, "subclass_funds_fixed_income", as_of.isoformat())
    write_parquet(df_rf, out_path)
    log.info(
        "silver.subclass_funds_fixed_income.written",
        path=str(out_path),
        rows=df_rf.height,
    )
    return out_path
