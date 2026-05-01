"""silver/quota_series_fixed_income — RF subset of quota_series.

Filters quota_series to rows whose underlying fund (class without subclasses
OR subclass) appears in the RF dimension tables.

Filter semantics (id_subclasse is the discriminator):
  - id_subclasse IS NULL   → cnpj_fundo_classe must be in class_funds_fixed_income.cnpj_classe
  - id_subclasse NOT NULL  → id_subclasse must be in subclass_funds_fixed_income.id_subclasse_cvm
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import silver_path, write_parquet

log = get_logger(__name__)


QUOTA_COLUMNS: list[str] = [
    "tp_fundo_classe",
    "cnpj_fundo_classe",
    "id_subclasse",
    "dt_comptc",
    "vl_total",
    "vl_quota",
    "vl_patrim_liq",
    "captc_dia",
    "resg_dia",
    "nr_cotst",
]


def run(settings: Settings, as_of: date) -> Path:
    qs_path = silver_path(settings, "quota_series", as_of.isoformat())
    cf_rf_path = silver_path(settings, "class_funds_fixed_income", as_of.isoformat())
    sf_rf_path = silver_path(settings, "subclass_funds_fixed_income", as_of.isoformat())

    for p, name in [(qs_path, "quota_series"), (cf_rf_path, "class_funds_fixed_income"), (sf_rf_path, "subclass_funds_fixed_income")]:
        if not p.exists():
            raise FileNotFoundError(
                f"silver/{name} not found at {p}; run the upstream build steps first."
            )

    qs = pl.read_parquet(qs_path)

    cf_rf_keys = (
        pl.read_parquet(cf_rf_path)
        .select(pl.col("cnpj_classe").alias("cnpj_fundo_classe"))
        .unique()
    )
    sf_rf_keys = (
        pl.read_parquet(sf_rf_path)
        .select(pl.col("id_subclasse_cvm").alias("id_subclasse"))
        .unique()
    )

    classe_rows = qs.filter(pl.col("id_subclasse").is_null()).join(
        cf_rf_keys, on="cnpj_fundo_classe", how="inner"
    )
    sub_rows = qs.filter(pl.col("id_subclasse").is_not_null()).join(
        sf_rf_keys, on="id_subclasse", how="inner"
    )
    rf_qs = pl.concat([classe_rows, sub_rows], how="vertical_relaxed").select(QUOTA_COLUMNS)

    log.info(
        "silver.quota_series_fixed_income.filtered",
        total_input=qs.height,
        classe_kept=classe_rows.height,
        sub_kept=sub_rows.height,
        total_output=rf_qs.height,
    )

    out_path = silver_path(settings, "quota_series_fixed_income", as_of.isoformat())
    write_parquet(rf_qs, out_path)
    log.info(
        "silver.quota_series_fixed_income.written",
        path=str(out_path),
        rows=rf_qs.height,
    )
    return out_path
