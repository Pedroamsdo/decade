"""gold/validacao — calendar-year 2025 return per RF fund (sanity / cross-check).

One row per investable fund (5,849 = 5,623 classes + 226 subclasses), with
`retorno_2025 = vl_quota[last <= 2025-12-31] / vl_quota[last <= 2024-12-31] − 1`.
Used to cross-check the score in `gold/fund_metrics` against the raw return
of the calendar year.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.gold._io import gold_path
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import silver_path, write_parquet

log = get_logger(__name__)


OUTPUT_COLUMNS: list[str] = [
    "cnpj_fundo",
    "cnpj_classe",
    "id_subclasse_cvm",
    "nome",
    "retorno_2025",
]


CLASSE_SENTINEL = "__CLASSE__"


def _build_dim_fund(cls: pl.DataFrame, sub: pl.DataFrame) -> pl.DataFrame:
    """Stack class + subclass tables with `nome`, cnpj_fundo, e chaves de join."""
    cls_dim = cls.select(
        pl.col("cnpj_fundo"),
        pl.col("cnpj_classe"),
        pl.lit(None, dtype=pl.Utf8).alias("id_subclasse_cvm"),
        pl.col("denom_social_classe").alias("nome"),
        pl.col("cnpj_classe").alias("cnpj_fundo_classe_join"),
        pl.lit(CLASSE_SENTINEL).alias("id_subclasse_join"),
    )
    sub_dim = sub.select(
        pl.col("cnpj_fundo"),
        pl.col("cnpj_classe"),
        pl.col("id_subclasse_cvm"),
        pl.col("denom_social_subclasse").alias("nome"),
        pl.col("cnpj_classe").alias("cnpj_fundo_classe_join"),
        pl.col("id_subclasse_cvm").alias("id_subclasse_join"),
    )
    return pl.concat([cls_dim, sub_dim], how="vertical_relaxed")


def _attach_quota_anchor(
    quotas: pl.DataFrame, dim_fund: pl.DataFrame, cutoff: date, alias: str
) -> pl.DataFrame:
    """Latest non-null `vl_quota` per fund up to `cutoff`. Returns
    (`cnpj_fundo_classe_join`, `id_subclasse_join`, alias)."""
    q = quotas.filter(
        pl.col("vl_quota").is_not_null() & (pl.col("dt_comptc") <= cutoff)
    ).with_columns(id_subclasse_join=pl.col("id_subclasse").fill_null(CLASSE_SENTINEL))
    last = (
        q.sort(["cnpj_fundo_classe", "id_subclasse_join", "dt_comptc"])
        .group_by(["cnpj_fundo_classe", "id_subclasse_join"], maintain_order=True)
        .agg(pl.col("vl_quota").last().alias(alias))
    )
    return last.rename({"cnpj_fundo_classe": "cnpj_fundo_classe_join"})


def run(settings: Settings, as_of: date) -> Path:
    cls_path = silver_path(settings, "class_funds_fixed_income_treated", as_of.isoformat())
    sub_path = silver_path(settings, "subclass_funds_fixed_income_treated", as_of.isoformat())
    qs_path = silver_path(settings, "quota_series", as_of.isoformat())
    for p, name in [
        (cls_path, "class_funds_fixed_income_treated"),
        (sub_path, "subclass_funds_fixed_income_treated"),
        (qs_path, "quota_series"),
    ]:
        if not p.exists():
            raise FileNotFoundError(f"silver/{name} not found at {p}; run upstream builds.")

    cls = pl.read_parquet(cls_path)
    sub = pl.read_parquet(sub_path)
    quotas = pl.read_parquet(qs_path)

    dim_fund = _build_dim_fund(cls, sub)
    log.info(
        "gold.validacao.dim_fund_built",
        funds=dim_fund.height,
        classes=cls.height,
        subclasses=sub.height,
    )

    cutoff_2024 = date(2024, 12, 31)
    cutoff_2025 = date(2025, 12, 31)

    anchor_2024 = _attach_quota_anchor(quotas, dim_fund, cutoff_2024, "vl_quota_2024")
    anchor_2025 = _attach_quota_anchor(quotas, dim_fund, cutoff_2025, "vl_quota_2025")

    df = (
        dim_fund.join(
            anchor_2024,
            on=["cnpj_fundo_classe_join", "id_subclasse_join"],
            how="left",
        )
        .join(
            anchor_2025,
            on=["cnpj_fundo_classe_join", "id_subclasse_join"],
            how="left",
        )
        .with_columns(
            _raw=pl.when(
                pl.col("vl_quota_2024").is_not_null()
                & pl.col("vl_quota_2025").is_not_null()
                & (pl.col("vl_quota_2024") > 0)
            )
            .then(pl.col("vl_quota_2025") / pl.col("vl_quota_2024") - 1.0)
            .otherwise(None)
        )
        .with_columns(
            retorno_2025=pl.when(pl.col("_raw").is_finite())
            .then(pl.col("_raw"))
            .otherwise(None)
        )
        .select(OUTPUT_COLUMNS)
    )

    out_path = gold_path(settings, "validacao", as_of.isoformat())
    write_parquet(df, out_path)
    log.info(
        "gold.validacao.written",
        path=str(out_path),
        rows=df.height,
        nulls_retorno=int(df["retorno_2025"].null_count()),
    )

    return out_path
