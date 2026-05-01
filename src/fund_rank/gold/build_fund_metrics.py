"""gold/fund_metrics — one row per investable fund, with score 0–100.

Pipeline:
  1. Build dim_fund (vertical-stack class + subclass treated tables).
  2. Filter quotas <= as_of, attach fund_key (inner join).
  3. Daily log returns + jump filter (|z|>5σ on rolling 60-day window).
  4. Monthly returns; canonical monthly benchmark returns.
  5. Attach metrics declared in `configs/scoring.yaml` (currently: information_ratio).
  6. Attach equity, nr_cotst, existing_time (used as eligibility filters).
  7. Score = percentile rank of the configured composite over the eligible universe × 100.

Eligibility filters and metric weights are loaded from `configs/scoring.yaml`
via `Settings.scoring` (see `fund_rank/settings.ScoringConfig`).

Funds outside the eligible universe keep their raw metrics but get score = null.
Output: 9 columns documented in `docs/data_contracts.md`.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.gold._benchmark_returns import monthly_benchmark_returns
from fund_rank.gold._io import gold_path
from fund_rank.gold._metrics import (
    attach_equity,
    attach_existing_time,
    attach_information_ratio,
    attach_nr_cotst,
    attach_sortino_ratio,
    attach_tax_efficiency,
    daily_log_returns,
    flag_jumps,
    monthly_returns_from_daily,
)
from fund_rank.obs.logging import get_logger
from fund_rank.settings import ScoringConfig, Settings
from fund_rank.silver._io import silver_path, write_parquet

log = get_logger(__name__)


OUTPUT_COLUMNS: list[str] = [
    "cnpj_classe",
    "id_subclasse_cvm",
    "situacao",
    "publico_alvo",
    "tributacao_alvo",
    "equity",
    "nr_cotst",
    "existing_time",
    "information_ratio",
    "sortino_ratio",
    "tax_efficiency",
    "score",
]


CLASSE_SENTINEL = "__CLASSE__"


def _build_dim_fund(cls: pl.DataFrame, sub: pl.DataFrame) -> pl.DataFrame:
    cls_dim = cls.select(
        pl.col("cnpj_classe"),
        pl.lit(None, dtype=pl.Utf8).alias("id_subclasse_cvm"),
        pl.col("situacao"),
        pl.col("publico_alvo"),
        pl.col("benchmark"),
        pl.col("tributacao_alvo"),
        pl.col("data_de_inicio"),
        (pl.lit("CLS_") + pl.col("cnpj_classe")).alias("fund_key"),
        pl.col("cnpj_classe").alias("cnpj_fundo_classe_join"),
        pl.lit(CLASSE_SENTINEL).alias("id_subclasse_join"),
    )
    sub_dim = sub.select(
        pl.col("cnpj_classe"),
        pl.col("id_subclasse_cvm"),
        pl.col("situacao"),
        pl.col("publico_alvo"),
        pl.col("benchmark"),
        pl.col("tributacao_alvo"),
        pl.col("data_de_inicio"),
        (pl.lit("SUB_") + pl.col("id_subclasse_cvm")).alias("fund_key"),
        pl.col("cnpj_classe").alias("cnpj_fundo_classe_join"),
        pl.col("id_subclasse_cvm").alias("id_subclasse_join"),
    )
    return pl.concat([cls_dim, sub_dim], how="vertical_relaxed")


def _attach_fund_key(quotas: pl.DataFrame, dim_fund: pl.DataFrame) -> pl.DataFrame:
    quotas_keyed = quotas.with_columns(
        id_subclasse_join=pl.col("id_subclasse").fill_null(CLASSE_SENTINEL)
    )
    join_keys = dim_fund.select(
        pl.col("cnpj_fundo_classe_join"),
        pl.col("id_subclasse_join"),
        pl.col("fund_key"),
    )
    return quotas_keyed.join(
        join_keys,
        left_on=["cnpj_fundo_classe", "id_subclasse_join"],
        right_on=["cnpj_fundo_classe_join", "id_subclasse_join"],
        how="inner",
    )


def _compute_score(metrics: pl.DataFrame, scoring: ScoringConfig) -> pl.DataFrame:
    """Score = percentile rank of the configured composite × 100, restricted to eligible funds.

    Composite is built from `scoring.metrics`: signed by direction, weighted, and
    z-scored over the eligible universe when more than one metric is configured.
    With a single metric (the current default — `information_ratio` peso 1.0),
    composite reduces to ±metric, so ranking is identical to the legacy behavior.

    Funds outside the eligibility universe get `score = null`.
    """
    elig = scoring.eligibility
    eligible_expr = (
        (pl.col("situacao") == elig.situacao)
        & (pl.col("nr_cotst") > elig.nr_cotst_min)
        & (pl.col("existing_time") >= elig.existing_time_min_days)
        & (pl.col("equity") >= elig.equity_min_brl)
    )

    metric_specs = list(scoring.metrics.items())
    missing = [name for name, _ in metric_specs if name not in metrics.columns]
    if missing:
        raise ValueError(
            f"scoring.metrics references columns not produced by gold pipeline: {missing}"
        )

    if len(metric_specs) == 1:
        name, spec = metric_specs[0]
        sign = 1.0 if spec.direction == "positive" else -1.0
        composite_expr = sign * pl.col(name)
    else:
        terms = []
        for name, spec in metric_specs:
            sign = 1.0 if spec.direction == "positive" else -1.0
            elig_metric = pl.when(eligible_expr).then(pl.col(name)).otherwise(None)
            z = (pl.col(name) - elig_metric.mean()) / elig_metric.std()
            terms.append(sign * spec.weight * z)
        composite_expr = sum(terms)

    metrics = metrics.with_columns(_composite=composite_expr)
    metrics = metrics.with_columns(
        _composite_eligible=pl.when(eligible_expr)
        .then(pl.col("_composite"))
        .otherwise(None)
    )
    n_eligible = pl.col("_composite_eligible").drop_nulls().count()
    return metrics.with_columns(
        score=pl.when(eligible_expr & pl.col("_composite").is_not_null())
        .then(
            (pl.col("_composite_eligible").rank(method="average") / n_eligible * 100.0)
            .round(2)
        )
        .otherwise(None)
    ).drop("_composite_eligible", "_composite")


def run(settings: Settings, as_of: date) -> Path:
    cls_path = silver_path(settings, "class_funds_fixed_income_treated", as_of.isoformat())
    sub_path = silver_path(settings, "subclass_funds_fixed_income_treated", as_of.isoformat())
    qs_path = silver_path(settings, "quota_series", as_of.isoformat())
    idx_path = silver_path(settings, "index_series", as_of.isoformat())
    for p, name in [
        (cls_path, "class_funds_fixed_income_treated"),
        (sub_path, "subclass_funds_fixed_income_treated"),
        (qs_path, "quota_series"),
        (idx_path, "index_series"),
    ]:
        if not p.exists():
            raise FileNotFoundError(f"silver/{name} not found at {p}; run upstream builds.")

    cls = pl.read_parquet(cls_path)
    sub = pl.read_parquet(sub_path)
    quotas = pl.read_parquet(qs_path)
    indices = pl.read_parquet(idx_path)

    dim_fund = _build_dim_fund(cls, sub)
    log.info(
        "gold.fund_metrics.dim_fund_built",
        funds=dim_fund.height,
        classes=cls.height,
        subclasses=sub.height,
    )

    quotas = quotas.filter(pl.col("dt_comptc") <= as_of)
    quotas_keyed = _attach_fund_key(quotas, dim_fund)

    daily = daily_log_returns(quotas_keyed)
    daily = flag_jumps(daily, ret_col="log_ret", window=60, sigma=5.0)
    daily_clean = daily.filter(~pl.col("is_jump"))

    monthly = monthly_returns_from_daily(daily_clean)
    bench_monthly = monthly_benchmark_returns(indices)

    tax_rates = settings.scoring.tax.rates if settings.scoring.tax else {}
    metrics = (
        dim_fund
        .pipe(attach_information_ratio, monthly, bench_monthly)
        .pipe(attach_sortino_ratio, monthly, bench_monthly)
        .pipe(attach_tax_efficiency, tax_rates)
        .pipe(attach_equity, quotas_keyed)
        .pipe(attach_nr_cotst, quotas_keyed)
        .pipe(attach_existing_time, as_of)
    )

    metrics = _compute_score(metrics, settings.scoring)
    eligible = metrics.filter(pl.col("score").is_not_null())
    log.info(
        "gold.fund_metrics.scored",
        eligible=eligible.height,
        total=metrics.height,
        score_min=float(eligible["score"].min() or 0.0),
        score_max=float(eligible["score"].max() or 0.0),
    )

    out_df = metrics.select(OUTPUT_COLUMNS).sort("score", descending=True, nulls_last=True)
    out_path = gold_path(settings, "fund_metrics", as_of.isoformat())
    write_parquet(out_df, out_path)
    log.info(
        "gold.fund_metrics.written",
        path=str(out_path),
        rows=out_df.height,
    )
    return out_path
