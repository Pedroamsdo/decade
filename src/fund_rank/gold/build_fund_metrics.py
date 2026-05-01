"""gold/fund_metrics — one row per investable fund, with score 0–100.

Pipeline:
  1. Build dim_fund (vertical-stack class + subclass treated tables).
  2. Filter quotas <= as_of, attach fund_key (inner join).
  3. Daily log returns + jump filter (|z|>5σ on rolling 60-day window).
  4. Monthly returns; canonical monthly benchmark returns.
  5. Attach Information Ratio (annualized, vs canonical benchmark).
  6. Attach equity, nr_cotst, existing_time (used as eligibility filters).
  7. Score = percentile rank of IR across the eligible universe × 100.

Eligibility (4 filters):
  situacao = "Em Funcionamento Normal"
  AND nr_cotst > 1000
  AND existing_time >= 252  (≈ 1 year)
  AND equity >= R$ 50M

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
    daily_log_returns,
    flag_jumps,
    monthly_returns_from_daily,
)
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import silver_path, write_parquet

log = get_logger(__name__)


OUTPUT_COLUMNS: list[str] = [
    "cnpj_classe",
    "id_subclasse_cvm",
    "situacao",
    "publico_alvo",
    "equity",
    "nr_cotst",
    "existing_time",
    "information_ratio",
    "score",
]


CLASSE_SENTINEL = "__CLASSE__"

# Filtros de elegibilidade (padrão de mercado)
ELIGIBILITY_NR_COTST_MIN = 1_000
ELIGIBILITY_EXISTING_TIME_MIN = 252  # ~1 ano de história
ELIGIBILITY_EQUITY_MIN = 5e7  # R$ 50M


def _build_dim_fund(cls: pl.DataFrame, sub: pl.DataFrame) -> pl.DataFrame:
    cls_dim = cls.select(
        pl.col("cnpj_classe"),
        pl.lit(None, dtype=pl.Utf8).alias("id_subclasse_cvm"),
        pl.col("situacao"),
        pl.col("publico_alvo"),
        pl.col("benchmark"),
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


def _compute_score(metrics: pl.DataFrame) -> pl.DataFrame:
    """Score = percentile rank of `information_ratio` over the eligible universe × 100.

    Funds outside the eligibility universe get `score = null`. Rank uses
    `method="average"` (ties get the average rank), divided by the count of
    eligible funds.
    """
    eligible_expr = (
        (pl.col("situacao") == "Em Funcionamento Normal")
        & (pl.col("nr_cotst") > ELIGIBILITY_NR_COTST_MIN)
        & (pl.col("existing_time") >= ELIGIBILITY_EXISTING_TIME_MIN)
        & (pl.col("equity") >= ELIGIBILITY_EQUITY_MIN)
    )
    # IR só dos elegíveis (resto vira null e cai fora do rank)
    metrics = metrics.with_columns(
        _ir_eligible=pl.when(eligible_expr)
        .then(pl.col("information_ratio"))
        .otherwise(None)
    )
    n_eligible = pl.col("_ir_eligible").drop_nulls().count()
    return metrics.with_columns(
        score=pl.when(eligible_expr & pl.col("information_ratio").is_not_null())
        .then(
            (pl.col("_ir_eligible").rank(method="average") / n_eligible * 100.0)
            .round(2)
        )
        .otherwise(None)
    ).drop("_ir_eligible")


def _write_quality_report(df: pl.DataFrame, as_of: date, settings: Settings) -> Path:
    rows = df.height
    distinct = (
        df.select("cnpj_classe", "id_subclasse_cvm").unique().height if rows else 0
    )

    lines: list[str] = []
    lines.append(f"# gold/fund_metrics — quality report (as_of={as_of.isoformat()})\n")
    lines.append(f"- Rows: **{rows:,}**")
    lines.append(f"- Distinct (cnpj_classe, id_subclasse_cvm): **{distinct:,}**\n")

    if "score" in df.columns and rows:
        s = df["score"].drop_nulls()
        if s.len():
            lines.append("## Score distribution (eligible universe)\n")
            lines.append(f"- Eligible: **{s.len():,}** funds (rest have `score = null`)")
            buckets = [(0, 20), (20, 40), (40, 60), (60, 80), (80, 100.01)]
            lines.append("")
            lines.append("| bucket | n | pct |")
            lines.append("|---|---|---|")
            for lo, hi in buckets:
                n = int(s.filter((s >= lo) & (s < hi)).len())
                pct = n / s.len() * 100.0
                hi_str = "100" if hi > 100 else f"{hi:g}"
                lines.append(f"| {lo:g}-{hi_str} | {n:,} | {pct:.2f}% |")
            lines.append("")
            lines.append(
                f"- min/median/mean/max: {s.min():.2f} / {s.median():.2f} / "
                f"{s.mean():.2f} / {s.max():.2f}"
            )
            lines.append("")

    lines.append("## Nulls and ranges by column\n")
    lines.append("| column | nulls | pct | min | max |")
    lines.append("|---|---|---|---|---|")
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            lines.append(f"| {col} | n/a | n/a | n/a | n/a |")
            continue
        nulls = int(df[col].null_count())
        pct = (nulls / rows * 100.0) if rows else 0.0
        nn = df.filter(pl.col(col).is_not_null())[col]
        if nn.len() > 0 and nn.dtype.is_numeric():
            mn = f"{nn.min():.4g}"
            mx = f"{nn.max():.4g}"
        elif nn.len() > 0:
            mn = str(nn.min())
            mx = str(nn.max())
        else:
            mn = mx = "n/a"
        lines.append(f"| {col} | {nulls:,} | {pct:.2f}% | {mn} | {mx} |")
    lines.append("")

    out = (
        settings.pipeline.reports_root
        / f"as_of={as_of.isoformat()}"
        / "fund_metrics_quality.md"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    log.info("gold.fund_metrics.quality_report", path=str(out), rows=rows)
    return out


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

    metrics = (
        dim_fund
        .pipe(attach_information_ratio, monthly, bench_monthly)
        .pipe(attach_equity, quotas_keyed)
        .pipe(attach_nr_cotst, quotas_keyed)
        .pipe(attach_existing_time, as_of)
    )

    metrics = _compute_score(metrics)
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
    _write_quality_report(out_df, as_of, settings)
    return out_path
