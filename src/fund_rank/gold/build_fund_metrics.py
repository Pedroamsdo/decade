"""gold/fund_metrics — one row per investable fund with all raw metrics.

A "fund" is a class without subclasses OR a subclass. Construction:

  1. Build `dim_fund` by vertical-stacking class_funds_fixed_income_treated
     and subclass_funds_fixed_income_treated, with a synthetic `fund_key` and
     join keys.
  2. Filter `silver/quota_series` to `dt_comptc <= as_of`, attach `fund_key`
     via a single inner join on (`cnpj_fundo_classe_join`, `id_subclasse_join`).
  3. Compute log returns, drop ±5σ jumps (universe.yaml#jump_detection_sigma),
     reduce to monthly returns, build canonical monthly benchmark returns.
  4. Attach 9 metrics to `dim_fund`. Write parquet + quality report.

Output schema is documented in `docs/data_contracts.md` (Gold layer).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.gold._benchmark_returns import monthly_benchmark_returns
from fund_rank.gold._io import gold_path
from fund_rank.gold._metrics import (
    attach_anbima_risk_weight,
    attach_equity,
    attach_existing_time,
    attach_hit_rate,
    attach_liquid_return_12m,
    attach_max_drawdown,
    attach_net_captation,
    attach_sharpe_rolling_std,
    attach_std_annualized,
    daily_log_returns,
    flag_jumps,
    monthly_returns_from_daily,
)
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import silver_path, write_parquet

log = get_logger(__name__)


OUTPUT_COLUMNS: list[str] = [
    "cnpj_fundo",
    "cnpj_classe",
    "id_subclasse_cvm",
    "situacao",
    "publico_alvo",
    "anbima_classification",
    "anbima_risk_weight",
    "redemption_days",
    "equity",
    "existing_time",
    "net_captation",
    "hit_rate",
    "sharpe_rolling",
    "liquid_return_12m",
    "standard_deviation_annualized",
    "max_drawdown",
]


CLASSE_SENTINEL = "__CLASSE__"


def _build_dim_fund(cls: pl.DataFrame, sub: pl.DataFrame) -> pl.DataFrame:
    """Vertical-stack class + subclass dim tables with a synthetic `fund_key`.

    The two source tables share a 17-column schema except for the subclass-only
    columns (`id_subclasse_cvm`, `denom_social_subclasse`). We harmonize on the
    columns the gold layer needs.
    """
    cls_dim = cls.select(
        pl.col("cnpj_fundo"),
        pl.col("cnpj_classe"),
        pl.lit(None, dtype=pl.Utf8).alias("id_subclasse_cvm"),
        pl.col("situacao"),
        pl.col("publico_alvo"),
        pl.col("classificacao_anbima"),
        pl.col("benchmark"),
        pl.col("prazo_de_resgate"),
        pl.col("data_de_inicio"),
        (pl.lit("CLS_") + pl.col("cnpj_classe")).alias("fund_key"),
        pl.col("cnpj_classe").alias("cnpj_fundo_classe_join"),
        pl.lit(CLASSE_SENTINEL).alias("id_subclasse_join"),
    )
    sub_dim = sub.select(
        pl.col("cnpj_fundo"),
        pl.col("cnpj_classe"),
        pl.col("id_subclasse_cvm"),
        pl.col("situacao"),
        pl.col("publico_alvo"),
        pl.col("classificacao_anbima"),
        pl.col("benchmark"),
        pl.col("prazo_de_resgate"),
        pl.col("data_de_inicio"),
        (pl.lit("SUB_") + pl.col("id_subclasse_cvm")).alias("fund_key"),
        pl.col("cnpj_classe").alias("cnpj_fundo_classe_join"),
        pl.col("id_subclasse_cvm").alias("id_subclasse_join"),
    )
    return pl.concat([cls_dim, sub_dim], how="vertical_relaxed")


def _attach_fund_key(quotas: pl.DataFrame, dim_fund: pl.DataFrame) -> pl.DataFrame:
    """Inner-join quotas with dim_fund on (cnpj_fundo_classe_join, id_subclasse_join).

    For class rows, `id_subclasse_join` is filled with the CLASSE_SENTINEL on
    both sides. Subclass cotas keep the literal `id_subclasse` value.
    """
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


def _write_quality_report(df: pl.DataFrame, as_of: date, settings: Settings) -> Path:
    rows = df.height
    distinct = df["fund_key"].n_unique() if "fund_key" in df.columns else rows
    dups = rows - distinct

    lines: list[str] = []
    lines.append(f"# gold/fund_metrics — quality report (as_of={as_of.isoformat()})\n")
    lines.append(f"- Rows: **{rows:,}**")
    if "fund_key" in df.columns:
        lines.append(f"- Distinct fund_key: **{distinct:,}**")
        lines.append(f"- Duplicates by fund_key: **{dups:,}**")
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
    log.info(
        "gold.fund_metrics.quality_report",
        path=str(out),
        rows=rows,
    )
    return out


def run(settings: Settings, as_of: date) -> Path:
    # 1. Inputs
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

    # 2. Build dim_fund (1 row per investable fund)
    dim_fund = _build_dim_fund(cls, sub)
    log.info(
        "gold.fund_metrics.dim_fund_built",
        funds=dim_fund.height,
        classes=cls.height,
        subclasses=sub.height,
    )

    # 3. Filter quotas to as_of, attach fund_key (inner join)
    quotas = quotas.filter(pl.col("dt_comptc") <= as_of)
    quotas_keyed = _attach_fund_key(quotas, dim_fund)
    log.info(
        "gold.fund_metrics.quotas_attached",
        rows_in=quotas.height,
        rows_keyed=quotas_keyed.height,
        distinct_funds_with_quotas=quotas_keyed["fund_key"].n_unique(),
    )

    # 4. Daily log returns + jump filter (drop |z| > 5σ on rolling 60d window)
    daily = daily_log_returns(quotas_keyed)
    daily = flag_jumps(daily, ret_col="log_ret", window=60, sigma=5.0)
    n_jumps = int(daily["is_jump"].sum())
    daily_clean = daily.filter(~pl.col("is_jump"))
    log.info(
        "gold.fund_metrics.jumps_filtered",
        rows_before=daily.height,
        rows_after=daily_clean.height,
        jumps_removed=n_jumps,
    )

    # 5. Monthly aggregates
    monthly = monthly_returns_from_daily(daily_clean)
    bench_monthly = monthly_benchmark_returns(indices)

    # 6. Per-fund metrics — chained joins
    scoring_cfg = settings.scoring
    weights = scoring_cfg.get("classificacao_anbima_risk", {})
    if not weights:
        raise RuntimeError(
            "configs/scoring.yaml is missing the `classificacao_anbima_risk` block."
        )

    metrics = (
        dim_fund
        .pipe(attach_hit_rate, monthly, bench_monthly)
        .pipe(attach_sharpe_rolling_std, monthly, bench_monthly)
        .pipe(attach_liquid_return_12m, monthly, as_of)
        .pipe(attach_equity, quotas_keyed)
        .pipe(attach_existing_time, as_of)
        .pipe(attach_net_captation, quotas_keyed)
        .pipe(attach_anbima_risk_weight, weights)
        .pipe(attach_std_annualized, daily_clean)
        .pipe(attach_max_drawdown, daily_clean)
    )

    # 7. Final schema (drop join helpers, rename to public names)
    out_df = metrics.with_columns(
        pl.col("classificacao_anbima").alias("anbima_classification"),
        pl.col("prazo_de_resgate").cast(pl.Int64).alias("redemption_days"),
    ).select(
        # fund_key kept for join in downstream ranking, dropped at the end
        "fund_key",
        *OUTPUT_COLUMNS,
    )

    # 8. Write parquet + quality report
    out_path = gold_path(settings, "fund_metrics", as_of.isoformat())
    write_parquet(out_df, out_path)
    log.info(
        "gold.fund_metrics.written",
        path=str(out_path),
        rows=out_df.height,
    )
    _write_quality_report(out_df, as_of, settings)
    return out_path
