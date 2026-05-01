"""gold/fund_metrics — one row per investable fund, raw metrics + final score.

A "fund" is a class without subclasses OR a subclass. Construction:

  1. Build `dim_fund` by vertical-stacking class_funds_fixed_income_treated
     and subclass_funds_fixed_income_treated, with a synthetic `fund_key`
     and quota-series join keys (used internally only).
  2. Filter `silver/quota_series` to `dt_comptc <= as_of`, attach `fund_key`.
  3. Compute log returns, drop ±5σ jumps (universe.yaml#jump_detection_sigma),
     reduce to monthly returns, build canonical monthly benchmark returns.
  4. Attach 8 raw metrics, then compute the `score` (0–100) inline.
  5. Drop internal helpers (`fund_key`, `cnpj_fundo`, `classificacao_anbima`,
     scoring auxiliaries) and write parquet + quality report.

Output: 14 columns documented in `docs/data_contracts.md` (Gold layer).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.gold._benchmark_returns import monthly_benchmark_returns
from fund_rank.gold._io import gold_path
from fund_rank.gold._metrics import (
    attach_cagr,
    attach_cv_metric,
    attach_equity,
    attach_existing_time,
    attach_hit_rate,
    attach_max_drawdown,
    attach_nr_cotst,
    daily_log_returns,
    flag_jumps,
    monthly_returns_from_daily,
)
from fund_rank.gold._scoring import apply_score_pipeline, minmax_normalize_expr
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
    "hit_rate",
    "cagr",
    "cv_metric",
    "max_drawdown",
    "score",
]


CLASSE_SENTINEL = "__CLASSE__"


def _build_dim_fund(cls: pl.DataFrame, sub: pl.DataFrame) -> pl.DataFrame:
    """Vertical-stack class + subclass dim tables with a synthetic `fund_key`."""
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
    """Inner-join quotas with dim_fund on (cnpj_fundo_classe_join, id_subclasse_join)."""
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
    """Apply the scoring pipeline:

    Numerator (retorno):
      hit_rate (positive, null=0) + cagr (positive, null=0)
        → minmax → retorno_score ∈ [0,1].

    Denominator (risco) = qualidade × volatilidade:
      qualidade    = minmax(equity_inv + existing_time_inv)        # null=0 after invert
      volatilidade = minmax(cv_metric_n + max_drawdown_inv)        # cv null=1, dd null=0

    Final:
      score_raw = retorno_score / risco_score  (0 if risco_score == 0)
      Outliers (|z|>3 over score_raw[eligible]) zeroed before minmax.
      score = round(minmax(score_raw[eligible]) × 100, 2), null fora dos elegíveis.
    """
    df = metrics

    # Retorno (positive direction, null=0)
    df = apply_score_pipeline(df, "hit_rate", direction="positive", null_value=0.0)
    df = apply_score_pipeline(df, "cagr",     direction="positive", null_value=0.0)
    df = df.with_columns(
        retorno_score=minmax_normalize_expr(
            pl.col("hit_rate_n") + pl.col("cagr_n")
        )
    )

    # Qualidade (high equity/idade = good → invert; null=0 after invert)
    df = apply_score_pipeline(df, "equity",        direction="negative", null_value=0.0)
    df = apply_score_pipeline(df, "existing_time", direction="negative", null_value=0.0)
    df = df.with_columns(
        qualidade=minmax_normalize_expr(
            pl.col("equity_n") + pl.col("existing_time_n")
        )
    )

    # Volatilidade (cv high = bad, null=1; max_drawdown is negative so invert with null=0)
    df = apply_score_pipeline(df, "cv_metric",    direction="positive", null_value=1.0)
    df = apply_score_pipeline(df, "max_drawdown", direction="negative", null_value=0.0)
    df = df.with_columns(
        volatilidade=minmax_normalize_expr(
            pl.col("cv_metric_n") + pl.col("max_drawdown_n")
        )
    )

    # Risco final = produto direto dos 2 subgrupos (qualidade × volatilidade).
    df = df.with_columns(
        risco_score=pl.col("qualidade") * pl.col("volatilidade")
    )

    # score_raw: retorno/risco com guard (0 quando risco == 0)
    df = df.with_columns(
        score_raw=pl.when(pl.col("risco_score") > 0)
        .then(pl.col("retorno_score") / pl.col("risco_score"))
        .otherwise(0.0)
    )

    # Filtro de outliers do score_raw: valores fora de mean ± 3·σ (computados
    # sobre o universo elegível) são tratados como 0. Isso evita que fundos
    # com risco quase-zero (mas não exatamente zero) puxem a escala para si e
    # comprimam todo o resto contra o piso.
    eligible_expr = pl.col("situacao") == "Em Funcionamento Normal"
    eligible_score_raw = pl.col("score_raw").filter(eligible_expr)
    mean_raw = eligible_score_raw.mean()
    std_raw = eligible_score_raw.std()
    lo_raw = mean_raw - 3.0 * std_raw
    hi_raw = mean_raw + 3.0 * std_raw

    df = df.with_columns(
        score_raw=pl.when(
            eligible_expr
            & ((pl.col("score_raw") < lo_raw) | (pl.col("score_raw") > hi_raw))
        )
        .then(pl.lit(0.0))
        .otherwise(pl.col("score_raw"))
    )

    # Re-normalização do score APENAS dentro do universo elegível.
    # Min/max recalculados sobre o score_raw já com outliers zerados.
    eligible_score_raw_clean = pl.col("score_raw").filter(eligible_expr)
    mn_e = eligible_score_raw_clean.min()
    mx_e = eligible_score_raw_clean.max()
    rng_e = mx_e - mn_e

    df = df.with_columns(
        score=pl.when(eligible_expr)
        .then(
            pl.when(rng_e == 0)
            .then(pl.lit(50.0))
            .otherwise(((pl.col("score_raw") - mn_e) / rng_e) * 100.0)
            .round(2)
        )
        .otherwise(None)
    )

    return df


def _write_quality_report(df: pl.DataFrame, as_of: date, settings: Settings) -> Path:
    rows = df.height
    distinct = (
        df.select("cnpj_classe", "id_subclasse_cvm").unique().height if rows else 0
    )
    dups = rows - distinct

    lines: list[str] = []
    lines.append(f"# gold/fund_metrics — quality report (as_of={as_of.isoformat()})\n")
    lines.append(f"- Rows: **{rows:,}**")
    lines.append(f"- Distinct (cnpj_classe, id_subclasse_cvm): **{distinct:,}**")
    lines.append(f"- Duplicates by composite key: **{dups:,}**\n")

    if "score" in df.columns and rows:
        s = df["score"].drop_nulls()
        if s.len():
            lines.append("## Score distribution\n")
            buckets = [(0, 20), (20, 40), (40, 60), (60, 80), (80, 100.01)]
            lines.append("| bucket | n | pct |")
            lines.append("|---|---|---|")
            for lo, hi in buckets:
                n = int(s.filter((s >= lo) & (s < hi)).len())
                pct = n / rows * 100.0
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

    # 2. Build dim_fund
    dim_fund = _build_dim_fund(cls, sub)
    log.info(
        "gold.fund_metrics.dim_fund_built",
        funds=dim_fund.height,
        classes=cls.height,
        subclasses=sub.height,
    )

    # 3. Quotas ≤ as_of, attach fund_key
    quotas = quotas.filter(pl.col("dt_comptc") <= as_of)
    quotas_keyed = _attach_fund_key(quotas, dim_fund)
    log.info(
        "gold.fund_metrics.quotas_attached",
        rows_in=quotas.height,
        rows_keyed=quotas_keyed.height,
        distinct_funds_with_quotas=quotas_keyed["fund_key"].n_unique(),
    )

    # 4. Daily log returns + jump filter
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

    # 5. Monthly aggregates + benchmark mensal
    monthly = monthly_returns_from_daily(daily_clean)
    bench_monthly = monthly_benchmark_returns(indices)

    # 6. Raw metrics
    metrics = (
        dim_fund
        .pipe(attach_hit_rate, monthly, bench_monthly)
        .pipe(attach_cagr, daily_clean)
        .pipe(attach_equity, quotas_keyed)
        .pipe(attach_nr_cotst, quotas_keyed)
        .pipe(attach_existing_time, as_of)
        .pipe(attach_cv_metric, monthly)
        .pipe(attach_max_drawdown, daily_clean)
    )

    # 7. Compute score inline
    metrics = _compute_score(metrics)
    log.info(
        "gold.fund_metrics.scored",
        score_min=float(metrics["score"].min() or 0.0),
        score_max=float(metrics["score"].max() or 0.0),
        score_zeros=int(metrics.filter(pl.col("score") == 0).height),
    )

    # 9. Final schema (drop internals)
    out_df = metrics.select(OUTPUT_COLUMNS).sort("score", descending=True)

    # 10. Write parquet + quality report
    out_path = gold_path(settings, "fund_metrics", as_of.isoformat())
    write_parquet(out_df, out_path)
    log.info(
        "gold.fund_metrics.written",
        path=str(out_path),
        rows=out_df.height,
    )
    _write_quality_report(out_df, as_of, settings)
    return out_path
