"""gold/ranking — apply scoring rules on top of `gold/fund_metrics`.

Pipeline:

  1. Numerator (`retorno_score`):
     - hit_rate         (positive)
     - sharpe_rolling   (negative — high σ ⇒ inconsistent ⇒ ranks bad)
     - liquid_return_12m (positive)
     Each column: clip 3σ → minmax 0-1 → invert if negative → null=0 → sum → minmax.

  2. Denominator (`risco_score`) — three subgroups, geometric mean:
     - qualidade (fragility): equity, existing_time, net_captation
        clip 3σ → minmax → INVERT (1−x) → null=1 → sum → minmax
     - liquidez: anbima_risk_weight, redemption_days
        clip 3σ → minmax → null=1 → sum → minmax
     - volatilidade: standard_deviation_annualized, |max_drawdown|
        clip 3σ → minmax → null=1 → sum → minmax
     risco_score = (qualidade * liquidez * volatilidade) ** (1/3)

  3. score = retorno_score / (risco_score + 0.01) → minmax → ×100, rounded to 2.

Auditability columns (`retorno_score`, `qualidade`, `liquidez`, `volatilidade`,
`risco_score`) are kept in the output.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.gold._io import gold_path
from fund_rank.gold._scoring import (
    apply_score_pipeline,
    minmax_normalize_expr,
)
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import write_parquet

log = get_logger(__name__)


FINAL_COLUMNS: list[str] = [
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
    "retorno_score",
    "qualidade",
    "liquidez",
    "volatilidade",
    "risco_score_geo",
    "risco_score",
    "score",
]


def _write_quality_report(df: pl.DataFrame, as_of: date, settings: Settings) -> Path:
    rows = df.height

    lines: list[str] = []
    lines.append(f"# gold/ranking — quality report (as_of={as_of.isoformat()})\n")
    lines.append(f"- Rows: **{rows:,}**\n")

    lines.append("## Score distribution\n")
    if rows:
        s = df["score"].drop_nulls()
        if s.len():
            buckets = [
                (0, 20),
                (20, 40),
                (40, 60),
                (60, 80),
                (80, 100.01),
            ]
            lines.append("| bucket | n | pct |")
            lines.append("|---|---|---|")
            for lo, hi in buckets:
                n = int(s.filter((s >= lo) & (s < hi)).len())
                pct = n / rows * 100.0
                hi_str = "100" if hi > 100 else f"{hi:g}"
                lines.append(f"| {lo:g}-{hi_str} | {n:,} | {pct:.2f}% |")
            lines.append("")
            lines.append(
                f"- min/median/mean/max: {s.min():.2f} / {s.median():.2f} / {s.mean():.2f} / {s.max():.2f}"
            )
            lines.append("")

    lines.append("## Nulls and ranges by column\n")
    lines.append("| column | nulls | pct | min | max |")
    lines.append("|---|---|---|---|---|")
    for col in FINAL_COLUMNS:
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
        / "ranking_quality.md"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    log.info("gold.ranking.quality_report", path=str(out), rows=rows)
    return out


def run(settings: Settings, as_of: date) -> Path:
    in_path = gold_path(settings, "fund_metrics", as_of.isoformat())
    if not in_path.exists():
        raise FileNotFoundError(
            f"gold/fund_metrics not found at {in_path}; run build_fund_metrics first."
        )
    metrics = pl.read_parquet(in_path)

    # ---- Numerator: retorno -------------------------------------------------
    metrics = (
        metrics
        .pipe(apply_score_pipeline, "hit_rate", direction="positive", null_value=0.0)
        .pipe(
            apply_score_pipeline,
            "sharpe_rolling",
            direction="negative",
            null_value=0.0,
        )
        .pipe(
            apply_score_pipeline,
            "liquid_return_12m",
            direction="positive",
            null_value=0.0,
        )
    )
    metrics = metrics.with_columns(
        retorno_score=minmax_normalize_expr(
            pl.col("hit_rate_n")
            + pl.col("sharpe_rolling_n")
            + pl.col("liquid_return_12m_n")
        )
    )

    # ---- Denominator: risco -------------------------------------------------
    # Qualidade (fragility): high equity / existing_time / net_captation reduce risk.
    metrics = (
        metrics
        .pipe(
            apply_score_pipeline, "equity", direction="negative", null_value=1.0
        )
        .pipe(
            apply_score_pipeline,
            "existing_time",
            direction="negative",
            null_value=1.0,
        )
        .pipe(
            apply_score_pipeline,
            "net_captation",
            direction="negative",
            null_value=1.0,
        )
    )
    metrics = metrics.with_columns(
        qualidade=minmax_normalize_expr(
            pl.col("equity_n") + pl.col("existing_time_n") + pl.col("net_captation_n")
        )
    )

    # Liquidez: anbima_risk_weight & redemption_days are already "high = bad".
    metrics = (
        metrics
        .pipe(
            apply_score_pipeline,
            "anbima_risk_weight",
            direction="positive",
            null_value=1.0,
        )
        .pipe(
            apply_score_pipeline,
            "redemption_days",
            direction="positive",
            null_value=1.0,
        )
    )
    metrics = metrics.with_columns(
        liquidez=minmax_normalize_expr(
            pl.col("anbima_risk_weight_n") + pl.col("redemption_days_n")
        )
    )

    # Volatilidade: std anualizado já é "high = bad". max_drawdown é negativo,
    # então tomamos o módulo antes de normalizar.
    metrics = metrics.with_columns(
        max_drawdown_abs=pl.col("max_drawdown").abs()
    )
    metrics = (
        metrics
        .pipe(
            apply_score_pipeline,
            "standard_deviation_annualized",
            direction="positive",
            null_value=1.0,
        )
        .pipe(
            apply_score_pipeline,
            "max_drawdown_abs",
            direction="positive",
            null_value=1.0,
        )
    )
    metrics = metrics.with_columns(
        volatilidade=minmax_normalize_expr(
            pl.col("standard_deviation_annualized_n") + pl.col("max_drawdown_abs_n")
        )
    )

    # Risco geométrico = média geométrica dos 3 subgrupos.
    # Por construção fica em [0,1], mas a distribuição costuma ser estreita;
    # re-normalizamos antes da divisão para usar o range completo.
    metrics = metrics.with_columns(
        risco_score_geo=(
            pl.col("qualidade") * pl.col("liquidez") * pl.col("volatilidade")
        ) ** (1.0 / 3.0)
    ).with_columns(
        risco_score=minmax_normalize_expr(pl.col("risco_score_geo"))
    )

    # ---- Score = retorno / (risco_norm + epsilon) → minmax → ×100 ------------
    metrics = metrics.with_columns(
        score_raw=pl.col("retorno_score") / (pl.col("risco_score") + 0.01)
    ).with_columns(
        score=(minmax_normalize_expr(pl.col("score_raw")) * 100.0).round(2)
    )

    out_df = metrics.select(FINAL_COLUMNS).sort("score", descending=True)

    out_path = gold_path(settings, "ranking", as_of.isoformat())
    write_parquet(out_df, out_path)
    log.info(
        "gold.ranking.written",
        path=str(out_path),
        rows=out_df.height,
        score_min=float(out_df["score"].min() or 0.0),
        score_max=float(out_df["score"].max() or 0.0),
    )
    _write_quality_report(out_df, as_of, settings)
    return out_path
