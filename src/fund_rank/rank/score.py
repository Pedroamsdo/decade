"""rank/score — z-score intra-segmento + weighted sum.

For each segment:
  1. Read gold/fund_metrics/segment=<seg>.
  2. For each metric in configs/scoring.yaml#weights[seg], standardize via z-score.
  3. Apply direction (positive/negative) and weight.
  4. Sum to a single score per fund.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings

log = get_logger(__name__)


def _zscore(col: pl.Expr) -> pl.Expr:
    """Robust z-score: subtract median, divide by IQR/1.349 (≈ std under normal). Falls back to mean/std if IQR=0."""
    return (
        ((col - col.median()) / (col.std() + 1e-12))
        .fill_null(0.0)
    )


def score_segment(
    metrics: pl.DataFrame,
    weights: dict[str, float],
    directions: dict[str, str],
) -> pl.DataFrame:
    """Returns metrics + score column (and z columns for transparency)."""
    score_expr = pl.lit(0.0)
    z_cols: list[pl.Expr] = []
    for metric, w in weights.items():
        if metric not in metrics.columns:
            log.warning("rank.score.missing_metric", metric=metric)
            continue
        direction = directions.get(metric, "positive")
        sign = -1.0 if direction == "negative" else 1.0
        z = _zscore(pl.col(metric)) * sign
        z_cols.append(z.alias(f"_z_{metric}"))
        score_expr = score_expr + w * z

    return metrics.with_columns(*z_cols, score_expr.alias("score"))


def run(settings: Settings, as_of: date) -> dict[str, Path]:
    metrics_root = settings.gold_root / "fund_metrics" / f"as_of={as_of.isoformat()}"
    if not metrics_root.exists():
        raise FileNotFoundError(f"No gold/fund_metrics at {metrics_root}; run `build` first.")

    scoring = settings.scoring
    directions = scoring.get("metric_directions", {})
    weights_per_seg = scoring.get("weights", {})

    out_paths: dict[str, Path] = {}
    out_root = settings.gold_root / "ranking" / f"as_of={as_of.isoformat()}"
    out_root.mkdir(parents=True, exist_ok=True)

    for seg_dir in sorted(metrics_root.iterdir()):
        if not seg_dir.is_dir() or not seg_dir.name.startswith("segment="):
            continue
        seg_id = seg_dir.name.split("=", 1)[1]
        weights = weights_per_seg.get(seg_id)
        if not weights:
            log.warning("rank.no_weights_for_segment", segment=seg_id)
            continue

        metrics = pl.read_parquet(seg_dir / "data.parquet")
        scored = score_segment(metrics, weights, directions)

        top_n = scoring.get("selection", {}).get("top_n", 5)
        tiebreak = [c for c in scoring.get("selection", {}).get("tiebreak_columns", []) if c in scored.columns]
        sort_cols = ["score", *tiebreak]
        # score: descending (higher is better). Tie-break columns: lower-is-better if direction=negative,
        # else higher-is-better.
        sort_desc = [True] + [False if directions.get(c) == "negative" else True for c in tiebreak]

        ranked = (
            scored.sort(by=sort_cols, descending=sort_desc, nulls_last=True)
            .with_row_index("rank", offset=1)
        )
        out = out_root / f"segment={seg_id}" / "data.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        ranked.write_parquet(out, compression="zstd")
        out_paths[seg_id] = out
        log.info(
            "rank.segment.scored",
            segment=seg_id,
            ranked=len(ranked),
            top_n=top_n,
            path=str(out),
        )

    return out_paths
