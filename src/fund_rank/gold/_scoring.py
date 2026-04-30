"""Scoring helpers — clip outliers, normalize 0-1, geometric mean.

All helpers are vectorized Polars expressions. They never materialize
intermediate Python lists.

Conventions:
- `clip_3sigma`: clips a column at `mean ± 3·std` of its non-null values.
- `minmax_normalize`: scales clipped values to [0,1]. Returns 0.5 if the
  whole column is constant (max == min).
- `apply_score_pipeline`: end-to-end (clip → minmax → optional invert →
  fill nulls). Writes a new column `f"{col}_n"` (or `output_col` if given).
- `geometric_mean`: row-wise (col1 * col2 * ... * colN) ** (1/N).
"""
from __future__ import annotations

import polars as pl


def clip_3sigma_expr(col: str) -> pl.Expr:
    """Polars expression: clip `col` to mean ± 3·std of its non-null values."""
    c = pl.col(col)
    mean = c.mean()
    std = c.std()
    lo = mean - 3.0 * std
    hi = mean + 3.0 * std
    return (
        pl.when(c < lo).then(lo)
        .when(c > hi).then(hi)
        .otherwise(c)
    )


def minmax_normalize_expr(clipped: pl.Expr) -> pl.Expr:
    """Polars expression: minmax normalize `clipped` to [0,1].

    If max == min (degenerate case), returns 0.5.
    """
    mn = clipped.min()
    mx = clipped.max()
    rng = mx - mn
    return (
        pl.when(rng == 0)
        .then(pl.lit(0.5))
        .otherwise((clipped - mn) / rng)
    )


def apply_score_pipeline(
    df: pl.DataFrame,
    col: str,
    direction: str = "positive",
    null_value: float = 0.0,
    output_col: str | None = None,
) -> pl.DataFrame:
    """Clip 3σ → minmax to [0,1] → invert if `direction='negative'` → fill nulls.

    Parameters
    ----------
    direction : "positive" (high = good) keeps the orientation;
                "negative" (high = bad) returns `1 - normalized`.
    null_value : value used to fill nulls **after** normalization.
                 Use 0.0 for return-side columns (penalize missing data),
                 1.0 for risk-side columns (treat missing as max risk).
    """
    if direction not in {"positive", "negative"}:
        raise ValueError(f"direction must be 'positive' or 'negative', got {direction!r}")

    out = output_col or f"{col}_n"

    clipped = clip_3sigma_expr(col)
    normed = minmax_normalize_expr(clipped)
    if direction == "negative":
        normed = 1.0 - normed
    final = normed.fill_null(null_value)

    return df.with_columns(final.alias(out))


def geometric_mean_expr(*cols: str) -> pl.Expr:
    """Polars expression: geometric mean of `cols` row-wise."""
    if not cols:
        raise ValueError("geometric_mean_expr requires at least one column")
    n = len(cols)
    product = pl.col(cols[0])
    for c in cols[1:]:
        product = product * pl.col(c)
    return product ** (1.0 / n)
