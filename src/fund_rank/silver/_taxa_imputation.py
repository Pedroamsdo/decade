"""Mode-based imputation for `taxa_adm` and `taxa_perform`.

Strategy (per spec):
- Mode (most-frequent value) is the imputation value.
- Outliers are rows where value is **outside** `mean ± 3·std` of the non-null
  population. Nulls AND outliers are replaced with the mode.
- Stats (mode, mean, std) are computed on the **non-null** subset of the
  reference DataFrame.
"""
from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class TaxaStats:
    mode: float | None
    mean: float | None
    std: float | None
    lo: float | None  # mean - 3·std
    hi: float | None  # mean + 3·std


def compute_taxa_stats(df: pl.DataFrame, col: str) -> TaxaStats:
    """Compute mode + mean ± 3·std bounds for `col` over its non-null values."""
    nn = df.filter(pl.col(col).is_not_null())
    if nn.is_empty():
        return TaxaStats(None, None, None, None, None)

    # Mode: most frequent non-null value.
    mode_val = (
        nn.group_by(col)
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .row(0, named=True)[col]
    )
    stats = nn.select(
        pl.col(col).mean().alias("mean"),
        pl.col(col).std().alias("std"),
    ).row(0, named=True)
    mean = stats["mean"]
    std = stats["std"] if stats["std"] is not None else 0.0
    lo = mean - 3.0 * std
    hi = mean + 3.0 * std
    return TaxaStats(mode=mode_val, mean=mean, std=std, lo=lo, hi=hi)


def apply_taxa_imputation(
    df: pl.DataFrame, col: str, stats: TaxaStats
) -> pl.DataFrame:
    """Replace nulls and out-of-bound values in `col` with `stats.mode`.

    No-op if `stats.mode is None` (i.e., reference frame had no non-null data).
    """
    if stats.mode is None:
        return df
    return df.with_columns(
        pl.when(
            pl.col(col).is_null()
            | (pl.col(col) < stats.lo)
            | (pl.col(col) > stats.hi)
        )
        .then(pl.lit(stats.mode))
        .otherwise(pl.col(col))
        .alias(col)
    )
