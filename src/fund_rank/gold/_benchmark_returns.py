"""Build monthly benchmark returns from `silver/index_series` for gold layer.

`index_series` is a wide table with 11 indices in 3 different units:
  - `percent_per_day` (CDI, SELIC) — daily rate; monthly return = prod(1+r)−1.
  - `percent_per_month` (IPCA, INPC, IGP-M) — already monthly variation.
  - `index_level` (IMA-B, IMA-B 5, IMA-B 5+, IMA-GERAL, IMA-S, IRF-M) —
    end-of-month level; monthly return = level[m]/level[m-1] − 1.

Output is a long DataFrame: (year_month, benchmark_code, monthly_bench_ret).
`benchmark_code` matches the canonical codes assigned by
`silver/_benchmark_mapping.py`: CDI, IPCA, INPC, IGP-M, IMA-B, IMA-B 5,
IMA-B 5+, IMA-GERAL, IMA-S, IRF-M (10 codes). SELIC is omitted because the
treated benchmark mapping never assigns it (folds into CDI).
"""
from __future__ import annotations

import polars as pl


# Mapping: silver/index_series column → (benchmark canonical code, unit)
INDEX_TO_BENCHMARK: dict[str, tuple[str, str]] = {
    "cdi":         ("CDI",       "percent_per_day"),
    "ipca":        ("IPCA",      "percent_per_month"),
    "inpc":        ("INPC",      "percent_per_month"),
    "igpm":        ("IGP-M",     "percent_per_month"),
    "ima_b":       ("IMA-B",     "index_level"),
    "ima_b_5":     ("IMA-B 5",   "index_level"),
    "ima_b_5plus": ("IMA-B 5+",  "index_level"),
    "ima_geral":   ("IMA-GERAL", "index_level"),
    "ima_s":       ("IMA-S",     "index_level"),
    "irf_m":       ("IRF-M",     "index_level"),
}


def monthly_benchmark_returns(index_series: pl.DataFrame) -> pl.DataFrame:
    """Build a long DF with columns: year_month (Date), benchmark_code (str),
    monthly_bench_ret (Float64).

    `year_month` is the first day of the month (date-truncated).
    """
    df = index_series.with_columns(year_month=pl.col("data").dt.truncate("1mo"))

    pieces: list[pl.DataFrame] = []
    for col, (code, unit) in INDEX_TO_BENCHMARK.items():
        if col not in df.columns:
            continue
        if unit == "percent_per_day":
            # CDI / SELIC: BCB SGS publishes the daily rate in PERCENT
            # (e.g. 0.0551 = 0.0551% per day, which is ≈ 14.9% a.a. composed).
            # We must divide by 100 to convert to fraction before compounding.
            # Monthly compound: prod(1 + r_d/100) - 1.
            piece = (
                df.filter(pl.col(col).is_not_null())
                  .group_by("year_month")
                  .agg(monthly_bench_ret=((pl.col(col) / 100.0 + 1.0).product() - 1.0))
            )
        elif unit == "percent_per_month":
            # IPCA / INPC / IGP-M: published once per month as percent (e.g. 0.62).
            # Convert to fraction; pick the (only) non-null observation in the month.
            piece = (
                df.filter(pl.col(col).is_not_null())
                  .group_by("year_month")
                  .agg(monthly_bench_ret=(pl.col(col).first() / 100.0))
            )
        else:  # index_level
            # IMA-* / IRF-M: take last level of each month, then pct_change.
            eom = (
                df.filter(pl.col(col).is_not_null())
                  .sort(["year_month", "data"])
                  .group_by("year_month", maintain_order=True)
                  .agg(level=pl.col(col).last())
                  .sort("year_month")
                  .with_columns(monthly_bench_ret=pl.col("level").pct_change())
            )
            piece = eom.select("year_month", "monthly_bench_ret").drop_nulls("monthly_bench_ret")
        piece = piece.with_columns(benchmark_code=pl.lit(code)).select(
            "year_month", "benchmark_code", "monthly_bench_ret"
        )
        pieces.append(piece)

    return pl.concat(pieces, how="vertical").sort(["benchmark_code", "year_month"])
