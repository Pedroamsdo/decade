"""Vectorized financial metric helpers for the gold layer.

All helpers operate on Polars DataFrames assuming `fund_key` (str) is the
unique identifier per investable fund. They never iterate in Python.
"""
from __future__ import annotations

from datetime import date

import polars as pl


# ----------------------------------------------------------------------------
# Daily-frame transforms
# ----------------------------------------------------------------------------


def daily_log_returns(quotas: pl.DataFrame) -> pl.DataFrame:
    """Add a `log_ret` column. First observation per fund is null."""
    return (
        quotas.sort(["fund_key", "dt_comptc"])
        .with_columns(
            log_ret=(
                pl.col("vl_quota") / pl.col("vl_quota").shift(1).over("fund_key")
            ).log()
        )
    )


def flag_jumps(
    daily: pl.DataFrame,
    ret_col: str = "log_ret",
    window: int = 60,
    sigma: float = 5.0,
) -> pl.DataFrame:
    """Flag rows where `|(ret - rolling_mean) / rolling_std| > sigma`."""
    return (
        daily.sort(["fund_key", "dt_comptc"])
        .with_columns(
            _r_mean=pl.col(ret_col).rolling_mean(window_size=window).over("fund_key"),
            _r_std=pl.col(ret_col).rolling_std(window_size=window).over("fund_key"),
        )
        .with_columns(
            _z=pl.when((pl.col("_r_std") > 0) & pl.col("_r_std").is_not_null())
            .then((pl.col(ret_col) - pl.col("_r_mean")) / pl.col("_r_std"))
            .otherwise(0.0)
        )
        .with_columns(is_jump=pl.col("_z").abs() > sigma)
        .drop("_r_mean", "_r_std", "_z")
    )


def monthly_returns_from_daily(daily: pl.DataFrame) -> pl.DataFrame:
    """Reduce daily quotas to month-end `vl_quota`, then compute monthly returns.

    Output: `fund_key`, `year_month` (Date), `vl_quota_eom`, `monthly_ret`.
    """
    return (
        daily.with_columns(year_month=pl.col("dt_comptc").dt.truncate("1mo"))
        .sort(["fund_key", "year_month", "dt_comptc"])
        .group_by(["fund_key", "year_month"], maintain_order=True)
        .agg(vl_quota_eom=pl.col("vl_quota").last())
        .sort(["fund_key", "year_month"])
        .with_columns(
            monthly_ret=pl.col("vl_quota_eom").pct_change().over("fund_key")
        )
    )


# ----------------------------------------------------------------------------
# Per-fund metric attachments
# ----------------------------------------------------------------------------


def attach_information_ratio(
    dim_fund: pl.DataFrame,
    monthly: pl.DataFrame,
    bench_monthly: pl.DataFrame,
) -> pl.DataFrame:
    """Annualized Information Ratio vs canonical benchmark.

    IR = mean(monthly_ret_fund − monthly_ret_bench) / std(...) × √12

    Uses each fund's canonical benchmark code (CDI, IPCA, IMA-B, ...). Funds
    with zero tracking-error or fewer than 2 valid months get null IR.
    NaN/inf sanitized to null.
    """
    fund_bench = dim_fund.select(
        "fund_key", pl.col("benchmark").alias("benchmark_code")
    )
    enriched = (
        monthly.join(fund_bench, on="fund_key", how="left")
        .join(bench_monthly, on=["year_month", "benchmark_code"], how="left")
        .filter(
            pl.col("monthly_ret").is_not_null()
            & pl.col("monthly_bench_ret").is_not_null()
        )
        .with_columns(excess=pl.col("monthly_ret") - pl.col("monthly_bench_ret"))
    )
    ir = (
        enriched.group_by("fund_key")
        .agg(
            _mean=pl.col("excess").mean(),
            _std=pl.col("excess").std(),
        )
        .with_columns(
            _raw=pl.when((pl.col("_std").is_not_null()) & (pl.col("_std") > 0))
            .then(pl.col("_mean") / pl.col("_std") * (12.0 ** 0.5))
            .otherwise(None)
        )
        .with_columns(
            information_ratio=pl.when(pl.col("_raw").is_finite())
            .then(pl.col("_raw"))
            .otherwise(None)
        )
        .select("fund_key", "information_ratio")
    )
    return dim_fund.join(ir, on="fund_key", how="left")


def attach_equity(dim_fund: pl.DataFrame, quotas: pl.DataFrame) -> pl.DataFrame:
    """Latest non-null `vl_patrim_liq` per fund_key."""
    last_pl = (
        quotas.filter(pl.col("vl_patrim_liq").is_not_null())
        .sort(["fund_key", "dt_comptc"])
        .group_by("fund_key", maintain_order=True)
        .agg(equity=pl.col("vl_patrim_liq").last())
    )
    return dim_fund.join(last_pl, on="fund_key", how="left")


def attach_nr_cotst(dim_fund: pl.DataFrame, quotas: pl.DataFrame) -> pl.DataFrame:
    """Latest non-null `nr_cotst` per fund_key. Fills 0 for funds without quotes."""
    last_n = (
        quotas.filter(pl.col("nr_cotst").is_not_null())
        .sort(["fund_key", "dt_comptc"])
        .group_by("fund_key", maintain_order=True)
        .agg(nr_cotst=pl.col("nr_cotst").last())
    )
    return dim_fund.join(last_n, on="fund_key", how="left").with_columns(
        nr_cotst=pl.col("nr_cotst").fill_null(0).cast(pl.Int64)
    )


def attach_existing_time(dim_fund: pl.DataFrame, as_of: date) -> pl.DataFrame:
    """Days between `data_de_inicio` and `as_of`. Clipped to ≥ 0."""
    raw_days = (
        pl.lit(as_of).cast(pl.Date) - pl.col("data_de_inicio")
    ).dt.total_days().cast(pl.Int64)
    return dim_fund.with_columns(
        existing_time=pl.when(raw_days < 0).then(0).otherwise(raw_days)
    )
