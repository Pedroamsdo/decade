"""Vectorized financial metric helpers for the gold layer.

All helpers operate on Polars DataFrames assuming `fund_key` (str) is the
unique identifier per investable fund (one row per class without subclasses,
one row per subclass). They never iterate in Python.

Contracts:
- `daily_log_returns` / `flag_jumps` / `monthly_returns_from_daily` produce
  intermediate frames consumed by `attach_*`.
- Each `attach_*` function takes `dim_fund` (one row per fund_key) and a
  daily/monthly frame, computes one column, and returns `dim_fund` with the
  new column joined left.
"""
from __future__ import annotations

from datetime import date

import polars as pl


# ----------------------------------------------------------------------------
# Daily-frame transforms
# ----------------------------------------------------------------------------


def daily_log_returns(quotas: pl.DataFrame) -> pl.DataFrame:
    """Add a `log_ret` column to a daily quotas frame keyed by `fund_key`.

    The first observation per fund has `log_ret = null`.
    """
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
    """Add `is_jump` boolean per row.

    A row is flagged when `|(ret - rolling_mean) / rolling_std| > sigma`,
    using a `window`-day rolling stats by `fund_key`.
    Rows with `rolling_std == 0` or null are not flagged.
    """
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

    Output columns: `fund_key`, `year_month` (Date, first day of month),
    `vl_quota_eom`, `monthly_ret`.
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
# Per-fund metric attachments — each returns dim_fund + 1 new column
# ----------------------------------------------------------------------------


def attach_hit_rate(
    dim_fund: pl.DataFrame,
    monthly: pl.DataFrame,
    bench_monthly: pl.DataFrame,
) -> pl.DataFrame:
    """% of months where `monthly_ret > monthly_bench_ret` for each fund.

    Uses each fund's canonical benchmark code (CDI, IPCA, IMA-B, …).
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
    )
    hit = enriched.group_by("fund_key").agg(
        hit_rate=(pl.col("monthly_ret") > pl.col("monthly_bench_ret"))
        .cast(pl.Float64)
        .mean()
    )
    return dim_fund.join(hit, on="fund_key", how="left")


def attach_cagr(dim_fund: pl.DataFrame, daily: pl.DataFrame) -> pl.DataFrame:
    """Compound Annual Growth Rate over each fund's full quote history.

    cagr = (vl_quota_last / vl_quota_first) ** (1 / years) - 1
    where `years = (date_last - date_first) / 365.25`. Returns null for
    funds with `years <= 0`, `vl_quota_first <= 0`, or NaN/inf result.
    """
    boundaries = (
        daily.filter(pl.col("vl_quota").is_not_null())
        .sort(["fund_key", "dt_comptc"])
        .group_by("fund_key", maintain_order=True)
        .agg(
            quota_first=pl.col("vl_quota").first(),
            quota_last=pl.col("vl_quota").last(),
            date_first=pl.col("dt_comptc").first(),
            date_last=pl.col("dt_comptc").last(),
        )
        .with_columns(
            years=(
                pl.col("date_last") - pl.col("date_first")
            ).dt.total_days().cast(pl.Float64) / 365.25
        )
        .with_columns(
            _raw=pl.when((pl.col("years") > 0) & (pl.col("quota_first") > 0))
            .then(
                (pl.col("quota_last") / pl.col("quota_first"))
                ** (1.0 / pl.col("years"))
                - 1.0
            )
            .otherwise(None)
        )
        .with_columns(
            cagr=pl.when(pl.col("_raw").is_finite())
            .then(pl.col("_raw"))
            .otherwise(None)
        )
        .select("fund_key", "cagr")
    )
    return dim_fund.join(boundaries, on="fund_key", how="left")


def attach_cv_metric(
    dim_fund: pl.DataFrame, monthly: pl.DataFrame
) -> pl.DataFrame:
    """Coefficient of variation of the fund's monthly returns:
    `cv_metric = std(monthly_ret) / |mean(monthly_ret)|`.

    Funds with `|mean| < 1e-10` (essentially zero) get null to avoid blow-up.
    NaN/inf are sanitized to null.
    """
    cv = (
        monthly.filter(pl.col("monthly_ret").is_not_null())
        .group_by("fund_key")
        .agg(
            _mean=pl.col("monthly_ret").mean(),
            _std=pl.col("monthly_ret").std(),
        )
        .with_columns(
            _raw=pl.when(
                (pl.col("_mean").abs() > 1e-10) & pl.col("_std").is_not_null()
            )
            .then(pl.col("_std") / pl.col("_mean").abs())
            .otherwise(None)
        )
        .with_columns(
            cv_metric=pl.when(pl.col("_raw").is_finite())
            .then(pl.col("_raw"))
            .otherwise(None)
        )
        .select("fund_key", "cv_metric")
    )
    return dim_fund.join(cv, on="fund_key", how="left")


def attach_equity(dim_fund: pl.DataFrame, quotas: pl.DataFrame) -> pl.DataFrame:
    """Latest non-null `vl_patrim_liq` per fund_key (≤ as_of)."""
    last_pl = (
        quotas.filter(pl.col("vl_patrim_liq").is_not_null())
        .sort(["fund_key", "dt_comptc"])
        .group_by("fund_key", maintain_order=True)
        .agg(equity=pl.col("vl_patrim_liq").last())
    )
    return dim_fund.join(last_pl, on="fund_key", how="left")


def attach_nr_cotst(dim_fund: pl.DataFrame, quotas: pl.DataFrame) -> pl.DataFrame:
    """Latest non-null `nr_cotst` per fund_key (≤ as_of). Funds with no quotes
    get `nr_cotst = 0`."""
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
    """Days between `data_de_inicio` and `as_of`. Clipped to ≥ 0.

    Funds whose `data_de_inicio` is after `as_of` (registered after the ranking
    reference date) get `existing_time = 0` rather than a negative number.
    """
    raw_days = (
        pl.lit(as_of).cast(pl.Date) - pl.col("data_de_inicio")
    ).dt.total_days().cast(pl.Int64)
    return dim_fund.with_columns(
        existing_time=pl.when(raw_days < 0).then(0).otherwise(raw_days)
    )


def attach_anbima_risk_weight(
    dim_fund: pl.DataFrame, weights: dict[str, float]
) -> pl.DataFrame:
    """Lookup `classificacao_anbima` → `anbima_risk_weight` from a dict mapping."""
    return dim_fund.with_columns(
        anbima_risk_weight=pl.col("classificacao_anbima")
        .replace_strict(weights, default=None, return_dtype=pl.Float64)
    )


def attach_max_drawdown(
    dim_fund: pl.DataFrame, daily: pl.DataFrame
) -> pl.DataFrame:
    """Maximum drawdown per fund_key over the entire daily history.

    Returns a non-positive number (the deepest valley relative to a prior peak).
    Funds with a single observation get `max_drawdown = 0.0`.
    """
    sorted_d = (
        daily.sort(["fund_key", "dt_comptc"])
        .with_columns(
            _cum_quota=pl.col("vl_quota")
            / pl.col("vl_quota").first().over("fund_key")
        )
        .with_columns(
            _peak=pl.col("_cum_quota").cum_max().over("fund_key")
        )
        .with_columns(
            _drawdown=pl.when(pl.col("_peak") > 0)
            .then(pl.col("_cum_quota") / pl.col("_peak") - 1.0)
            .otherwise(None)
        )
    )
    mdd = (
        sorted_d.group_by("fund_key")
        .agg(_raw=pl.col("_drawdown").min())
        .with_columns(
            max_drawdown=pl.when(pl.col("_raw").is_finite())
            .then(pl.col("_raw"))
            .otherwise(None)
        )
        .drop("_raw")
    )
    return dim_fund.join(mdd, on="fund_key", how="left")
