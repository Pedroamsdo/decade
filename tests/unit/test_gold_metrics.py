"""Unit tests for fund_rank/gold/_metrics.py."""
from __future__ import annotations

import math
from datetime import date, timedelta

import polars as pl

from fund_rank.gold._metrics import (
    attach_existing_time,
    attach_max_drawdown,
    attach_std_annualized,
    daily_log_returns,
    flag_jumps,
    monthly_returns_from_daily,
)


def _fixture_daily(fund_key: str, dates: list[date], quotas: list[float]) -> pl.DataFrame:
    return pl.DataFrame({
        "fund_key": [fund_key] * len(dates),
        "dt_comptc": dates,
        "vl_quota": quotas,
    })


def test_daily_log_returns_first_row_is_null():
    d = _fixture_daily(
        "F1",
        [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
        [100.0, 101.0, 102.0],
    )
    out = daily_log_returns(d)
    assert out["log_ret"][0] is None
    # log(101/100) ≈ 0.00995
    assert math.isclose(out["log_ret"][1], math.log(101 / 100), abs_tol=1e-9)


def test_daily_log_returns_independent_per_group():
    df = pl.concat([
        _fixture_daily("A", [date(2024, 1, 1), date(2024, 1, 2)], [100.0, 110.0]),
        _fixture_daily("B", [date(2024, 1, 1), date(2024, 1, 2)], [50.0, 55.0]),
    ])
    out = daily_log_returns(df)
    # First row per group must be null
    assert out.filter(pl.col("dt_comptc") == date(2024, 1, 1))["log_ret"].null_count() == 2


def test_flag_jumps_detects_large_outlier():
    # 69 stable cota grow + 1 huge jump on day 70
    start = date(2024, 1, 1)
    dates = [start + timedelta(days=i) for i in range(70)]
    quotas = [100.0 * (1.0001) ** i for i in range(69)]
    quotas.append(quotas[-1] * 2.0)  # 100% jump on the last day
    d = _fixture_daily("F1", dates, quotas)
    daily = daily_log_returns(d)
    flagged = flag_jumps(daily, ret_col="log_ret", window=60, sigma=5.0)
    # The last row should be flagged as a jump
    assert bool(flagged["is_jump"][-1]) is True
    # Steady-state rows shouldn't be flagged
    assert int(flagged.filter(pl.col("dt_comptc") < start + timedelta(days=65))["is_jump"].sum()) == 0


def test_monthly_returns_from_daily_aggregates_to_eom():
    dates = [
        date(2024, 1, 5),
        date(2024, 1, 31),  # end of January (last value of month)
        date(2024, 2, 1),
        date(2024, 2, 29),  # end of February
    ]
    quotas = [100.0, 110.0, 110.0, 121.0]
    d = _fixture_daily("F1", dates, quotas)
    monthly = monthly_returns_from_daily(d)
    # Two months: Jan eom=110, Feb eom=121
    assert monthly.height == 2
    feb = monthly.filter(pl.col("year_month") == date(2024, 2, 1))
    # monthly_ret of Feb = 121/110 - 1 = 0.10
    assert math.isclose(feb["monthly_ret"][0], 0.10, abs_tol=1e-9)


def test_attach_max_drawdown_known_fixture():
    # Cota 100 → 120 (peak) → 90 (trough) → 110.
    # Drawdown at trough = 90/120 - 1 = -0.25
    dates = [date(2024, 1, i) for i in (1, 2, 3, 4)]
    d = _fixture_daily("F1", dates, [100.0, 120.0, 90.0, 110.0])
    dim = pl.DataFrame({"fund_key": ["F1"]})
    out = attach_max_drawdown(dim, d)
    assert math.isclose(out["max_drawdown"][0], -0.25, abs_tol=1e-9)


def test_attach_max_drawdown_monotonically_increasing_is_zero():
    dates = [date(2024, 1, i) for i in (1, 2, 3, 4)]
    d = _fixture_daily("F1", dates, [100.0, 110.0, 121.0, 133.0])
    dim = pl.DataFrame({"fund_key": ["F1"]})
    out = attach_max_drawdown(dim, d)
    assert out["max_drawdown"][0] == 0.0


def test_attach_std_annualized():
    # log returns of [0.01, 0.02, -0.01, 0.005] → std ~ 0.013, annualized * sqrt(252)
    dates = [date(2024, 1, i) for i in (1, 2, 3, 4, 5)]
    d = _fixture_daily(
        "F1",
        dates,
        [100.0, 101.0, 103.02, 101.99, 102.5],
    )
    daily = daily_log_returns(d)
    dim = pl.DataFrame({"fund_key": ["F1"]})
    out = attach_std_annualized(dim, daily)
    expected = daily["log_ret"].std() * (252 ** 0.5)
    assert math.isclose(out["standard_deviation_annualized"][0], expected, rel_tol=1e-6)


def test_attach_existing_time():
    dim = pl.DataFrame({
        "fund_key": ["F1", "F2"],
        "data_de_inicio": [date(2023, 1, 1), date(2024, 6, 15)],
    })
    out = attach_existing_time(dim, as_of=date(2025, 1, 1))
    assert out["existing_time"][0] == 366 + 365  # 2023-01-01 → 2025-01-01: 731
    # Quick sanity: 2024-06-15 → 2025-01-01 = 200 days
    assert out["existing_time"][1] == 200
