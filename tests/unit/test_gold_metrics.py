"""Unit tests for fund_rank/gold/_metrics.py."""
from __future__ import annotations

import math
from datetime import date, timedelta

import polars as pl

from fund_rank.gold._metrics import (
    attach_existing_time,
    attach_information_ratio,
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
    assert math.isclose(out["log_ret"][1], math.log(101 / 100), abs_tol=1e-9)


def test_daily_log_returns_independent_per_group():
    df = pl.concat([
        _fixture_daily("A", [date(2024, 1, 1), date(2024, 1, 2)], [100.0, 110.0]),
        _fixture_daily("B", [date(2024, 1, 1), date(2024, 1, 2)], [50.0, 55.0]),
    ])
    out = daily_log_returns(df)
    assert out.filter(pl.col("dt_comptc") == date(2024, 1, 1))["log_ret"].null_count() == 2


def test_flag_jumps_detects_large_outlier():
    start = date(2024, 1, 1)
    dates = [start + timedelta(days=i) for i in range(70)]
    quotas = [100.0 * (1.0001) ** i for i in range(69)]
    quotas.append(quotas[-1] * 2.0)  # 100% jump no último dia
    d = _fixture_daily("F1", dates, quotas)
    daily = daily_log_returns(d)
    flagged = flag_jumps(daily, ret_col="log_ret", window=60, sigma=5.0)
    assert bool(flagged["is_jump"][-1]) is True
    assert int(flagged.filter(pl.col("dt_comptc") < start + timedelta(days=65))["is_jump"].sum()) == 0


def test_monthly_returns_from_daily_aggregates_to_eom():
    dates = [
        date(2024, 1, 5),
        date(2024, 1, 31),
        date(2024, 2, 1),
        date(2024, 2, 29),
    ]
    quotas = [100.0, 110.0, 110.0, 121.0]
    d = _fixture_daily("F1", dates, quotas)
    monthly = monthly_returns_from_daily(d)
    assert monthly.height == 2
    feb = monthly.filter(pl.col("year_month") == date(2024, 2, 1))
    assert math.isclose(feb["monthly_ret"][0], 0.10, abs_tol=1e-9)


def test_attach_existing_time():
    dim = pl.DataFrame({
        "fund_key": ["F1", "F2"],
        "data_de_inicio": [date(2023, 1, 1), date(2024, 6, 15)],
    })
    out = attach_existing_time(dim, as_of=date(2025, 1, 1))
    assert out["existing_time"][0] == 366 + 365  # 2023-01-01 → 2025-01-01: 731
    assert out["existing_time"][1] == 200


def test_attach_information_ratio_known_fixture():
    # Fundo entrega +0.5% a.m. de excess sobre benchmark com std 0.2% a.m.
    # IR mensal = 0.005 / 0.002 = 2.5; IR anualizado = 2.5 * sqrt(12) ≈ 8.66
    months = [date(2024, m, 1) for m in range(1, 13)]
    monthly = pl.DataFrame({
        "fund_key": ["F1"] * 12,
        "year_month": months,
        "monthly_ret": [0.012, 0.013, 0.014, 0.011, 0.013, 0.015,
                        0.012, 0.013, 0.014, 0.011, 0.013, 0.015],
    })
    bench_monthly = pl.DataFrame({
        "year_month": months,
        "benchmark_code": ["CDI"] * 12,
        "monthly_bench_ret": [0.008, 0.008, 0.008, 0.008, 0.008, 0.008,
                              0.008, 0.008, 0.008, 0.008, 0.008, 0.008],
    })
    dim = pl.DataFrame({"fund_key": ["F1"], "benchmark": ["CDI"]})
    out = attach_information_ratio(dim, monthly, bench_monthly)
    excess = pl.Series([
        0.012 - 0.008, 0.013 - 0.008, 0.014 - 0.008, 0.011 - 0.008,
        0.013 - 0.008, 0.015 - 0.008, 0.012 - 0.008, 0.013 - 0.008,
        0.014 - 0.008, 0.011 - 0.008, 0.013 - 0.008, 0.015 - 0.008,
    ])
    expected_ir = excess.mean() / excess.std() * (12 ** 0.5)
    assert math.isclose(out["information_ratio"][0], expected_ir, rel_tol=1e-6)


def test_attach_information_ratio_returns_null_for_zero_tracking_error():
    # Fundo perfeitamente colado no benchmark: std do excess = 0 → IR é null
    months = [date(2024, m, 1) for m in (1, 2, 3)]
    monthly = pl.DataFrame({
        "fund_key": ["F1"] * 3,
        "year_month": months,
        "monthly_ret": [0.01, 0.01, 0.01],
    })
    bench_monthly = pl.DataFrame({
        "year_month": months,
        "benchmark_code": ["CDI"] * 3,
        "monthly_bench_ret": [0.01, 0.01, 0.01],
    })
    dim = pl.DataFrame({"fund_key": ["F1"], "benchmark": ["CDI"]})
    out = attach_information_ratio(dim, monthly, bench_monthly)
    assert out["information_ratio"][0] is None
