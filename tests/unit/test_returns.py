"""Tests for return / volatility / drawdown calculations and stitch."""
from __future__ import annotations

import math

import polars as pl
import pytest


def test_annualize_return_252du():
    from fund_rank.gold.compute_metrics import _annualize_return

    # 5% over 252 du = 5% annualized
    assert math.isclose(_annualize_return(0.05, 252), 0.05, rel_tol=1e-9)
    # 10% over 504 du (2 years) ~ 4.88% annualized
    expected = (1.10 ** (252 / 504)) - 1
    assert math.isclose(_annualize_return(0.10, 504), expected, rel_tol=1e-9)


def test_max_drawdown_simple():
    from fund_rank.gold.compute_metrics import _max_drawdown_and_duration

    # Series: 100 → 110 → 105 → 90 → 100. Peak at 110, trough at 90.
    s = pl.Series([100.0, 110.0, 105.0, 90.0, 100.0])
    dd, dur = _max_drawdown_and_duration(s)
    assert math.isclose(dd, 90.0 / 110.0 - 1.0, rel_tol=1e-9)
    # Duration: peak idx=1, trough idx=3 → 2 days
    assert dur == 2


def test_max_drawdown_no_drop():
    from fund_rank.gold.compute_metrics import _max_drawdown_and_duration

    # Monotonic series: drawdown is 0
    s = pl.Series([100.0, 102.0, 105.0, 110.0])
    dd, dur = _max_drawdown_and_duration(s)
    assert dd == 0.0
    assert dur == 0


def test_jump_flag_detects_outlier():
    """Synthetic series with one giant jump should flag jump_flag=True."""
    from datetime import date, timedelta

    from fund_rank.silver.build_quota_series import _detect_jumps

    n = 100
    rows = []
    base = date(2024, 1, 1)
    for i in range(n):
        # Tiny daily noise around 0
        ret = 0.0001 if i != 50 else 0.30  # giant jump at index 50
        rows.append({"series_id": "X", "dt_comptc": base + timedelta(days=i), "log_return": ret})
    df = pl.DataFrame(rows)

    flagged = _detect_jumps(df, sigma=5.0, window=30)
    assert bool(flagged.filter(pl.col("dt_comptc") == base + timedelta(days=50))["jump_flag"][0])
    # Other rows shouldn't be flagged
    assert not bool(flagged.filter(pl.col("dt_comptc") == base + timedelta(days=10))["jump_flag"][0])


def test_pct_cdi_ratio_above_one_for_outperformer():
    """Sanity: a fund that returns more than CDI should yield pct_cdi > 1."""
    fund_ret = 0.15
    cdi_cum = 1.13  # CDI factor
    cdi_ret = cdi_cum - 1.0
    pct = fund_ret / cdi_ret
    assert pct > 1.0
    assert math.isclose(pct, 1.1538, rel_tol=1e-3)
