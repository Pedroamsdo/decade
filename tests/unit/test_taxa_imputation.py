"""Unit tests for silver/_taxa_imputation."""
from __future__ import annotations

import polars as pl
import pytest


def test_compute_stats_picks_most_frequent_value_as_mode():
    from fund_rank.silver._taxa_imputation import compute_taxa_stats

    df = pl.DataFrame({"taxa_adm": [0.0, 0.0, 0.0, 0.005, 0.01]})
    stats = compute_taxa_stats(df, "taxa_adm")
    assert stats.mode == 0.0


def test_compute_stats_excludes_nulls_from_mode_and_stats():
    from fund_rank.silver._taxa_imputation import compute_taxa_stats

    df = pl.DataFrame({"taxa_adm": [None, 0.005, 0.005, None, 0.01]})
    stats = compute_taxa_stats(df, "taxa_adm")
    assert stats.mode == 0.005
    # mean of {0.005, 0.005, 0.01} = 0.0066...
    assert stats.mean == pytest.approx(0.006666666, rel=1e-3)


def test_compute_stats_bounds_are_mean_plus_minus_3_sigma():
    from fund_rank.silver._taxa_imputation import compute_taxa_stats

    df = pl.DataFrame({"taxa_adm": [0.0, 0.005, 0.01, 0.02, 10.0]})
    stats = compute_taxa_stats(df, "taxa_adm")
    assert stats.lo == pytest.approx(stats.mean - 3 * stats.std)
    assert stats.hi == pytest.approx(stats.mean + 3 * stats.std)


def test_apply_imputation_replaces_nulls_with_mode():
    from fund_rank.silver._taxa_imputation import (
        TaxaStats,
        apply_taxa_imputation,
    )

    stats = TaxaStats(mode=0.005, mean=0.005, std=0.001, lo=0.002, hi=0.008)
    df = pl.DataFrame({"taxa_adm": [None, 0.005, None]})
    out = apply_taxa_imputation(df, "taxa_adm", stats)
    assert out["taxa_adm"].to_list() == [0.005, 0.005, 0.005]


def test_apply_imputation_replaces_outliers_above_and_below():
    from fund_rank.silver._taxa_imputation import (
        TaxaStats,
        apply_taxa_imputation,
    )

    stats = TaxaStats(mode=0.005, mean=0.005, std=0.001, lo=0.002, hi=0.008)
    df = pl.DataFrame({"taxa_adm": [0.0, 0.005, 0.01]})  # 0.0 < lo, 0.01 > hi
    out = apply_taxa_imputation(df, "taxa_adm", stats)
    assert out["taxa_adm"].to_list() == [0.005, 0.005, 0.005]


def test_apply_imputation_keeps_in_range_values():
    from fund_rank.silver._taxa_imputation import (
        TaxaStats,
        apply_taxa_imputation,
    )

    stats = TaxaStats(mode=0.005, mean=0.005, std=0.001, lo=0.002, hi=0.008)
    df = pl.DataFrame({"taxa_adm": [0.003, 0.005, 0.007]})  # all in range
    out = apply_taxa_imputation(df, "taxa_adm", stats)
    assert out["taxa_adm"].to_list() == [0.003, 0.005, 0.007]


def test_apply_imputation_noop_when_reference_all_null():
    from fund_rank.silver._taxa_imputation import (
        compute_taxa_stats,
        apply_taxa_imputation,
    )

    ref = pl.DataFrame({"taxa_adm": [None, None]}, schema={"taxa_adm": pl.Float64})
    stats = compute_taxa_stats(ref, "taxa_adm")
    assert stats.mode is None

    df = pl.DataFrame({"taxa_adm": [None, 0.01]})
    out = apply_taxa_imputation(df, "taxa_adm", stats)
    # No-op: nulls and values pass through unchanged.
    assert out["taxa_adm"].to_list() == [None, 0.01]
