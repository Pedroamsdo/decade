"""Unit tests for fund_rank/gold/_scoring.py."""
from __future__ import annotations

import math

import polars as pl
import pytest

from fund_rank.gold._scoring import (
    apply_score_pipeline,
    clip_3sigma_expr,
    geometric_mean_expr,
    minmax_normalize_expr,
)


def test_clip_3sigma_clips_extreme_values():
    # 50 ones + 1 outlier of 100. mean ≈ 2.94, std ≈ 13.74, hi ≈ 44.16; 100 clips to hi.
    values = [1.0] * 50 + [100.0]
    df = pl.DataFrame({"x": values})
    out = df.with_columns(clip_3sigma_expr("x").alias("x_c"))
    mean = float(df["x"].mean())
    std = float(df["x"].std())
    hi = mean + 3 * std
    assert hi < 100.0  # the outlier really is outside the band
    assert math.isclose(out["x_c"].max(), hi, abs_tol=1e-6)
    # Non-outliers stay equal to their original value
    assert out["x_c"][0] == 1.0


def test_minmax_normalize_basic():
    df = pl.DataFrame({"x": [0.0, 5.0, 10.0]})
    out = df.with_columns(minmax_normalize_expr(pl.col("x")).alias("x_n"))
    assert out["x_n"].to_list() == [0.0, 0.5, 1.0]


def test_minmax_normalize_constant_returns_half():
    df = pl.DataFrame({"x": [5.0, 5.0, 5.0]})
    out = df.with_columns(minmax_normalize_expr(pl.col("x")).alias("x_n"))
    assert out["x_n"].to_list() == [0.5, 0.5, 0.5]


def test_apply_score_pipeline_positive():
    df = pl.DataFrame({"x": [1.0, 2.0, 3.0, 4.0]})
    out = apply_score_pipeline(df, "x", direction="positive", null_value=0.0)
    assert "x_n" in out.columns
    assert out["x_n"].min() == 0.0
    assert out["x_n"].max() == 1.0


def test_apply_score_pipeline_negative_inverts():
    df = pl.DataFrame({"x": [1.0, 2.0, 3.0, 4.0]})
    out = apply_score_pipeline(df, "x", direction="negative", null_value=0.0)
    # negative direction reverses ordering: lowest x → 1.0
    assert out["x_n"][0] == 1.0
    assert out["x_n"][-1] == 0.0


def test_apply_score_pipeline_fills_nulls_in_numerator():
    df = pl.DataFrame({"x": [1.0, 2.0, None, 4.0]})
    out = apply_score_pipeline(df, "x", direction="positive", null_value=0.0)
    assert out["x_n"][2] == 0.0


def test_apply_score_pipeline_fills_nulls_in_denominator_with_one():
    df = pl.DataFrame({"x": [1.0, 2.0, None, 4.0]})
    out = apply_score_pipeline(df, "x", direction="positive", null_value=1.0)
    assert out["x_n"][2] == 1.0


def test_apply_score_pipeline_invalid_direction():
    df = pl.DataFrame({"x": [1.0, 2.0]})
    with pytest.raises(ValueError):
        apply_score_pipeline(df, "x", direction="weird")


def test_geometric_mean_three_columns():
    df = pl.DataFrame({"a": [1.0, 0.5, 0.25], "b": [1.0, 0.5, 0.5], "c": [1.0, 0.5, 1.0]})
    out = df.with_columns(g=geometric_mean_expr("a", "b", "c"))
    # row 0: (1*1*1)^(1/3) = 1
    # row 1: (0.5*0.5*0.5)^(1/3) = 0.5
    # row 2: (0.25*0.5*1)^(1/3) ≈ 0.5
    assert math.isclose(out["g"][0], 1.0, abs_tol=1e-6)
    assert math.isclose(out["g"][1], 0.5, abs_tol=1e-6)
    assert math.isclose(out["g"][2], 0.5, abs_tol=1e-6)


def test_geometric_mean_zero_in_one_column_returns_zero():
    df = pl.DataFrame({"a": [0.0], "b": [0.5], "c": [0.5]})
    out = df.with_columns(g=geometric_mean_expr("a", "b", "c"))
    assert out["g"][0] == 0.0


def test_geometric_mean_requires_at_least_one_column():
    with pytest.raises(ValueError):
        geometric_mean_expr()
