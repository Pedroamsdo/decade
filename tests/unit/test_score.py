"""Tests for z-score + weighted sum scoring and tie-breaks."""
from __future__ import annotations

import math

import polars as pl


def test_zscore_centers_at_median_and_scales_by_std():
    from fund_rank.rank.score import _zscore

    df = pl.DataFrame({"x": [1.0, 2.0, 3.0, 4.0, 5.0]})
    z = df.with_columns(_zscore(pl.col("x")).alias("z"))
    # Median is 3, std ~1.58
    z_values = z["z"].to_list()
    assert math.isclose(z_values[2], 0.0, abs_tol=1e-9)  # median row → 0
    assert z_values[4] > z_values[0]  # higher → positive z


def test_score_segment_uses_directions():
    """A negative-direction metric should subtract z, not add."""
    from fund_rank.rank.score import score_segment

    df = pl.DataFrame({
        "cnpj_classe": ["A", "B"],
        "tracking_error_cdi_12m": [0.001, 0.10],   # A has tighter TE — better
        "pct_cdi_12m": [1.05, 1.05],                # equal
    })
    weights = {"tracking_error_cdi_12m": 0.5, "pct_cdi_12m": 0.5}
    directions = {"tracking_error_cdi_12m": "negative", "pct_cdi_12m": "positive"}
    scored = score_segment(df, weights, directions)
    # A should have a higher score than B because lower TE is better
    a_score = scored.filter(pl.col("cnpj_classe") == "A")["score"][0]
    b_score = scored.filter(pl.col("cnpj_classe") == "B")["score"][0]
    assert a_score > b_score


def test_missing_metric_warns_but_does_not_crash(caplog):
    """Missing metrics in the data are skipped with a warning."""
    from fund_rank.rank.score import score_segment

    df = pl.DataFrame({"cnpj_classe": ["A", "B"], "pct_cdi_12m": [1.0, 1.05]})
    weights = {"pct_cdi_12m": 0.5, "fictitious_metric": 0.5}
    directions = {"pct_cdi_12m": "positive", "fictitious_metric": "positive"}
    scored = score_segment(df, weights, directions)
    assert "score" in scored.columns
    # Score is still computed from the available metric
    assert scored["score"][1] > scored["score"][0]
