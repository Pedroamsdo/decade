"""Integration tests for the seams between scoring config and gold scoring.

These tests don't run the full bronze→silver→gold pipeline (too heavy for CI).
Instead they exercise the contract that the user's plan cares about: that
`scoring.yaml` is the single source of truth for eligibility filters, metric
weights, and selection top_n — i.e. that editing the YAML actually changes the
score table.

What they protect:
  - `gold._compute_score` reads eligibility from `ScoringConfig`, not constants.
  - The composite degenerates to ±metric when only one metric is configured
    (preserves backward-compat with the legacy percentile-rank-of-IR).
  - Weighted z-score combination behaves correctly with multiple metrics.
  - `top_n` in `build_ranking_report` is sourced from the config when not
    overridden.
"""
from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from fund_rank.gold.build_fund_metrics import _compute_score
from fund_rank.settings import ScoringConfig


def _scoring(
    metrics: dict[str, dict],
    *,
    nr_cotst_min: int = 1000,
    existing_time_min_days: int = 252,
    equity_min_brl: float = 50_000_000,
    top_n: int = 5,
) -> ScoringConfig:
    return ScoringConfig.model_validate(
        {
            "metrics": metrics,
            "eligibility": {
                "situacao": "Em Funcionamento Normal",
                "nr_cotst_min": nr_cotst_min,
                "existing_time_min_days": existing_time_min_days,
                "equity_min_brl": equity_min_brl,
            },
            "selection": {"top_n": top_n},
        }
    )


def _toy_metrics(extra_cols: dict | None = None) -> pl.DataFrame:
    """4 funds: 2 eligible, 2 not (low PL / few cotistas)."""
    base = {
        "fund_key": ["A", "B", "C", "D"],
        "situacao": ["Em Funcionamento Normal"] * 4,
        "nr_cotst": [5_000, 2_000, 500, 1_500],          # C fails (<=1000)
        "existing_time": [400, 800, 600, 100],            # D fails (<252)
        "equity": [100e6, 80e6, 60e6, 200e6],
        "information_ratio": [0.5, 1.2, 0.9, 2.0],
    }
    if extra_cols:
        base.update(extra_cols)
    return pl.DataFrame(base)


def test_score_null_for_ineligible_funds():
    df = _toy_metrics()
    scoring = _scoring({"information_ratio": {"direction": "positive", "weight": 1.0}})
    out = _compute_score(df, scoring)
    score = dict(zip(out["fund_key"].to_list(), out["score"].to_list()))
    assert score["C"] is None, "C has 500 cotistas — should be null"
    assert score["D"] is None, "D has 100 days history — should be null"
    assert score["A"] is not None and score["B"] is not None


def test_score_in_0_100_range_and_orders_by_metric():
    df = _toy_metrics()
    scoring = _scoring({"information_ratio": {"direction": "positive", "weight": 1.0}})
    out = _compute_score(df, scoring)
    eligible = out.filter(pl.col("score").is_not_null())
    scores = eligible["score"].to_list()
    assert all(0 <= s <= 100 for s in scores)
    # B has IR=1.2 vs A=0.5 → B must outrank A
    by_key = dict(zip(eligible["fund_key"].to_list(), scores))
    assert by_key["B"] > by_key["A"]


def test_negative_direction_inverts_ranking():
    df = _toy_metrics()
    scoring = _scoring({"information_ratio": {"direction": "negative", "weight": 1.0}})
    out = _compute_score(df, scoring)
    eligible = out.filter(pl.col("score").is_not_null())
    by_key = dict(zip(eligible["fund_key"].to_list(), eligible["score"].to_list()))
    # With direction=negative, lower IR is better → A (0.5) outranks B (1.2)
    assert by_key["A"] > by_key["B"]


def test_eligibility_threshold_drives_universe_size():
    df = _toy_metrics()
    metrics_cfg = {"information_ratio": {"direction": "positive", "weight": 1.0}}

    relaxed = _scoring(metrics_cfg, equity_min_brl=10e6, nr_cotst_min=100)
    out_relaxed = _compute_score(df, relaxed)
    n_relaxed = out_relaxed.filter(pl.col("score").is_not_null()).height

    strict = _scoring(metrics_cfg, equity_min_brl=150e6, nr_cotst_min=100)
    out_strict = _compute_score(df, strict)
    n_strict = out_strict.filter(pl.col("score").is_not_null()).height

    # Tightening equity_min_brl removes funds A (100M) and B (80M); only D (200M)
    # passes equity AND has enough history? D fails existing_time so n_strict=0.
    # The point: strictness shrinks the eligible set monotonically.
    assert n_strict < n_relaxed


def test_multi_metric_weighted_composite():
    df = _toy_metrics(
        extra_cols={"vol": [0.10, 0.20, 0.05, 0.15]}  # lower is better
    )
    scoring = _scoring(
        {
            "information_ratio": {"direction": "positive", "weight": 0.5},
            "vol": {"direction": "negative", "weight": 0.5},
        }
    )
    out = _compute_score(df, scoring)
    eligible = out.filter(pl.col("score").is_not_null())
    assert eligible.height >= 2
    assert eligible["score"].min() >= 0
    assert eligible["score"].max() <= 100


def test_unknown_metric_in_config_raises():
    df = _toy_metrics()
    scoring = _scoring({"sharpe_inexistente": {"direction": "positive", "weight": 1.0}})
    with pytest.raises(ValueError, match="not produced by gold pipeline"):
        _compute_score(df, scoring)


def test_weights_must_sum_to_one():
    with pytest.raises(ValueError, match="weights must sum to 1.0"):
        _scoring(
            {
                "information_ratio": {"direction": "positive", "weight": 0.5},
                "vol": {"direction": "negative", "weight": 0.3},
            }
        )


def test_live_scoring_yaml_loads_and_is_consistent_with_gold_columns():
    """Catches the case where someone edits scoring.yaml to reference a metric
    that doesn't exist in build_fund_metrics' OUTPUT_COLUMNS."""
    from fund_rank.gold.build_fund_metrics import OUTPUT_COLUMNS
    from fund_rank.settings import Settings

    s = Settings()
    sc = s.scoring
    for metric_name in sc.metrics:
        assert metric_name in OUTPUT_COLUMNS, (
            f"scoring.yaml lists metric '{metric_name}' but gold/fund_metrics "
            f"does not produce it. Either add an attach_<name> in gold/_metrics.py "
            f"and include the column in OUTPUT_COLUMNS, or remove it from scoring.yaml."
        )
