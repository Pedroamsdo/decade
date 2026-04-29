"""Unit tests for silver/_benchmark_mapping."""
from __future__ import annotations

import polars as pl


def test_all_18_known_values_mapped():
    from fund_rank.silver._benchmark_mapping import (
        BENCHMARK_MAPPING,
        apply_benchmark_mapping,
    )

    raw = list(BENCHMARK_MAPPING.keys())
    expected = list(BENCHMARK_MAPPING.values())
    df = pl.DataFrame({"benchmark": raw})
    out = apply_benchmark_mapping(df)
    assert out["benchmark"].to_list() == expected


def test_null_becomes_cdi():
    from fund_rank.silver._benchmark_mapping import apply_benchmark_mapping

    df = pl.DataFrame(
        {"benchmark": [None, "DI de um dia", None]},
        schema={"benchmark": pl.Utf8},
    )
    out = apply_benchmark_mapping(df)
    assert out["benchmark"].to_list() == ["CDI", "CDI", "CDI"]


def test_unknown_value_kept_unchanged():
    from fund_rank.silver._benchmark_mapping import apply_benchmark_mapping

    df = pl.DataFrame({"benchmark": ["string desconhecida", "CDI"]})
    out = apply_benchmark_mapping(df)
    # Unmapped values pass through unchanged.
    assert out["benchmark"].to_list() == ["string desconhecida", "CDI"]


def test_mapping_count_is_18():
    from fund_rank.silver._benchmark_mapping import BENCHMARK_MAPPING

    assert len(BENCHMARK_MAPPING) == 18


def test_target_codes_set():
    from fund_rank.silver._benchmark_mapping import BENCHMARK_MAPPING

    targets = set(BENCHMARK_MAPPING.values())
    assert targets == {
        "CDI",
        "IPCA",
        "INPC",
        "IGP-M",
        "IMA-B",
        "IMA-B 5",
        "IMA-B 5+",
        "IMA-GERAL",
        "IMA-S",
        "IRF-M",
    }
