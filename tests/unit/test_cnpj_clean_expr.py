"""Tests for the polars-native CNPJ cleaner used in build_funds / build_quota_series."""
from __future__ import annotations

import polars as pl


def test_cnpj_clean_strips_punctuation_and_pads():
    from fund_rank.silver.build_funds import _cnpj_clean_expr

    df = pl.DataFrame({
        "raw": [
            "12.345.678/0001-99",
            "12345678000199",
            "00.017.024/0001-53",
            "123",                # short → pad with zeros
        ]
    })
    out = df.select(_cnpj_clean_expr("raw").alias("clean"))
    expected = ["12345678000199", "12345678000199", "00017024000153", "00000000000123"]
    assert out["clean"].to_list() == expected
