"""Canonical mapping of CVM `cad_fi_rentab.csv#RENTAB_FUNDO` strings to short codes.

Used by `build_class_funds_fixed_income` and `build_subclass_funds_fixed_income`
to reduce the 18 raw benchmark strings down to 10 canonical codes the gold
ranking layer can branch on.

`Não se aplica`, `OUTROS` and `Ibovespa` are intentionally folded into `CDI`
per product spec — fundos RF que declaram benchmark vazio/incoerente são
tratados como CDI no ranking.
"""
from __future__ import annotations

import polars as pl

# Exact-match mapping. Keys are the literal strings emitted by CVM in
# `cad_fi_rentab.csv#RENTAB_FUNDO`; values are the canonical short codes.
BENCHMARK_MAPPING: dict[str, str] = {
    "DI de um dia": "CDI",
    "Taxa Selic": "CDI",
    "Não se aplica": "CDI",
    "OUTROS": "CDI",
    "Ibovespa": "CDI",
    "Índice de Preços ao Consumidor Amplo (IPCA/IBGE)": "IPCA",
    "Índice de preços": "IPCA",
    "Índice Nacional de Preços ao Consumidor (INPC/IBGE)": "INPC",
    "Índice Geral de Preços-Mercado (IGP-M)": "IGP-M",
    "Índice de Mercado Andima todas NTN-B": "IMA-B",
    "Índice de Mercado Andima NTN-B até 5 anos": "IMA-B 5",
    "Índice de Mercado Andima NTN-C até 5 anos": "IMA-B 5",
    "Índice de Mercado Andima NTN-B mais de 5 anos": "IMA-B 5+",
    "Índice de Mercado Andima Geral": "IMA-GERAL",
    "Índice de Mercado Andima todas NTN-C": "IMA-GERAL",
    "Índice de Mercado Andima LFT": "IMA-S",
    "IRF-M": "IRF-M",
    "Taxa de juro prefixada": "IRF-M",
}


def apply_benchmark_mapping(df: pl.DataFrame, col: str = "benchmark") -> pl.DataFrame:
    """Replace raw CVM benchmark strings with canonical short codes.

    - Values present in `BENCHMARK_MAPPING` → mapped to the canonical code.
    - Values absent from the mapping → kept unchanged (defensive against CVM drift).
    - Nulls → filled with `"CDI"` (RF default benchmark).
    """
    return df.with_columns(
        pl.col(col)
        .replace_strict(BENCHMARK_MAPPING, default=pl.col(col))
        .fill_null("CDI")
        .alias(col)
    )
