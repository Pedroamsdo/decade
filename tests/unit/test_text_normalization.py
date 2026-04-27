"""Unit tests for text normalization and CNPJ helpers."""
from __future__ import annotations

import pytest


def test_strip_accents_basic():
    from fund_rank.silver._io import strip_accents

    assert strip_accents("Duração Baixa") == "Duracao Baixa"
    assert strip_accents("Crédito Privado") == "Credito Privado"
    assert strip_accents("Público Geral") == "Publico Geral"
    assert strip_accents(None) is None


def test_normalize_text():
    from fund_rank.silver._io import normalize_text

    assert normalize_text("Renda Fixa Duração Baixa") == "renda fixa duracao baixa"
    assert normalize_text("  Em   Funcionamento Normal  ") == "em funcionamento normal"
    assert normalize_text(None) is None


def test_normalize_cnpj_pads_and_strips():
    from fund_rank.silver._io import normalize_cnpj

    assert normalize_cnpj("12.345.678/0001-99") == "12345678000199"
    assert normalize_cnpj("12345678000199") == "12345678000199"
    assert normalize_cnpj("123") == "00000000000123"
    assert normalize_cnpj("") is None
    assert normalize_cnpj(None) is None


def test_anbima_prefix_match_after_normalization():
    """Documented behavior in ADR-009: prefix match after accent-strip."""
    from fund_rank.silver._io import normalize_text

    cvm_value = "Renda Fixa Duração Baixa Grau de Invest."
    yaml_pattern = "Renda Fixa Duracao Baixa Grau de Invest"  # ASCII, no period

    cvm_norm = normalize_text(cvm_value)
    yaml_norm = normalize_text(yaml_pattern)
    assert cvm_norm.startswith(yaml_norm), (cvm_norm, yaml_norm)


@pytest.mark.parametrize(
    "value,truncated",
    [
        ("Renda Fixa Duração Baixa Grau de Invest.", "renda fixa duracao baixa grau de invest"),
        ("Renda Fixa Duração Livre Crédito Liv", "renda fixa duracao livre credito"),
        ("Renda Fixa Duração Média Crédito Livre", "renda fixa duracao media credito"),
    ],
)
def test_yaml_patterns_match_truncated_cvm_strings(value, truncated):
    """CVM truncates long classifications to ~40 chars; our prefix patterns must catch them."""
    from fund_rank.silver._io import normalize_text

    assert normalize_text(value).startswith(truncated)
