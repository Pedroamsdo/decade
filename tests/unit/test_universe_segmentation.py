"""End-to-end style test for segment classification logic."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _chdir_repo_root(monkeypatch):
    monkeypatch.chdir(REPO_ROOT)


def test_classe_anbima_match_expr_caixa_includes_renda_fixa_simples():
    from fund_rank.silver.build_universe import _classe_anbima_match
    from fund_rank.settings import Settings

    cfg = Settings().universe["segments"]["caixa"]
    df = pl.DataFrame({
        "classe_anbima_norm": [
            "renda fixa simples",
            "renda fixa duracao baixa soberano",
            "renda fixa duracao baixa grau de invest.",
            "renda fixa duracao media soberano",  # NOT caixa
            "multimercados livre",                 # NOT RF
            None,                                  # null → reject
        ]
    })
    matched = df.filter(_classe_anbima_match(cfg, pl.col("classe_anbima_norm")))
    expected = {
        "renda fixa simples",
        "renda fixa duracao baixa soberano",
        "renda fixa duracao baixa grau de invest.",
    }
    assert set(matched["classe_anbima_norm"].to_list()) == expected


def test_classe_anbima_match_expr_qualificado_includes_credito():
    from fund_rank.silver.build_universe import _classe_anbima_match
    from fund_rank.settings import Settings

    cfg = Settings().universe["segments"]["qualificado"]
    df = pl.DataFrame({
        "classe_anbima_norm": [
            "renda fixa duracao livre credito livre",
            "renda fixa duracao media credito livre",
            "renda fixa divida externa",
            "renda fixa duracao livre soberano",       # NOT qualificado
            "renda fixa simples",                       # NOT qualificado
        ]
    })
    matched = df.filter(_classe_anbima_match(cfg, pl.col("classe_anbima_norm")))
    expected = {
        "renda fixa duracao livre credito livre",
        "renda fixa duracao media credito livre",
        "renda fixa divida externa",
    }
    assert set(matched["classe_anbima_norm"].to_list()) == expected


def test_segment_publico_filters_caixa_to_publico_geral_only():
    from fund_rank.silver.build_universe import _publico_aceito
    from fund_rank.settings import Settings

    cfg = Settings().universe["segments"]["caixa"]
    df = pl.DataFrame({
        "publico_norm": ["publico geral", "qualificado", "profissional", None],
    })
    matched = df.filter(_publico_aceito(cfg, pl.col("publico_norm")))
    assert matched["publico_norm"].to_list() == ["publico geral"]


def test_yaml_segment_weights_sum_to_one():
    """Belt-and-suspenders: re-asserting the property tested in test_settings.py."""
    from fund_rank.settings import Settings

    weights = Settings().scoring["weights"]
    for seg, w in weights.items():
        total = sum(w.values())
        assert abs(total - 1.0) < 1e-9, f"{seg} weights sum {total}"
