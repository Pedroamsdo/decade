"""Index series URL builders + chunking helper — unit tests, no network."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _chdir_repo_root(monkeypatch):
    monkeypatch.chdir(REPO_ROOT)


def test_sgs_url_format_selic():
    from fund_rank.settings import Settings
    from fund_rank.sources.bcb_sgs import sgs_url

    s = Settings()
    ep = sgs_url(s, "bcb_selic", 11, date(2020, 1, 1), date(2025, 12, 31))
    assert "bcdata.sgs.11" in ep.url
    assert "01/01/2020" in ep.url
    assert "31/12/2025" in ep.url
    assert ep.name == "bcb_selic"
    assert ep.series_id == 11


def test_sgs_url_format_ipca():
    from fund_rank.settings import Settings
    from fund_rank.sources.bcb_sgs import sgs_url

    s = Settings()
    ep = sgs_url(s, "bcb_ipca", 433, date(2010, 6, 15), date(2020, 6, 15))
    assert "bcdata.sgs.433" in ep.url
    assert "15/06/2010" in ep.url
    assert "15/06/2020" in ep.url


def test_chunk_decade_2000_2025():
    from fund_rank.sources.bcb_sgs import chunk_decade

    chunks = chunk_decade(date(2000, 1, 1), date(2025, 12, 31), chunk_years=10)
    assert len(chunks) == 3
    assert chunks[0] == (date(2000, 1, 1), date(2009, 12, 31))
    assert chunks[1] == (date(2010, 1, 1), date(2019, 12, 31))
    assert chunks[2] == (date(2020, 1, 1), date(2025, 12, 31))


def test_chunk_decade_single_window():
    from fund_rank.sources.bcb_sgs import chunk_decade

    chunks = chunk_decade(date(2020, 1, 1), date(2025, 12, 31), chunk_years=10)
    assert chunks == [(date(2020, 1, 1), date(2025, 12, 31))]


def test_chunk_decade_empty_when_inverted():
    from fund_rank.sources.bcb_sgs import chunk_decade

    assert chunk_decade(date(2025, 1, 1), date(2020, 1, 1)) == []


def test_chunk_decade_no_overlap():
    """Janelas adjacentes não podem se sobrepor (start[i+1] = end[i] + 1 dia)."""
    from datetime import timedelta

    from fund_rank.sources.bcb_sgs import chunk_decade

    chunks = chunk_decade(date(1990, 3, 17), date(2025, 12, 31), chunk_years=10)
    for prev, nxt in zip(chunks, chunks[1:]):
        assert nxt[0] == prev[1] + timedelta(days=1)


def test_anbima_filename_to_competence_via_benchmarks_yaml():
    """Drop XLS filename → competence label, derivado de configs/benchmarks.yaml."""
    from fund_rank.bronze.ingest_anbima_indices import (
        _drop_filename_index,
        _filename_to_competence,
    )
    from fund_rank.settings import Settings

    s = Settings()
    idx = _drop_filename_index(s)

    # Mapeamento esperado pela config atual
    assert _filename_to_competence("IMAB-HISTORICO.xls", idx) == "ima_b"
    assert _filename_to_competence("IMAB5-HISTORICO.xls", idx) == "ima_b_5"
    assert _filename_to_competence("IMAB5MAIS-HISTORICO.xls", idx) == "ima_b_5plus"
    assert _filename_to_competence("IMAGERAL-HISTORICO.xls", idx) == "ima_geral"
    assert _filename_to_competence("IMAS-HISTORICO.xls", idx) == "ima_s"
    assert _filename_to_competence("IRFM-HISTORICO.xls", idx) == "irf_m"
    # Case-insensitive
    assert _filename_to_competence("imab-historico.XLS", idx) == "ima_b"
    # Unknown filename → None
    assert _filename_to_competence("RANDOM-FILE.xls", idx) is None


def test_benchmarks_yaml_has_granularity_and_source_for_all_indices():
    """Toda série de `silver/index_series` precisa ter metadata em benchmarks.yaml."""
    from fund_rank.settings import Settings
    from fund_rank.silver.build_index_series import INDEX_SOURCES

    s = Settings()
    for col in INDEX_SOURCES:
        cfg = s.benchmarks.get(col)
        assert cfg is not None, f"{col} faltando em benchmarks.yaml"
        assert cfg.get("granularity") in ("daily", "monthly"), f"{col}: granularity inválida"
        assert cfg.get("source") in ("bcb_sgs", "anbima_drop"), f"{col}: source inválida"
