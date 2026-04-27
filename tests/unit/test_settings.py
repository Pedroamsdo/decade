"""Smoke tests for the settings loader."""
from __future__ import annotations

from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _chdir_repo_root(monkeypatch):
    monkeypatch.chdir(REPO_ROOT)


def test_settings_loads_pipeline_config():
    from fund_rank.settings import Settings

    s = Settings()
    p = s.pipeline
    assert p.data_root == Path("data")
    assert p.reports_root == Path("reports")
    assert p.sources.cvm_cad_fi.url and "cad_fi.csv" in p.sources.cvm_cad_fi.url
    assert p.sources.cvm_inf_diario.url_template and "{yyyymm}" in p.sources.cvm_inf_diario.url_template
    assert p.http.max_retries == 5


def test_settings_loads_universe_segments():
    from fund_rank.settings import Settings

    s = Settings()
    u = s.universe
    assert set(u["segments"].keys()) == {"caixa", "rfgeral", "qualificado"}
    assert u["filters_global"]["fundo_exclusivo"] is False


def test_scoring_weights_sum_to_one():
    from fund_rank.settings import Settings

    s = Settings()
    w = s.scoring["weights"]
    for seg, weights in w.items():
        total = sum(weights.values())
        assert abs(total - 1.0) < 1e-9, f"segment {seg} weights sum to {total}, expected 1.0"
