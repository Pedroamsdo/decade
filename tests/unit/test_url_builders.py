"""URL builder unit tests — no network."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _chdir_repo_root(monkeypatch):
    monkeypatch.chdir(REPO_ROOT)


def test_inf_diario_monthly_url():
    from fund_rank.settings import Settings
    from fund_rank.sources.cvm import inf_diario_url

    s = Settings()
    ep = inf_diario_url(s, 2025, 12)
    assert ep.url.endswith("inf_diario_fi_202512.zip")
    assert ep.competence == "2025-12"


def test_cda_monthly_url():
    from fund_rank.settings import Settings
    from fund_rank.sources.cvm import cda_url

    s = Settings()
    ep = cda_url(s, 2025, 12)
    assert ep.url.endswith("cda_fi_202512.zip")
    assert ep.competence == "2025-12"


def test_months_between():
    from fund_rank.sources.cvm import months_between

    out = months_between(date(2024, 11, 15), date(2025, 2, 1))
    assert out == [(2024, 11), (2024, 12), (2025, 1), (2025, 2)]


def test_cdi_url_format():
    from fund_rank.settings import Settings
    from fund_rank.sources.bcb_sgs import cdi_url

    s = Settings()
    ep = cdi_url(s, date(2020, 12, 31), date(2025, 12, 31))
    assert "bcdata.sgs.12" in ep.url
    assert "31/12/2020" in ep.url
    assert "31/12/2025" in ep.url
