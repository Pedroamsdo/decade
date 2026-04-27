"""Ingest BCB SGS series 12 (CDI daily)."""
from __future__ import annotations

from datetime import date

import httpx
from dateutil.relativedelta import relativedelta

from fund_rank.bronze._common import IngestOutcome, ingest_one
from fund_rank.settings import Settings
from fund_rank.sources.bcb_sgs import cdi_url


def run(
    settings: Settings,
    client: httpx.Client,
    as_of: date,
    today: date | None = None,
    lookback_years: int | None = None,
) -> IngestOutcome:
    today = today or date.today()
    lookback_years = lookback_years or settings.pipeline.ingest.cdi_lookback_years
    dt_ini = as_of - relativedelta(years=lookback_years)
    dt_fim = as_of
    ep = cdi_url(settings, dt_ini, dt_fim)
    competence = f"{dt_ini.isoformat()}_to_{dt_fim.isoformat()}"
    return ingest_one(
        settings,
        client,
        source=ep.name,
        url=ep.url,
        extension=settings.pipeline.sources.bcb_cdi.extension,
        competence=competence,
        today=today,
        accept_404=False,
    )
