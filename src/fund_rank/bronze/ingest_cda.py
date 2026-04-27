"""Ingest CVM CDA monthly zips."""
from __future__ import annotations

from datetime import date

import httpx
from dateutil.relativedelta import relativedelta

from fund_rank.bronze._common import IngestOutcome, ingest_one
from fund_rank.settings import Settings
from fund_rank.sources.cvm import cda_url, months_between


def run(
    settings: Settings,
    client: httpx.Client,
    as_of: date,
    today: date | None = None,
    lookback_months: int | None = None,
) -> list[IngestOutcome]:
    today = today or date.today()
    lookback_months = lookback_months or settings.pipeline.ingest.cda_lookback_months
    start = (as_of - relativedelta(months=lookback_months - 1)).replace(day=1)
    end = as_of.replace(day=1)

    outcomes: list[IngestOutcome] = []
    for year, month in months_between(start, end):
        ep = cda_url(settings, year, month)
        out = ingest_one(
            settings,
            client,
            source=ep.name,
            url=ep.url,
            extension=settings.pipeline.sources.cvm_cda.extension,
            competence=ep.competence,
            today=today,
            accept_404=True,
        )
        outcomes.append(out)
    return outcomes
