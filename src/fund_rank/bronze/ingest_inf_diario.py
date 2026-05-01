"""Ingest CVM INF_DIARIO monthly zips, with HIST yearly fallback for older years."""
from __future__ import annotations

from datetime import date

import httpx
from dateutil.relativedelta import relativedelta

from fund_rank.bronze._common import IngestOutcome, ingest_one
from fund_rank.settings import Settings
from fund_rank.sources.cvm import inf_diario_hist_url, inf_diario_url, months_between


def run(
    settings: Settings,
    client: httpx.Client,
    as_of: date,
    today: date | None = None,
    lookback_months: int | None = None,
    ingest_until: date | None = None,
) -> list[IngestOutcome]:
    """Download monthly INF_DIARIO zips spanning lookback before ingest_until through ingest_until.

    `ingest_until` (default: today) bounds the download window. `as_of` is kept
    for backwards-compat / partition naming but no longer caps the download —
    silver/gold layers apply the `<= as_of` cutoff themselves.

    For months whose monthly zip returns 404, fall back to the yearly HIST zip
    (one per year, regardless of which months are missing).
    """
    today = today or date.today()
    ingest_until = ingest_until or today
    lookback_months = lookback_months or settings.pipeline.ingest.inf_diario_lookback_months
    start = (ingest_until - relativedelta(months=lookback_months - 1)).replace(day=1)
    end = ingest_until.replace(day=1)

    outcomes: list[IngestOutcome] = []
    years_needing_hist: set[int] = set()

    for year, month in months_between(start, end):
        ep = inf_diario_url(settings, year, month)
        out = ingest_one(
            settings,
            client,
            source=ep.name,
            url=ep.url,
            extension=settings.pipeline.sources.cvm_inf_diario.extension,
            competence=ep.competence,
            today=today,
            accept_404=True,
        )
        outcomes.append(out)
        if out.status == "not_found":
            years_needing_hist.add(year)

    for year in sorted(years_needing_hist):
        ep_hist = inf_diario_hist_url(settings, year)
        if ep_hist is None:
            continue
        out = ingest_one(
            settings,
            client,
            source=ep_hist.name,
            url=ep_hist.url,
            extension="zip",
            competence=ep_hist.competence,
            today=today,
            accept_404=True,
        )
        outcomes.append(out)

    return outcomes
