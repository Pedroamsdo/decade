"""Ingest CVM CAD_FI snapshot."""
from __future__ import annotations

from datetime import date

import httpx

from fund_rank.bronze._common import IngestOutcome, ingest_one
from fund_rank.settings import Settings
from fund_rank.sources.cvm import cad_fi_url


def run(settings: Settings, client: httpx.Client, today: date | None = None) -> IngestOutcome:
    ep = cad_fi_url(settings)
    return ingest_one(
        settings,
        client,
        source=ep.name,
        url=ep.url,
        extension=settings.pipeline.sources.cvm_cad_fi.extension,
        competence=None,
        today=today,
        accept_404=False,
    )
