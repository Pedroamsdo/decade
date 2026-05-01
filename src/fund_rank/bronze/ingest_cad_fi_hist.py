"""Ingest CVM CAD_FI hist zip (cad_fi_hist.zip).

The zip contains the per-attribute history files (taxa_adm, taxa_perfm,
rentab, situacao, ...). Silver builders read individual members on demand.
"""
from __future__ import annotations

import httpx

from fund_rank.bronze._common import IngestOutcome, ingest_one
from fund_rank.settings import Settings
from fund_rank.sources.cvm import cad_fi_hist_url


def run(settings: Settings, client: httpx.Client) -> IngestOutcome:
    ep = cad_fi_hist_url(settings)
    return ingest_one(
        settings,
        client,
        source=ep.name,
        url=ep.url,
        extension=settings.pipeline.sources.cvm_cad_fi_hist.extension,
        competence=None,
        accept_404=False,
    )
