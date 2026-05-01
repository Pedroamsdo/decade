"""Ingest CVM Registro Fundo/Classe snapshot."""
from __future__ import annotations

import httpx

from fund_rank.bronze._common import IngestOutcome, ingest_one
from fund_rank.settings import Settings
from fund_rank.sources.cvm import registro_classe_url


def run(settings: Settings, client: httpx.Client) -> IngestOutcome:
    ep = registro_classe_url(settings)
    return ingest_one(
        settings,
        client,
        source=ep.name,
        url=ep.url,
        extension=settings.pipeline.sources.cvm_registro_classe.extension,
        competence=None,
        accept_404=True,  # this file is sometimes briefly unavailable; not fatal
    )
