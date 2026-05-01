"""Ingest BCB SGS series for all index_series benchmarks.

Cobre CDI (12), SELIC (11), IPCA (433), INPC (188), IGP-M (189). Cada série é
ingerida em janelas de até 10 anos (limite SGS) — uma `competence` por chunk,
ancorada em ``as_of`` para nomes determinísticos entre runs.
"""
from __future__ import annotations

from datetime import date

import httpx
from dateutil.relativedelta import relativedelta

from fund_rank.bronze._common import IngestOutcome, ingest_one
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.sources.bcb_sgs import chunk_decade, sgs_url

log = get_logger(__name__)


# (source_name, sgs_series_id) — source_name precisa existir em _SourcesConfig.
BCB_INDICES: list[tuple[str, int]] = [
    ("bcb_cdi", 12),
    ("bcb_selic", 11),
    ("bcb_ipca", 433),
    ("bcb_inpc", 188),
    ("bcb_igpm", 189),
]


def run(
    settings: Settings,
    client: httpx.Client,
    as_of: date,
    lookback_years: int | None = None,
) -> list[IngestOutcome]:
    lookback = lookback_years or settings.pipeline.ingest.index_series_lookback_years
    dt_fim = date(as_of.year, 12, 31)
    dt_ini = dt_fim - relativedelta(years=lookback)
    chunks = chunk_decade(dt_ini, dt_fim, chunk_years=10)

    log.info(
        "bronze.bcb_indices.start",
        as_of=as_of.isoformat(),
        lookback_years=lookback,
        n_series=len(BCB_INDICES),
        n_chunks=len(chunks),
    )

    outcomes: list[IngestOutcome] = []
    for name, series_id in BCB_INDICES:
        for c_ini, c_fim in chunks:
            ep = sgs_url(settings, name, series_id, c_ini, c_fim)
            competence = f"{c_ini.isoformat()}_to_{c_fim.isoformat()}"
            outcomes.append(
                ingest_one(
                    settings,
                    client,
                    source=ep.name,
                    url=ep.url,
                    extension="json",
                    competence=competence,
                    accept_404=True,
                )
            )

    fetched = sum(1 for o in outcomes if o.status == "fetched")
    not_modified = sum(1 for o in outcomes if o.status == "not_modified")
    not_found = sum(1 for o in outcomes if o.status == "not_found")
    log.info(
        "bronze.bcb_indices.done",
        total=len(outcomes),
        fetched=fetched,
        not_modified=not_modified,
        not_found=not_found,
    )
    return outcomes
