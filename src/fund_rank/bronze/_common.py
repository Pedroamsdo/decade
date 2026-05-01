"""Shared helpers for bronze ingestion."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import httpx

from fund_rank.bronze.manifest import (
    Manifest,
    now_iso,
    partition_dir,
    read_manifest,
    write_manifest,
    write_payload,
)
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.sources.http import FetchResult, fetch_with_etag, sha256_hex

log = get_logger(__name__)


@dataclass
class IngestOutcome:
    source: str
    competence: str | None
    status: str          # "fetched" | "not_modified" | "not_found"
    partition: Path | None
    manifest: Manifest | None


def ingest_one(
    settings: Settings,
    client: httpx.Client,
    source: str,
    url: str,
    extension: str,
    competence: str | None = None,
    accept_404: bool = True,
) -> IngestOutcome:
    """Etag-aware ingest of one URL into the canonical bronze path.

    Path is fixed: ``data/bronze/{source}[/competence={competence}]/``. On every
    run the manifest's ``ingested_at`` is refreshed (so the run date is always
    visible) — even when content didn't change. On 304/sha-match the payload is
    untouched; on a fresh body, ``raw.{ext}`` is overwritten.
    """
    part = partition_dir(settings.bronze_root, source, competence=competence)
    prior = read_manifest(part)
    prior_etag = prior.etag if prior else None
    prior_lm = prior.last_modified if prior else None

    res: FetchResult = fetch_with_etag(
        client,
        url,
        prior_etag=prior_etag,
        prior_last_modified=prior_lm,
        max_retries=settings.pipeline.http.max_retries,
        backoff_min=settings.pipeline.http.retry_backoff_min_seconds,
        backoff_max=settings.pipeline.http.retry_backoff_max_seconds,
    )

    def _refresh(prior_manifest: Manifest, status: str) -> IngestOutcome:
        manifest = Manifest(
            source=prior_manifest.source,
            url=prior_manifest.url,
            competence=prior_manifest.competence,
            etag=prior_manifest.etag,
            last_modified=prior_manifest.last_modified,
            sha256=prior_manifest.sha256,
            byte_size=prior_manifest.byte_size,
            row_count=prior_manifest.row_count,
            ingested_at=now_iso(),
            status=status,
        )
        write_manifest(part, manifest)
        log.info("bronze.ingest.refreshed", source=source, competence=competence, status=status)
        return IngestOutcome(source, competence, status, part, manifest)

    if res.status_code == 304 and prior is not None:
        return _refresh(prior, "not_modified")

    if res.status_code == 404:
        if not accept_404:
            raise RuntimeError(f"Source {source} returned 404 for {url}")
        if prior is not None:
            return _refresh(prior, "not_found")
        return IngestOutcome(source, competence, "not_found", None, None)

    sha = sha256_hex(res.content)

    if prior is not None and prior.sha256 == sha:
        return _refresh(prior, "not_modified")

    write_payload(part, res.content, extension=extension)
    manifest = Manifest(
        source=source,
        url=url,
        competence=competence,
        etag=res.etag,
        last_modified=res.last_modified,
        sha256=sha,
        byte_size=len(res.content),
        row_count=None,
        ingested_at=now_iso(),
        status="fetched",
    )
    write_manifest(part, manifest)
    log.info(
        "bronze.ingest.fetched",
        source=source,
        competence=competence,
        bytes=len(res.content),
        sha256=sha[:12],
    )
    return IngestOutcome(source, competence, "fetched", part, manifest)
