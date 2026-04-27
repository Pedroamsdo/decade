"""Shared helpers for bronze ingestion."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import httpx

from fund_rank.bronze.manifest import (
    Manifest,
    latest_partition_dir,
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
    today: date | None = None,
    accept_404: bool = True,
) -> IngestOutcome:
    """Generic etag-aware ingest of a single URL into the bronze layer.

    On 304/Not Modified, returns the existing latest partition without writing.
    On 404, returns status="not_found" without writing (caller decides if fatal).
    """
    today = today or date.today()
    bronze_root = settings.bronze_root

    prior = latest_partition_dir(bronze_root, source, competence=competence)
    prior_manifest = read_manifest(prior) if prior else None
    prior_etag = prior_manifest.etag if prior_manifest else None
    prior_lm = prior_manifest.last_modified if prior_manifest else None

    log.info(
        "bronze.ingest.start",
        source=source,
        competence=competence,
        url=url,
        prior_etag=prior_etag,
    )

    res: FetchResult = fetch_with_etag(
        client,
        url,
        prior_etag=prior_etag,
        prior_last_modified=prior_lm,
        max_retries=settings.pipeline.http.max_retries,
        backoff_min=settings.pipeline.http.retry_backoff_min_seconds,
        backoff_max=settings.pipeline.http.retry_backoff_max_seconds,
    )

    if res.status_code == 304:
        log.info("bronze.ingest.not_modified", source=source, competence=competence)
        return IngestOutcome(
            source=source,
            competence=competence,
            status="not_modified",
            partition=prior,
            manifest=prior_manifest,
        )

    if res.status_code == 404:
        if accept_404:
            log.warning("bronze.ingest.not_found", source=source, competence=competence, url=url)
            return IngestOutcome(
                source=source,
                competence=competence,
                status="not_found",
                partition=None,
                manifest=None,
            )
        raise RuntimeError(f"Source {source} returned 404 for {url}")

    if res.content is None:
        raise RuntimeError(
            f"Source {source} returned status {res.status_code} with no content"
        )

    sha = sha256_hex(res.content)

    # Idempotency by content: if sha256 matches the latest partition, no-op.
    if prior_manifest and prior_manifest.sha256 == sha:
        log.info(
            "bronze.ingest.same_sha",
            source=source,
            competence=competence,
            sha256=sha,
        )
        return IngestOutcome(
            source=source,
            competence=competence,
            status="not_modified",
            partition=prior,
            manifest=prior_manifest,
        )

    part = partition_dir(bronze_root, source, today, competence=competence)
    write_payload(part, res.content, extension=extension)
    manifest = Manifest(
        source=source,
        url=url,
        competence=competence,
        etag=res.etag,
        last_modified=res.last_modified,
        sha256=sha,
        byte_size=len(res.content),
        row_count=None,  # row count is computed in silver where parsing happens
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
    return IngestOutcome(
        source=source,
        competence=competence,
        status="fetched",
        partition=part,
        manifest=manifest,
    )
