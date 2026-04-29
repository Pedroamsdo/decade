"""Ingest ANBIMA FUNDOS-175-CARACTERISTICAS-PUBLICO xlsx (drop-based, no HTTP).

The ANBIMA portal requires authentication; instead of an HTTP fetch the user
drops the xlsx into ``data/bronze/anbima_175/dropped/`` and this ingestor
moves the most recent file into a manifest-tracked partition. Idempotent by
content hash.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from fund_rank.bronze._common import IngestOutcome
from fund_rank.bronze.manifest import (
    Manifest,
    latest_partition_dir,
    now_iso,
    partition_dir,
    read_manifest,
    write_manifest,
)
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.sources.http import sha256_hex

log = get_logger(__name__)

SOURCE_NAME = "anbima_175"
DROP_SUBDIR = "dropped"


def run(settings: Settings, today: date | None = None) -> IngestOutcome:
    today = today or date.today()
    bronze_root = settings.bronze_root
    drop_dir = bronze_root / SOURCE_NAME / DROP_SUBDIR
    drop_dir.mkdir(parents=True, exist_ok=True)

    candidates = sorted(
        (p for p in drop_dir.glob("*.xlsx") if p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        log.warning(
            "bronze.anbima_175.no_drop",
            drop_dir=str(drop_dir),
            hint="place FUNDOS-175-CARACTERISTICAS-PUBLICO.xlsx here",
        )
        return IngestOutcome(
            source=SOURCE_NAME,
            competence=None,
            status="not_found",
            partition=None,
            manifest=None,
        )

    src = candidates[0]
    content = src.read_bytes()
    sha = sha256_hex(content)

    prior = latest_partition_dir(bronze_root, SOURCE_NAME, competence=None)
    prior_manifest = read_manifest(prior) if prior else None
    if prior_manifest and prior_manifest.sha256 == sha:
        log.info("bronze.anbima_175.same_sha", sha256=sha[:12])
        return IngestOutcome(
            source=SOURCE_NAME,
            competence=None,
            status="not_modified",
            partition=prior,
            manifest=prior_manifest,
        )

    part = partition_dir(bronze_root, SOURCE_NAME, today, competence=None)
    part.mkdir(parents=True, exist_ok=True)
    out = part / "raw.xlsx"
    out.write_bytes(content)

    manifest = Manifest(
        source=SOURCE_NAME,
        url=f"file://{src}",
        competence=None,
        etag=None,
        last_modified=None,
        sha256=sha,
        byte_size=len(content),
        row_count=None,
        ingested_at=now_iso(),
        status="fetched",
    )
    write_manifest(part, manifest)
    log.info(
        "bronze.anbima_175.fetched",
        partition=str(part),
        bytes=len(content),
        sha256=sha[:12],
        original_filename=src.name,
    )
    return IngestOutcome(
        source=SOURCE_NAME,
        competence=None,
        status="fetched",
        partition=part,
        manifest=manifest,
    )
