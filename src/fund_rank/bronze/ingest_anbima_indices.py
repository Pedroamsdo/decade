"""Ingest ANBIMA indices históricos (XLS drop-based, no HTTP).

A API pública do `data.anbima.com.br` é protegida por reCAPTCHA v3 e a API
oficial (`api.anbima.com.br/feed/`) requer OAuth pago. O usuário baixa
manualmente o XLS de cada índice no portal e dropa em
``data/bronze/anbima_indices/dropped/``. Este ingestor move/copia para
partições manifest-tracked, idempotentes por sha256 do conteúdo.

O mapeamento `filename → competence` vem de ``configs/benchmarks.yaml`` —
cada índice ANBIMA tem um campo ``drop_filename`` que casa com o XLS
publicado pelo portal (ex.: ``IMAB-HISTORICO.xls`` → coluna ``ima_b``).
Arquivos com nomes não reconhecidos são ignorados (com aviso).
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

SOURCE_NAME = "anbima_indices"
DROP_SUBDIR = "dropped"


def _drop_filename_index(settings: Settings) -> dict[str, str]:
    """Build a `{normalized_filename: competence}` index from `benchmarks.yaml`.

    Only entries with `source: anbima_drop` and a `drop_filename` are included.
    Filename matching is case-insensitive and ignores extension.
    """
    out: dict[str, str] = {}
    for competence, cfg in settings.benchmarks.items():
        if not isinstance(cfg, dict):
            continue
        if cfg.get("source") != "anbima_drop":
            continue
        fname = cfg.get("drop_filename")
        if not fname:
            continue
        key = fname.rsplit(".", 1)[0].lower()
        out[key] = competence
    return out


def _filename_to_competence(filename: str, idx: dict[str, str]) -> str | None:
    """Map a dropped XLS filename to its competence using the config index."""
    stem = filename.rsplit(".", 1)[0].lower()
    return idx.get(stem)


def _ingest_one_drop(
    settings: Settings,
    today: date,
    src_path: Path,
    competence: str,
) -> IngestOutcome:
    bronze_root = settings.bronze_root
    content = src_path.read_bytes()
    sha = sha256_hex(content)

    prior = latest_partition_dir(bronze_root, SOURCE_NAME, competence=competence)
    prior_manifest = read_manifest(prior) if prior else None
    if prior_manifest and prior_manifest.sha256 == sha:
        log.info(
            "bronze.anbima_indices.same_sha",
            competence=competence,
            sha256=sha[:12],
        )
        return IngestOutcome(
            source=SOURCE_NAME,
            competence=competence,
            status="not_modified",
            partition=prior,
            manifest=prior_manifest,
        )

    part = partition_dir(bronze_root, SOURCE_NAME, today, competence=competence)
    part.mkdir(parents=True, exist_ok=True)
    out = part / "raw.xlsx"
    out.write_bytes(content)

    manifest = Manifest(
        source=SOURCE_NAME,
        url=f"file://{src_path}",
        competence=competence,
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
        "bronze.anbima_indices.fetched",
        competence=competence,
        partition=str(part),
        bytes=len(content),
        sha256=sha[:12],
        original_filename=src_path.name,
    )
    return IngestOutcome(
        source=SOURCE_NAME,
        competence=competence,
        status="fetched",
        partition=part,
        manifest=manifest,
    )


def run(settings: Settings, today: date | None = None) -> list[IngestOutcome]:
    today = today or date.today()
    bronze_root = settings.bronze_root
    drop_dir = bronze_root / SOURCE_NAME / DROP_SUBDIR
    drop_dir.mkdir(parents=True, exist_ok=True)

    fname_idx = _drop_filename_index(settings)
    expected_competences = set(fname_idx.values())

    candidates = sorted(p for p in drop_dir.iterdir() if p.is_file() and p.suffix.lower() in (".xls", ".xlsx"))
    if not candidates:
        log.warning(
            "bronze.anbima_indices.no_drops",
            drop_dir=str(drop_dir),
            hint=f"place ANBIMA XLS files matching: {sorted(fname_idx.keys())}",
        )
        return []

    outcomes: list[IngestOutcome] = []
    seen_competences: set[str] = set()
    for src in candidates:
        competence = _filename_to_competence(src.name, fname_idx)
        if competence is None:
            log.warning(
                "bronze.anbima_indices.unknown_filename",
                filename=src.name,
                expected=sorted(fname_idx.keys()),
            )
            continue
        if competence in seen_competences:
            log.warning(
                "bronze.anbima_indices.duplicate_competence",
                competence=competence,
                filename=src.name,
            )
            continue
        seen_competences.add(competence)
        outcomes.append(_ingest_one_drop(settings, today, src, competence))

    fetched = sum(1 for o in outcomes if o.status == "fetched")
    not_modified = sum(1 for o in outcomes if o.status == "not_modified")
    log.info(
        "bronze.anbima_indices.done",
        total=len(outcomes),
        fetched=fetched,
        not_modified=not_modified,
        missing=sorted(expected_competences - seen_competences),
    )
    return outcomes
