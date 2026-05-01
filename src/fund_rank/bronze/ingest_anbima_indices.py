"""Validate ANBIMA indices drop (no partition copy).

Each XLS dropped under ``data/bronze/anbima_indices/dropped/`` is the bronze
artifact itself — committed to the repo and read directly by the silver layer
(`build_index_series._anbima_xlsx_to_frame` resolves filename via
`benchmarks.yaml: drop_filename`). This function only validates presence and
warns about unknown/missing files.
"""
from __future__ import annotations

from fund_rank.bronze._common import IngestOutcome
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings

log = get_logger(__name__)

SOURCE_NAME = "anbima_indices"
DROP_SUBDIR = "dropped"


def _drop_filename_index(settings: Settings) -> dict[str, str]:
    """Build `{normalized_filename_stem: competence}` from `benchmarks.yaml`."""
    out: dict[str, str] = {}
    for competence, cfg in settings.benchmarks.items():
        if not isinstance(cfg, dict):
            continue
        if cfg.get("source") != "anbima_drop":
            continue
        fname = cfg.get("drop_filename")
        if not fname:
            continue
        out[fname.rsplit(".", 1)[0].lower()] = competence
    return out


def _filename_to_competence(filename: str, idx: dict[str, str]) -> str | None:
    """Map a dropped XLS filename (case-insensitive, ignoring extension) to its competence."""
    return idx.get(filename.rsplit(".", 1)[0].lower())


def run(settings: Settings) -> list[IngestOutcome]:
    drop_dir = settings.bronze_root / SOURCE_NAME / DROP_SUBDIR
    fname_idx = _drop_filename_index(settings)
    expected_competences = set(fname_idx.values())

    if not drop_dir.exists():
        log.warning("bronze.anbima_indices.no_drops", drop_dir=str(drop_dir))
        return []

    candidates = sorted(
        p for p in drop_dir.iterdir()
        if p.is_file() and p.suffix.lower() in (".xls", ".xlsx")
    )

    seen_competences: set[str] = set()
    outcomes: list[IngestOutcome] = []
    for src in candidates:
        competence = _filename_to_competence(src.name, fname_idx)
        if competence is None:
            log.warning(
                "bronze.anbima_indices.unknown_filename",
                filename=src.name,
                expected=sorted(fname_idx.keys()),
            )
            continue
        seen_competences.add(competence)
        outcomes.append(IngestOutcome(
            source=SOURCE_NAME, competence=competence, status="not_modified",
            partition=src.parent, manifest=None,
        ))

    log.info(
        "bronze.anbima_indices.done",
        total=len(outcomes),
        present=sorted(seen_competences),
        missing=sorted(expected_competences - seen_competences),
    )
    return outcomes
