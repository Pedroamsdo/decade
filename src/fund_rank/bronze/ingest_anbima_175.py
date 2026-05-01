"""Validate ANBIMA FUNDOS-175 drop (no partition copy).

The XLS dropped by the user under ``data/bronze/anbima_175/dropped/`` is the
bronze artifact itself — it's committed to the repo and read directly by the
silver layer. This function only validates that a file is present.
"""
from __future__ import annotations

from fund_rank.bronze._common import IngestOutcome
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings

log = get_logger(__name__)

SOURCE_NAME = "anbima_175"
DROP_SUBDIR = "dropped"


def run(settings: Settings) -> IngestOutcome:
    drop_dir = settings.bronze_root / SOURCE_NAME / DROP_SUBDIR
    candidates = sorted(drop_dir.glob("*.xlsx")) if drop_dir.exists() else []

    if not candidates:
        log.warning(
            "bronze.anbima_175.no_drop",
            drop_dir=str(drop_dir),
            hint="place FUNDOS-175-CARACTERISTICAS-PUBLICO.xlsx here",
        )
        return IngestOutcome(
            source=SOURCE_NAME, competence=None, status="not_found",
            partition=None, manifest=None,
        )

    src = candidates[0]
    log.info("bronze.anbima_175.present", path=str(src), bytes=src.stat().st_size)
    return IngestOutcome(
        source=SOURCE_NAME, competence=None, status="not_modified",
        partition=src.parent, manifest=None,
    )
