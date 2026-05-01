"""Bronze partition layout + manifest sidecar.

Layout:
  data/bronze/{source}[/competence={…}]/raw.{ext}
  data/bronze/{source}[/competence={…}]/_manifest.json

The path is canonical (no ``ingested_at=…`` partition). The run date lives
in ``Manifest.ingested_at`` and is rewritten on every run, even when content
did not change (etag/sha hit).
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class Manifest:
    source: str
    url: str
    competence: str | None
    etag: str | None
    last_modified: str | None
    sha256: str
    byte_size: int
    row_count: int | None
    ingested_at: str  # ISO timestamp — last time this run touched the source
    status: str        # "fetched" | "not_modified" | "not_found"


def partition_dir(
    bronze_root: Path,
    source: str,
    competence: str | None = None,
) -> Path:
    """Canonical bronze path for (source, competence)."""
    base = bronze_root / source
    if competence:
        base = base / f"competence={competence}"
    return base


def read_manifest(part_dir: Path) -> Manifest | None:
    f = part_dir / "_manifest.json"
    if not f.exists():
        return None
    raw = json.loads(f.read_text())
    return Manifest(**raw)


def write_manifest(part_dir: Path, manifest: Manifest) -> None:
    part_dir.mkdir(parents=True, exist_ok=True)
    (part_dir / "_manifest.json").write_text(
        json.dumps(asdict(manifest), indent=2, ensure_ascii=False, sort_keys=True)
    )


def write_payload(part_dir: Path, content: bytes, extension: str) -> Path:
    part_dir.mkdir(parents=True, exist_ok=True)
    out = part_dir / f"raw.{extension}"
    out.write_bytes(content)
    return out


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"
