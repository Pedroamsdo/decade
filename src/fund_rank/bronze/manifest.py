"""Bronze partition layout + manifest sidecar.

Layout:
  data/bronze/{source}/ingested_at={YYYY-MM-DD}[/competence={YYYY-MM}]/raw.{ext}
  data/bronze/{source}/ingested_at={YYYY-MM-DD}[/competence={YYYY-MM}]/_manifest.json
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
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
    ingested_at: str  # ISO timestamp
    status: str        # "fetched" | "not_modified" | "not_found"


def partition_dir(
    bronze_root: Path,
    source: str,
    ingested_at: date,
    competence: str | None = None,
) -> Path:
    parts: list[str] = [str(source), f"ingested_at={ingested_at.isoformat()}"]
    if competence:
        parts.append(f"competence={competence}")
    return bronze_root.joinpath(*parts)


def latest_partition_dir(
    bronze_root: Path,
    source: str,
    competence: str | None = None,
) -> Path | None:
    """Return the most recently ingested partition for (source, competence) or None."""
    src_dir = bronze_root / source
    if not src_dir.exists():
        return None

    candidates: list[Path] = []
    for p in src_dir.iterdir():
        if not p.is_dir() or not p.name.startswith("ingested_at="):
            continue
        if competence:
            cdir = p / f"competence={competence}"
            if cdir.is_dir() and (cdir / "_manifest.json").exists():
                candidates.append(cdir)
        else:
            if (p / "_manifest.json").exists():
                candidates.append(p)
    if not candidates:
        return None
    candidates.sort(key=lambda d: d.parent.name if competence else d.name, reverse=True)
    return candidates[0]


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
