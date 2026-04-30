"""Path helpers for the gold layer.

Mirrors `silver/_io.py:silver_path` — same convention, different root.
"""
from __future__ import annotations

from pathlib import Path

from fund_rank.settings import Settings


def gold_path(settings: Settings, table: str, as_of: str, *parts: str) -> Path:
    """data/gold/{table}/as_of=YYYY-MM-DD/{parts...}/data.parquet"""
    base = settings.gold_root / table / f"as_of={as_of}"
    if parts:
        base = base.joinpath(*parts)
    return base / "data.parquet"
