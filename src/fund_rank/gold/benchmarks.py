"""gold/benchmarks — load CDI series from BCB SGS bronze JSON.

BCB SGS series 12 returns daily % rate. We convert to a daily multiplier
(1 + r/100) and expose:
  - load_cdi_daily(): polars Frame [dt, cdi_factor, cdi_pct_dia]
  - load_cdi_aligned(dates, asof): cumulative CDI factor aligned to fund dates
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.bronze.manifest import latest_partition_dir
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings

log = get_logger(__name__)


def load_cdi_daily(settings: Settings) -> pl.DataFrame:
    """Returns a DataFrame with columns:
        dt: Date
        cdi_pct_dia: Float64 (% per day, e.g. 0.0518)
        cdi_factor: Float64 (1 + r/100, e.g. 1.000518)
    """
    src_dir = settings.bronze_root / "bcb_cdi"
    if not src_dir.exists():
        raise FileNotFoundError(f"No CDI bronze partition at {src_dir}.")

    candidates: list[Path] = []
    for ingest_dir in src_dir.iterdir():
        if not ingest_dir.is_dir():
            continue
        for comp_dir in ingest_dir.iterdir():
            if comp_dir.is_dir() and (comp_dir / "raw.json").exists():
                candidates.append(comp_dir)
    if not candidates:
        raise FileNotFoundError("No CDI raw.json found in any partition.")

    # Pick the partition whose competence range is widest (earliest start).
    # Competence name format: "YYYY-MM-DD_to_YYYY-MM-DD"
    def _start_of(p: Path) -> str:
        comp = p.name.split("=", 1)[1]
        return comp.split("_to_", 1)[0]

    candidates.sort(key=_start_of)
    raw = json.loads((candidates[0] / "raw.json").read_text())

    df = pl.DataFrame(raw).with_columns(
        pl.col("data").str.to_date(format="%d/%m/%Y").alias("dt"),
        pl.col("valor").cast(pl.Float64).alias("cdi_pct_dia"),
    ).select(
        "dt",
        "cdi_pct_dia",
        (1.0 + pl.col("cdi_pct_dia") / 100.0).alias("cdi_factor"),
    )
    log.info("gold.cdi.loaded", rows=len(df), first=str(df["dt"].min()), last=str(df["dt"].max()))
    return df


def cdi_cum_factor(cdi_daily: pl.DataFrame, start: date, end: date) -> float | None:
    """Cumulative CDI factor between [start, end] inclusive (uses dt > start, dt <= end)."""
    span = cdi_daily.filter((pl.col("dt") > start) & (pl.col("dt") <= end))
    if span.is_empty():
        return None
    return float(span["cdi_factor"].product())
