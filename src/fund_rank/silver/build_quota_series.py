"""silver/quota_series — daily quota observations across CVM 175 schemas.

Reads every bronze partition for `cvm_inf_diario` (post-CVM 175 monthly zips)
and `cvm_inf_diario_hist` (pre-CVM 175 yearly zips), unifies the two schemas
into a single 10-column canonical frame with **lowercase columns**, and writes
one parquet:

  silver/quota_series/as_of=YYYY-MM-DD/data.parquet

Output columns (lowercase):
  tp_fundo_classe, cnpj_fundo_classe, id_subclasse, dt_comptc,
  vl_total, vl_quota, vl_patrim_liq, captc_dia, resg_dia, nr_cotst

Pre-CVM 175 era (yearly zips): tp_fundo_classe and id_subclasse are null;
cnpj_fundo_classe is taken from CNPJ_FUNDO. All numeric/date columns map 1:1.

Post-CVM 175 era (monthly zips): all 10 source columns map 1:1 to lowercase.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterator, Literal

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import (
    all_partitions_for,
    cnpj_clean_expr,
    list_zip_members,
    read_csv_from_zip,
    silver_path,
    write_parquet,
)

log = get_logger(__name__)


OUTPUT_COLUMNS: list[str] = [
    "tp_fundo_classe",
    "cnpj_fundo_classe",
    "id_subclasse",
    "dt_comptc",
    "vl_total",
    "vl_quota",
    "vl_patrim_liq",
    "captc_dia",
    "resg_dia",
    "nr_cotst",
]

Era = Literal["pre175", "post175"]


def _iter_inf_diario_zips(settings: Settings) -> Iterator[tuple[Path, Era]]:
    """Yield (zip_path, era) for every available inf_diario partition.

    Pre-CVM 175 era is sourced from `cvm_inf_diario_hist` (yearly).
    Post-CVM 175 era is sourced from `cvm_inf_diario` (monthly).
    """
    for part in all_partitions_for(settings.bronze_root, "cvm_inf_diario_hist"):
        z = part / "raw.zip"
        if z.exists():
            yield z, "pre175"
    for part in all_partitions_for(settings.bronze_root, "cvm_inf_diario"):
        z = part / "raw.zip"
        if z.exists():
            yield z, "post175"


def _normalize_csv(df: pl.DataFrame, era: Era) -> pl.DataFrame:
    """Map a raw CVM CSV DataFrame to the canonical 10-column lowercase schema."""
    cols = set(df.columns)
    has_post_key = "CNPJ_FUNDO_CLASSE" in cols
    has_pre_key = "CNPJ_FUNDO" in cols and "CNPJ_FUNDO_CLASSE" not in cols

    if era == "post175" and not has_post_key and has_pre_key:
        # CVM occasionally re-publishes a month with the legacy schema. Treat as pre.
        log.warning("silver.quota_series.post_zip_with_pre_schema", cols=df.columns)
        era = "pre175"
    elif era == "pre175" and has_post_key:
        log.warning("silver.quota_series.pre_zip_with_post_schema", cols=df.columns)
        era = "post175"

    if era == "post175":
        if "CNPJ_FUNDO_CLASSE" not in cols:
            log.error("silver.quota_series.unknown_schema", era=era, cols=df.columns)
            return pl.DataFrame()
        return df.select(
            pl.col("TP_FUNDO_CLASSE").cast(pl.Utf8, strict=False).alias("tp_fundo_classe")
            if "TP_FUNDO_CLASSE" in cols
            else pl.lit(None, dtype=pl.Utf8).alias("tp_fundo_classe"),
            cnpj_clean_expr("CNPJ_FUNDO_CLASSE", "cnpj_fundo_classe"),
            pl.col("ID_SUBCLASSE").cast(pl.Utf8, strict=False).alias("id_subclasse")
            if "ID_SUBCLASSE" in cols
            else pl.lit(None, dtype=pl.Utf8).alias("id_subclasse"),
            pl.col("DT_COMPTC").cast(pl.Utf8, strict=False).alias("dt_comptc_raw"),
            pl.col("VL_TOTAL").cast(pl.Utf8, strict=False).alias("vl_total_raw")
            if "VL_TOTAL" in cols
            else pl.lit(None, dtype=pl.Utf8).alias("vl_total_raw"),
            pl.col("VL_QUOTA").cast(pl.Utf8, strict=False).alias("vl_quota_raw"),
            pl.col("VL_PATRIM_LIQ").cast(pl.Utf8, strict=False).alias("vl_patrim_liq_raw"),
            pl.col("CAPTC_DIA").cast(pl.Utf8, strict=False).alias("captc_dia_raw")
            if "CAPTC_DIA" in cols
            else pl.lit(None, dtype=pl.Utf8).alias("captc_dia_raw"),
            pl.col("RESG_DIA").cast(pl.Utf8, strict=False).alias("resg_dia_raw")
            if "RESG_DIA" in cols
            else pl.lit(None, dtype=pl.Utf8).alias("resg_dia_raw"),
            pl.col("NR_COTST").cast(pl.Utf8, strict=False).alias("nr_cotst_raw")
            if "NR_COTST" in cols
            else pl.lit(None, dtype=pl.Utf8).alias("nr_cotst_raw"),
        )

    # era == "pre175"
    if "CNPJ_FUNDO" not in cols:
        log.error("silver.quota_series.unknown_schema", era=era, cols=df.columns)
        return pl.DataFrame()
    return df.select(
        pl.lit(None, dtype=pl.Utf8).alias("tp_fundo_classe"),
        cnpj_clean_expr("CNPJ_FUNDO", "cnpj_fundo_classe"),
        pl.lit(None, dtype=pl.Utf8).alias("id_subclasse"),
        pl.col("DT_COMPTC").cast(pl.Utf8, strict=False).alias("dt_comptc_raw"),
        pl.col("VL_TOTAL").cast(pl.Utf8, strict=False).alias("vl_total_raw")
        if "VL_TOTAL" in cols
        else pl.lit(None, dtype=pl.Utf8).alias("vl_total_raw"),
        pl.col("VL_QUOTA").cast(pl.Utf8, strict=False).alias("vl_quota_raw"),
        pl.col("VL_PATRIM_LIQ").cast(pl.Utf8, strict=False).alias("vl_patrim_liq_raw"),
        pl.col("CAPTC_DIA").cast(pl.Utf8, strict=False).alias("captc_dia_raw")
        if "CAPTC_DIA" in cols
        else pl.lit(None, dtype=pl.Utf8).alias("captc_dia_raw"),
        pl.col("RESG_DIA").cast(pl.Utf8, strict=False).alias("resg_dia_raw")
        if "RESG_DIA" in cols
        else pl.lit(None, dtype=pl.Utf8).alias("resg_dia_raw"),
        pl.col("NR_COTST").cast(pl.Utf8, strict=False).alias("nr_cotst_raw")
        if "NR_COTST" in cols
        else pl.lit(None, dtype=pl.Utf8).alias("nr_cotst_raw"),
    )


def _apply_types(df: pl.DataFrame) -> pl.DataFrame:
    """Cast the *_raw columns to their final types and drop the raw versions."""
    return df.with_columns(
        pl.col("dt_comptc_raw").str.to_date(format="%Y-%m-%d", strict=False).alias("dt_comptc"),
        pl.col("vl_total_raw").cast(pl.Float64, strict=False).alias("vl_total"),
        pl.col("vl_quota_raw").cast(pl.Float64, strict=False).alias("vl_quota"),
        pl.col("vl_patrim_liq_raw").cast(pl.Float64, strict=False).alias("vl_patrim_liq"),
        pl.col("captc_dia_raw").cast(pl.Float64, strict=False).alias("captc_dia"),
        pl.col("resg_dia_raw").cast(pl.Float64, strict=False).alias("resg_dia"),
        pl.col("nr_cotst_raw").cast(pl.Int64, strict=False).alias("nr_cotst"),
    ).select(OUTPUT_COLUMNS)


def _read_zip_to_canonical(zip_path: Path, era: Era) -> pl.DataFrame:
    """Read every CSV inside a zip and return the concatenated canonical frame."""
    members = [m for m in list_zip_members(zip_path) if m.lower().endswith(".csv")]
    parts: list[pl.DataFrame] = []
    for m in members:
        try:
            raw = read_csv_from_zip(zip_path, m)
        except Exception as e:
            log.warning("silver.quota_series.csv_read_failed", zip=str(zip_path), member=m, error=str(e))
            continue
        df = _normalize_csv(raw, era)
        if df.is_empty():
            continue
        parts.append(df)
    if not parts:
        return pl.DataFrame()
    return pl.concat(parts, how="vertical_relaxed")


def run(settings: Settings, as_of: date) -> Path:
    pre_rows = 0
    post_rows = 0
    frames: list[pl.DataFrame] = []

    for zip_path, era in _iter_inf_diario_zips(settings):
        df = _read_zip_to_canonical(zip_path, era)
        if df.is_empty():
            continue
        if era == "pre175":
            pre_rows += df.height
        else:
            post_rows += df.height
        frames.append(df)
        log.info(
            "silver.quota_series.zip_read",
            zip=str(zip_path),
            era=era,
            rows=df.height,
        )

    if not frames:
        raise RuntimeError(
            "No inf_diario partitions found in bronze; run `ingest` first."
        )

    combined = pl.concat(frames, how="vertical_relaxed")
    typed = _apply_types(combined)

    pre_cutoff_rows = typed.height
    typed = typed.filter(pl.col("dt_comptc") <= as_of)
    cutoff_dropped = pre_cutoff_rows - typed.height

    before = typed.height
    typed = typed.unique(
        subset=["cnpj_fundo_classe", "id_subclasse", "dt_comptc"],
        keep="first",
        maintain_order=True,
    )
    deduped = before - typed.height

    out_path = silver_path(settings, "quota_series", as_of.isoformat())
    write_parquet(typed, out_path)

    log.info(
        "silver.quota_series.written",
        path=str(out_path),
        rows=typed.height,
        pre175_rows=pre_rows,
        post175_rows=post_rows,
        cutoff_dropped=cutoff_dropped,
        deduped=deduped,
        distinct_cnpj=typed["cnpj_fundo_classe"].n_unique(),
        dt_min=str(typed["dt_comptc"].min()),
        dt_max=str(typed["dt_comptc"].max()),
    )
    return out_path
