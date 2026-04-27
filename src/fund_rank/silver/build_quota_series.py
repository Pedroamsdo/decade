"""silver/quota_series — daily quotas normalized across CVM 175 schemas.

Reads all bronze partitions for cvm_inf_diario (and optionally cvm_inf_diario_hist),
unifies pre- vs post-CVM 175 column names, parses types, computes log returns,
and flags anomalous jumps.

Output:
  silver/quota_series/as_of=YYYY-MM-DD/data.parquet
columns:
  cnpj_classe, cnpj_fundo, dt_comptc, vl_quota, vl_patrim_liq,
  captc_dia, resg_dia, nr_cotst, log_return, jump_flag
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import (
    all_partitions_for,
    list_zip_members,
    read_csv_from_zip,
    silver_path,
    write_parquet,
)

log = get_logger(__name__)


def _cnpj_clean(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .str.replace_all(r"\D", "")
        .str.pad_start(14, "0")
        .str.slice(0, 14)
    )


def _read_one_inf_diario_zip(zip_path: Path) -> pl.DataFrame:
    """Read one INF_DIARIO ZIP, returning a normalized DataFrame.

    Handles both pre- and post-CVM 175 schemas.
    """
    members = [m for m in list_zip_members(zip_path) if m.endswith(".csv")]
    frames: list[pl.DataFrame] = []
    for m in members:
        df = read_csv_from_zip(zip_path, m)
        cols = set(df.columns)
        if {"CNPJ_FUNDO_CLASSE", "DT_COMPTC", "VL_QUOTA"} <= cols:
            # post-CVM 175
            df_n = df.select(
                _cnpj_clean("CNPJ_FUNDO_CLASSE").alias("cnpj_classe"),
                pl.lit(None, dtype=pl.Utf8).alias("cnpj_fundo"),
                pl.col("DT_COMPTC").str.to_date(format="%Y-%m-%d", strict=False).alias("dt_comptc"),
                pl.col("VL_QUOTA").cast(pl.Float64, strict=False).alias("vl_quota"),
                pl.col("VL_PATRIM_LIQ").cast(pl.Float64, strict=False).alias("vl_patrim_liq"),
                pl.col("CAPTC_DIA").cast(pl.Float64, strict=False).alias("captc_dia"),
                pl.col("RESG_DIA").cast(pl.Float64, strict=False).alias("resg_dia"),
                pl.col("NR_COTST").cast(pl.Int64, strict=False).alias("nr_cotst"),
            )
        elif {"CNPJ_FUNDO", "DT_COMPTC", "VL_QUOTA"} <= cols:
            # pre-CVM 175
            df_n = df.select(
                pl.lit(None, dtype=pl.Utf8).alias("cnpj_classe"),
                _cnpj_clean("CNPJ_FUNDO").alias("cnpj_fundo"),
                pl.col("DT_COMPTC").str.to_date(format="%Y-%m-%d", strict=False).alias("dt_comptc"),
                pl.col("VL_QUOTA").cast(pl.Float64, strict=False).alias("vl_quota"),
                pl.col("VL_PATRIM_LIQ").cast(pl.Float64, strict=False).alias("vl_patrim_liq"),
                pl.col("CAPTC_DIA").cast(pl.Float64, strict=False).alias("captc_dia"),
                pl.col("RESG_DIA").cast(pl.Float64, strict=False).alias("resg_dia"),
                pl.col("NR_COTST").cast(pl.Int64, strict=False).alias("nr_cotst"),
            )
        else:
            log.warning("silver.quota_series.unknown_schema", path=str(zip_path), member=m, cols=df.columns)
            continue
        frames.append(df_n)

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical_relaxed")


def _detect_jumps(df: pl.DataFrame, sigma: float = 5.0, window: int = 60) -> pl.DataFrame:
    """Add jump_flag = True where |log_return - rolling_mean| > sigma * rolling_std."""
    return (
        df.with_columns(
            pl.col("log_return").rolling_mean(window).over("series_id").alias("_roll_mean"),
            pl.col("log_return").rolling_std(window).over("series_id").alias("_roll_std"),
        )
        .with_columns(
            (
                (pl.col("log_return") - pl.col("_roll_mean")).abs()
                > sigma * pl.col("_roll_std")
            )
            .fill_null(False)
            .alias("jump_flag")
        )
        .drop("_roll_mean", "_roll_std")
    )


def _build_stitch_table(funds_path: Path) -> pl.DataFrame:
    """Build cnpj_fundo (umbrella) -> cnpj_classe map for 1-to-1 cases.

    Pre-CVM 175 INF_DIARIO reports under CNPJ_FUNDO. Post-adaptation, the
    fundo guarda-chuva contains 1+ classes with their own CNPJs. Where there is
    exactly one class per fundo, we attribute the legacy series to that class.
    """
    if not funds_path.exists():
        log.warning("silver.stitch.no_funds_table")
        return pl.DataFrame(schema={
            "cnpj_fundo": pl.Utf8,
            "cnpj_classe_target": pl.Utf8,
        })
    funds = pl.read_parquet(funds_path)
    if "cnpj_fundo" not in funds.columns or "cnpj_classe" not in funds.columns:
        return pl.DataFrame(schema={
            "cnpj_fundo": pl.Utf8,
            "cnpj_classe_target": pl.Utf8,
        })
    grouped = (
        funds.filter(pl.col("cnpj_fundo").is_not_null() & pl.col("cnpj_classe").is_not_null())
        .group_by("cnpj_fundo")
        .agg(
            pl.col("cnpj_classe").alias("classes"),
            pl.len().alias("n_classes"),
        )
        .filter(pl.col("n_classes") == 1)
        .select(
            "cnpj_fundo",
            pl.col("classes").list.first().alias("cnpj_classe_target"),
        )
    )
    log.info("silver.stitch.table_built", rows=len(grouped))
    return grouped


def run(
    settings: Settings,
    as_of: date,
    *,
    sigma: float = 5.0,
    window: int = 60,
) -> Path:
    parts = all_partitions_for(settings.bronze_root, "cvm_inf_diario")
    parts += all_partitions_for(settings.bronze_root, "cvm_inf_diario_hist")
    if not parts:
        raise FileNotFoundError("No bronze partition for cvm_inf_diario; run `ingest` first.")

    log.info("silver.quota_series.start", partitions=len(parts))
    frames: list[pl.DataFrame] = []
    for p in parts:
        zips = list(p.glob("raw.zip"))
        if not zips:
            continue
        df = _read_one_inf_diario_zip(zips[0])
        if df.is_empty():
            continue
        frames.append(df)
        log.info("silver.quota_series.read", source_dir=str(p), rows=len(df))

    if not frames:
        raise RuntimeError("All INF_DIARIO partitions yielded zero rows; check encoding.")

    df = pl.concat(frames, how="vertical_relaxed")

    # CVM 175 stitch: legacy CNPJ_FUNDO records get re-tagged with the unique class CNPJ
    funds_path = silver_path(settings, "funds", as_of.isoformat()).parent / "data.parquet"
    stitch = _build_stitch_table(funds_path)
    df = df.join(stitch, on="cnpj_fundo", how="left")

    df = df.with_columns(
        pl.coalesce([pl.col("cnpj_classe"), pl.col("cnpj_classe_target")]).alias("series_id"),
        pl.when(pl.col("cnpj_classe").is_null() & pl.col("cnpj_classe_target").is_not_null())
        .then(pl.lit("stitched_cvm175"))
        .when(pl.col("cnpj_classe").is_not_null())
        .then(pl.lit("own"))
        .otherwise(pl.lit("orphan_pre_cvm175"))
        .alias("history_source"),
    ).filter(
        pl.col("dt_comptc").is_not_null()
        & pl.col("vl_quota").is_not_null()
        & (pl.col("vl_quota") > 0)
        & pl.col("series_id").is_not_null()
    )

    # Drop the helper column
    df = df.drop("cnpj_classe_target")

    # log returns within each series, ordered by date
    df = df.sort(["series_id", "dt_comptc"])
    df = df.with_columns(
        (pl.col("vl_quota").log() - pl.col("vl_quota").shift(1).log())
        .over("series_id")
        .alias("log_return")
    )
    df = _detect_jumps(df, sigma=sigma, window=window)

    out = silver_path(settings, "quota_series", as_of.isoformat()).parent / "data.parquet"
    write_parquet(df, out)
    log.info(
        "silver.quota_series.written",
        path=str(out),
        rows=len(df),
        series=df["series_id"].n_unique(),
        stitched=df.filter(pl.col("history_source") == "stitched_cvm175").height,
        orphan=df.filter(pl.col("history_source") == "orphan_pre_cvm175").height,
    )
    return out
