"""silver/funds — typed dimension table at CLASSE level.

Joins:
  - registro_classe.csv (post-CVM 175 source of truth for classe-level metadata)
  - registro_fundo.csv  (umbrella fund attributes + Data_Adaptacao_RCVM175)
  - cad_fi.csv          (taxa_adm, taxa_perfm text, fee schedules)
  - cda_fi BLC_2        (master/feeder edges where ≥95% holdings in single target)

Output schema (silver/funds/as_of=.../data.parquet):
  cnpj_classe, cnpj_fundo, denom_social, classe_anbima_raw, classe_anbima_norm,
  tipo_classe, situacao, condominio, exclusivo, publico_alvo, trib_lprazo,
  taxa_adm_pct, taxa_perfm_text, dt_inicio, dt_adaptacao_175, cnpj_master,
  master_share, cnpj_administrador, cnpj_gestor.
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
    normalize_cnpj,
    normalize_text,
    read_csv_from_path,
    read_csv_from_zip,
    silver_path,
    write_parquet,
)

log = get_logger(__name__)


def _cnpj_clean_expr(col: str) -> pl.Expr:
    """Strip non-digits, zfill 14."""
    return (
        pl.col(col)
        .str.replace_all(r"\D", "")
        .str.pad_start(14, "0")
        .str.slice(0, 14)
        .alias(col)
    )


def _normalize_text_expr(col: str, alias: str) -> pl.Expr:
    """polars-native text normalization: strip accents (best-effort), lowercase, collapse whitespace."""
    return (
        pl.col(col)
        .str.normalize("NFKD")
        .str.replace_all(r"[̀-ͯ]", "")  # combining marks
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.to_lowercase()
        .alias(alias)
    )


def _read_registro_classe(settings: Settings) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    parts = all_partitions_for(settings.bronze_root, "cvm_registro_classe")
    if not parts:
        raise FileNotFoundError("No bronze partition for cvm_registro_classe; run `ingest` first.")
    zip_path = parts[-1] / "raw.zip"

    df_classe = read_csv_from_zip(zip_path, "registro_classe.csv")
    df_fundo = read_csv_from_zip(zip_path, "registro_fundo.csv")
    df_subclasse = read_csv_from_zip(zip_path, "registro_subclasse.csv")

    log.info(
        "silver.read_registro_classe",
        classes=len(df_classe),
        fundos=len(df_fundo),
        subclasses=len(df_subclasse),
    )
    return df_classe, df_fundo, df_subclasse


def _read_cad_fi(settings: Settings) -> pl.DataFrame:
    parts = all_partitions_for(settings.bronze_root, "cvm_cad_fi")
    if not parts:
        raise FileNotFoundError("No bronze partition for cvm_cad_fi.")
    csv_path = parts[-1] / "raw.csv"
    df = read_csv_from_path(csv_path)
    log.info("silver.read_cad_fi", rows=len(df), cols=len(df.columns))
    return df


def _detect_master_feeder(settings: Settings, threshold: float = 0.95) -> pl.DataFrame:
    """Read CDA BLC_2 (Cotas de Fundos), aggregate per (cnpj_classe, cnpj_target),
    and for each cnpj_classe pick the largest target. If its share >= threshold,
    classify as feeder.

    Returns: cnpj_classe, cnpj_master, master_share
    """
    parts = all_partitions_for(settings.bronze_root, "cvm_cda")
    if not parts:
        log.warning("silver.master_feeder.no_cda")
        return pl.DataFrame(schema={
            "cnpj_classe": pl.Utf8,
            "cnpj_master": pl.Utf8,
            "master_share": pl.Float64,
        })

    # Use the latest CDA partition only (master/feeder is slow-changing)
    zip_path = parts[-1] / "raw.zip"
    members = list_zip_members(zip_path)
    blc2 = next((m for m in members if "BLC_2" in m), None)
    pl_member = next((m for m in members if m.startswith("cda_fi_PL_")), None)
    if not blc2 or not pl_member:
        log.warning("silver.master_feeder.missing_blocks", members=members)
        return pl.DataFrame(schema={
            "cnpj_classe": pl.Utf8,
            "cnpj_master": pl.Utf8,
            "master_share": pl.Float64,
        })

    df_blc = read_csv_from_zip(zip_path, blc2)
    df_pl = read_csv_from_zip(zip_path, pl_member)

    # Filter to actual cotas-de-fundos rows
    if "TP_APLIC" in df_blc.columns:
        df_blc = df_blc.filter(pl.col("TP_APLIC").str.contains("Cotas de Fundos", literal=False))
    if df_blc.height == 0:
        log.warning("silver.master_feeder.no_cotas_de_fundos_rows")
        return pl.DataFrame(schema={
            "cnpj_classe": pl.Utf8,
            "cnpj_master": pl.Utf8,
            "master_share": pl.Float64,
        })

    # Pick CNPJ identification columns. Post-CVM 175 uses CNPJ_FUNDO_CLASSE.
    src_col = "CNPJ_FUNDO_CLASSE" if "CNPJ_FUNDO_CLASSE" in df_blc.columns else "CNPJ_FUNDO"
    # Target column varies; CVM exports often use CNPJ_FUNDO_COTA or EMISSOR-related; fall back via VL_MERC_POS_FINAL aggregation.
    tgt_candidates = [c for c in ["CNPJ_FUNDO_COTA", "CNPJ_FUNDO_CLASSE_COTA", "CNPJ_EMISSOR"] if c in df_blc.columns]
    if not tgt_candidates:
        log.warning("silver.master_feeder.no_target_cnpj", cols=df_blc.columns)
        return pl.DataFrame(schema={
            "cnpj_classe": pl.Utf8,
            "cnpj_master": pl.Utf8,
            "master_share": pl.Float64,
        })
    tgt_col = tgt_candidates[0]

    df_holdings = (
        df_blc.select(
            pl.col(src_col).alias("cnpj_classe_raw"),
            pl.col(tgt_col).alias("cnpj_master_raw"),
            pl.col("VL_MERC_POS_FINAL").cast(pl.Float64, strict=False).alias("vl_merc"),
        )
        .with_columns(
            _cnpj_clean_expr("cnpj_classe_raw").alias("cnpj_classe"),
            _cnpj_clean_expr("cnpj_master_raw").alias("cnpj_master"),
        )
        .filter(pl.col("cnpj_classe") != pl.col("cnpj_master"))
        .group_by(["cnpj_classe", "cnpj_master"])
        .agg(pl.col("vl_merc").sum())
    )

    # Total per source
    df_total = (
        df_blc.select(
            pl.col(src_col).alias("cnpj_classe_raw"),
            pl.col("VL_MERC_POS_FINAL").cast(pl.Float64, strict=False).alias("vl_merc"),
        )
        .with_columns(_cnpj_clean_expr("cnpj_classe_raw").alias("cnpj_classe"))
        .group_by("cnpj_classe")
        .agg(pl.col("vl_merc").sum().alias("vl_merc_total"))
    )

    df_top = (
        df_holdings.join(df_total, on="cnpj_classe", how="left")
        .with_columns(
            (pl.col("vl_merc") / pl.col("vl_merc_total")).alias("master_share")
        )
        .sort(["cnpj_classe", "master_share"], descending=[False, True])
        .group_by("cnpj_classe", maintain_order=True)
        .head(1)
        .filter(pl.col("master_share") >= threshold)
        .select("cnpj_classe", "cnpj_master", "master_share")
    )
    log.info("silver.master_feeder.detected", feeders=len(df_top))
    return df_top


def run(settings: Settings, as_of: date) -> Path:
    """Build silver/funds dimension."""
    df_classe, df_fundo, _df_sub = _read_registro_classe(settings)
    df_cad = _read_cad_fi(settings)

    # --- Build classe-level frame from registro_classe ---
    classe = (
        df_classe.with_columns(
            _cnpj_clean_expr("CNPJ_Classe").alias("cnpj_classe"),
            pl.col("ID_Registro_Fundo").cast(pl.Int64, strict=False),
        )
        .select(
            "cnpj_classe",
            pl.col("ID_Registro_Fundo").alias("id_registro_fundo"),
            pl.col("Denominacao_Social").alias("denom_social"),
            pl.col("Tipo_Classe").alias("tipo_classe"),
            pl.col("Situacao").alias("situacao"),
            pl.col("Forma_Condominio").alias("condominio"),
            pl.col("Exclusivo").alias("exclusivo"),
            pl.col("Publico_Alvo").alias("publico_alvo"),
            pl.col("Tributacao_Longo_Prazo").alias("trib_lprazo"),
            pl.col("Classificacao_Anbima").alias("classe_anbima_raw"),
            _normalize_text_expr("Classificacao_Anbima", "classe_anbima_norm"),
            pl.col("Data_Inicio").alias("dt_inicio_classe"),
        )
    )

    # --- Umbrella fund attributes ---
    fundo = (
        df_fundo.with_columns(
            _cnpj_clean_expr("CNPJ_Fundo").alias("cnpj_fundo"),
            pl.col("ID_Registro_Fundo").cast(pl.Int64, strict=False),
        )
        .select(
            pl.col("ID_Registro_Fundo").alias("id_registro_fundo"),
            "cnpj_fundo",
            pl.col("Tipo_Fundo").alias("tipo_fundo"),
            pl.col("Data_Adaptacao_RCVM175").alias("dt_adaptacao_175"),
            pl.col("CNPJ_Administrador").alias("cnpj_administrador_raw"),
            pl.col("CPF_CNPJ_Gestor").alias("cnpj_gestor_raw"),
        )
        .with_columns(
            _cnpj_clean_expr("cnpj_administrador_raw").alias("cnpj_administrador"),
            _cnpj_clean_expr("cnpj_gestor_raw").alias("cnpj_gestor"),
        )
        .drop("cnpj_administrador_raw", "cnpj_gestor_raw")
    )

    funds = classe.join(fundo, on="id_registro_fundo", how="left")

    # --- CAD fees by CNPJ_FUNDO (umbrella). For pre-CVM 175 era, CAD has fees
    #     at fundo-level which we attribute to all classes under that fundo.
    cad_fees = (
        df_cad.select(
            _cnpj_clean_expr("CNPJ_FUNDO").alias("cnpj_fundo"),
            pl.col("TAXA_ADM").cast(pl.Float64, strict=False).alias("taxa_adm_pct"),
            pl.col("TAXA_PERFM").alias("taxa_perfm_text"),
        )
        .group_by("cnpj_fundo")
        .agg(pl.col("taxa_adm_pct").mean(), pl.col("taxa_perfm_text").first())
    )

    funds = funds.join(cad_fees, on="cnpj_fundo", how="left")

    # --- Master/feeder ---
    mf = _detect_master_feeder(settings)
    funds = funds.join(mf, on="cnpj_classe", how="left")

    # Dedupe on cnpj_classe — registro_classe occasionally has multiple rows
    # per class across history snapshots; we keep the most recent by dt_inicio_classe.
    before = len(funds)
    funds = (
        funds.sort("dt_inicio_classe", descending=True, nulls_last=True)
        .unique(subset=["cnpj_classe"], keep="first", maintain_order=True)
    )
    if len(funds) != before:
        log.info("silver.funds.dedupe", before=before, after=len(funds))

    out = silver_path(settings, "funds", as_of.isoformat()).parent / "data.parquet"
    write_parquet(funds, out)
    log.info("silver.funds.written", path=str(out), rows=len(funds))
    return out
