"""silver/funds — typed dimension table at CLASSE level.

Joins:
  - registro_classe.csv (post-CVM 175 source of truth for classe-level metadata)
  - registro_fundo.csv  (umbrella fund attributes + Data_Adaptacao_RCVM175)
  - cad_fi.csv          (taxa_adm, taxa_perfm text, fee schedules)

Output schema (silver/funds/as_of=.../data.parquet):
  cnpj_classe, cnpj_fundo, denom_social, classe_anbima_raw, classe_anbima_norm,
  tipo_classe, situacao, condominio, exclusivo, publico_alvo, trib_lprazo,
  taxa_adm_pct, taxa_perfm_text, dt_inicio, dt_adaptacao_175,
  cnpj_administrador, cnpj_gestor.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import (
    all_partitions_for,
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
