"""silver/class_funds — typed dimension at CLASSE level (1 row per cnpj_classe).

Excludes classes that appear in registro_subclasse.csv (those go to subclass_funds).

Joins:
  - registro_classe.csv (CVM 175 source of truth at classe level)
  - registro_fundo.csv  (umbrella attributes; via ID_Registro_Fundo)
  - ANBIMA FUNDOS-175 xlsx (Tipo ANBIMA + product attributes; via (cnpj_fundo, cnpj_classe))
  - cad_fi_hist_taxa_adm.csv  (most-recent TAXA_ADM per CNPJ_Fundo)
  - cad_fi_hist_taxa_perfm.csv (most-recent VL_TAXA_PERFM per CNPJ_Fundo)
  - cad_fi_rentab.csv          (most-recent RENTAB_FUNDO per CNPJ_Fundo)

Output (17 cols): cnpj_fundo, cnpj_classe, denom_social_fundo, denom_social_classe,
situacao, data_de_inicio, exclusivo, publico_alvo, condominio, classificacao_anbima,
composicao_fundos, tributacao_alvo, aplicacao_minima, prazo_de_resgate, taxa_adm,
taxa_perform, benchmark.

Side effect: writes a markdown quality report listing nulls per column and
duplicates by cnpj_classe at reports/as_of=YYYY-MM-DD/class_funds_quality.md.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.bronze.manifest import latest_partition_dir
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import (
    cnpj_clean_expr,
    read_cad_fi_hist_latest,
    read_csv_from_zip,
    silver_path,
    write_parquet,
)

log = get_logger(__name__)


OUTPUT_COLUMNS: list[str] = [
    "cnpj_fundo",
    "cnpj_classe",
    "denom_social_fundo",
    "denom_social_classe",
    "situacao",
    "data_de_inicio",
    "exclusivo",
    "publico_alvo",
    "condominio",
    "classificacao_anbima",
    "composicao_fundos",
    "tributacao_alvo",
    "aplicacao_minima",
    "prazo_de_resgate",
    "taxa_adm",
    "taxa_perform",
    "benchmark",
]


def _read_registro_classe_zip(settings: Settings) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    part = latest_partition_dir(settings.bronze_root, "cvm_registro_classe")
    if part is None:
        raise FileNotFoundError(
            "No bronze partition for cvm_registro_classe; run `ingest` first."
        )
    zip_path = part / "raw.zip"
    df_classe = read_csv_from_zip(zip_path, "registro_classe.csv")
    df_fundo = read_csv_from_zip(zip_path, "registro_fundo.csv")
    df_subclasse = read_csv_from_zip(zip_path, "registro_subclasse.csv")
    log.info(
        "silver.class_funds.read_registro_classe",
        classes=len(df_classe),
        fundos=len(df_fundo),
        subclasses=len(df_subclasse),
    )
    return df_classe, df_fundo, df_subclasse


def _apply_subclass_filter(df_classe: pl.DataFrame, df_subclasse: pl.DataFrame) -> pl.DataFrame:
    """Anti-join: drop classes that appear in registro_subclasse."""
    if df_subclasse.is_empty() or "ID_Registro_Classe" not in df_subclasse.columns:
        return df_classe
    sub_ids = (
        df_subclasse.select(pl.col("ID_Registro_Classe").cast(pl.Int64, strict=False))
        .drop_nulls()
        .unique()
    )
    before = len(df_classe)
    out = df_classe.with_columns(
        pl.col("ID_Registro_Classe").cast(pl.Int64, strict=False)
    ).join(sub_ids, on="ID_Registro_Classe", how="anti")
    log.info(
        "silver.class_funds.subclass_filter",
        before=before,
        excluded=before - len(out),
        after=len(out),
    )
    return out




def _read_anbima(settings: Settings) -> pl.DataFrame:
    """Read ANBIMA FUNDOS-175 xlsx; returns frame keyed by (cnpj_fundo, cnpj_classe).

    Returns an empty frame with the expected schema if the bronze partition is
    missing or the xlsx cannot be parsed — the caller continues and the quality
    report flags the high null rate on ANBIMA-derived columns.
    """
    expected_schema: dict[str, pl.DataType] = {
        "cnpj_fundo": pl.Utf8,
        "cnpj_classe": pl.Utf8,
        "classificacao_anbima": pl.Utf8,
        "composicao_fundos": pl.Utf8,
        "tributacao_alvo": pl.Utf8,
        "aplicacao_minima": pl.Utf8,
        "prazo_de_resgate": pl.Int64,
    }

    part = latest_partition_dir(settings.bronze_root, "anbima_175")
    if part is None:
        log.warning("silver.class_funds.anbima_missing")
        return pl.DataFrame(schema=expected_schema)

    xlsx_path = part / "raw.xlsx"
    if not xlsx_path.exists():
        log.warning("silver.class_funds.anbima_xlsx_missing", path=str(xlsx_path))
        return pl.DataFrame(schema=expected_schema)

    try:
        df = pl.read_excel(xlsx_path, engine="calamine")
    except Exception as e:
        log.error("silver.class_funds.anbima_read_failed", error=str(e))
        return pl.DataFrame(schema=expected_schema)

    col_map = {c: c for c in df.columns}
    # Flexible column resolution: tolerate small variations in header naming.
    def _find(*candidates: str) -> str | None:
        for cand in candidates:
            for c in df.columns:
                if c.strip().lower() == cand.strip().lower():
                    return c
        return None

    cnpj_fundo_col = _find("CNPJ Fundo", "CNPJ_Fundo", "CNPJ do Fundo")
    cnpj_classe_col = _find("CNPJ Classe", "CNPJ_Classe", "CNPJ da Classe")
    estrutura_col = _find("Estrutura")
    tipo_col = _find("Tipo ANBIMA")
    composicao_col = _find("Composição do Fundo", "Composição dos Fundos", "Composicao do Fundo")
    trib_col = _find("Tributação Alvo", "Tributacao Alvo")
    aplic_col = _find("Aplicação Inicial Mínima", "Aplicacao Inicial Minima")
    prazo_col = _find(
        "Prazo Pagamento Resgate em dias",
        "Prazo de Pagamento Resgate em dias",
        "Prazo Pagamento Resgate (dias)",
    )

    if not cnpj_fundo_col or not cnpj_classe_col:
        log.error(
            "silver.class_funds.anbima_missing_join_keys",
            cols=df.columns,
            cnpj_fundo_col=cnpj_fundo_col,
            cnpj_classe_col=cnpj_classe_col,
        )
        return pl.DataFrame(schema=expected_schema)

    if estrutura_col:
        before = df.height
        df = df.filter(
            pl.col(estrutura_col).cast(pl.Utf8, strict=False).str.strip_chars().str.to_lowercase()
            == "classe"
        )
        log.info(
            "silver.class_funds.anbima_estrutura_filter",
            before=before,
            after=df.height,
            excluded=before - df.height,
        )
    else:
        log.warning("silver.class_funds.anbima_no_estrutura_col", cols=df.columns)

    out = df.select(
        cnpj_clean_expr(cnpj_fundo_col, "cnpj_fundo"),
        cnpj_clean_expr(cnpj_classe_col, "cnpj_classe"),
        (
            pl.col(tipo_col).cast(pl.Utf8, strict=False).alias("classificacao_anbima")
            if tipo_col
            else pl.lit(None, dtype=pl.Utf8).alias("classificacao_anbima")
        ),
        (
            pl.col(composicao_col).cast(pl.Utf8, strict=False).alias("composicao_fundos")
            if composicao_col
            else pl.lit(None, dtype=pl.Utf8).alias("composicao_fundos")
        ),
        (
            pl.col(trib_col).cast(pl.Utf8, strict=False).alias("tributacao_alvo")
            if trib_col
            else pl.lit(None, dtype=pl.Utf8).alias("tributacao_alvo")
        ),
        (
            pl.col(aplic_col).cast(pl.Utf8, strict=False).alias("aplicacao_minima")
            if aplic_col
            else pl.lit(None, dtype=pl.Utf8).alias("aplicacao_minima")
        ),
        (
            pl.col(prazo_col).cast(pl.Int64, strict=False).alias("prazo_de_resgate")
            if prazo_col
            else pl.lit(None, dtype=pl.Int64).alias("prazo_de_resgate")
        ),
    )
    # Some classes appear with multiple Estrutura=Classe rows in the ANBIMA
    # xlsx (versioning artifact). Keep the first to avoid join inflation.
    before = out.height
    out = out.unique(subset=["cnpj_fundo", "cnpj_classe"], keep="first", maintain_order=True)
    log.info(
        "silver.class_funds.anbima_loaded",
        rows=len(out),
        deduped=before - len(out),
    )
    return out


def _build_classe_dim(df_classe: pl.DataFrame, df_fundo: pl.DataFrame) -> pl.DataFrame:
    classe = df_classe.select(
        pl.col("ID_Registro_Fundo").cast(pl.Int64, strict=False),
        cnpj_clean_expr("CNPJ_Classe", "cnpj_classe"),
        pl.col("Denominacao_Social").cast(pl.Utf8, strict=False).str.strip_chars().alias(
            "denom_social_classe"
        ),
        pl.col("Situacao").cast(pl.Utf8, strict=False).alias("situacao"),
        pl.col("Data_Inicio_Situacao")
        .str.to_date(format="%Y-%m-%d", strict=False)
        .alias("data_de_inicio"),
        pl.col("Exclusivo").cast(pl.Utf8, strict=False).alias("exclusivo"),
        pl.col("Publico_Alvo").cast(pl.Utf8, strict=False).alias("publico_alvo"),
        pl.col("Forma_Condominio").cast(pl.Utf8, strict=False).alias("condominio"),
    )
    fundo = (
        df_fundo.select(
            pl.col("ID_Registro_Fundo").cast(pl.Int64, strict=False),
            cnpj_clean_expr("CNPJ_Fundo", "cnpj_fundo"),
            pl.col("Denominacao_Social").cast(pl.Utf8, strict=False).str.strip_chars().alias(
                "denom_social_fundo"
            ),
        )
        # CVM registro_fundo.csv occasionally ships literal duplicate rows for
        # the same ID_Registro_Fundo. Dedupe to avoid inflating the join.
        .unique(subset=["ID_Registro_Fundo"], keep="first", maintain_order=True)
    )
    return classe.join(fundo, on="ID_Registro_Fundo", how="left").drop("ID_Registro_Fundo")


def _write_quality_report(df: pl.DataFrame, as_of: date, settings: Settings) -> Path:
    rows = len(df)
    distinct_classe = df["cnpj_classe"].n_unique() if rows else 0
    dups = rows - distinct_classe

    lines: list[str] = []
    lines.append(f"# class_funds — quality report (as_of={as_of.isoformat()})\n")
    lines.append(f"- Rows: **{rows}**")
    lines.append(f"- Distinct cnpj_classe: **{distinct_classe}**")
    lines.append(f"- Duplicates by cnpj_classe: **{dups}**\n")
    lines.append("## Nulls by column\n")
    lines.append("| column | nulls | pct |")
    lines.append("|---|---|---|")
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            lines.append(f"| {col} | n/a | n/a |")
            continue
        nulls = int(df[col].null_count())
        pct = (nulls / rows * 100.0) if rows else 0.0
        lines.append(f"| {col} | {nulls} | {pct:.2f}% |")
    lines.append("")

    if dups > 0:
        dup_rows = (
            df.group_by("cnpj_classe")
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") > 1)
            .sort("n", descending=True)
            .head(20)
        )
        lines.append("## Duplicate cnpj_classe (top 20)\n")
        lines.append("| cnpj_classe | n |")
        lines.append("|---|---|")
        for r in dup_rows.iter_rows(named=True):
            lines.append(f"| {r['cnpj_classe']} | {r['n']} |")
        lines.append("")

    out = settings.pipeline.reports_root / f"as_of={as_of.isoformat()}" / "class_funds_quality.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    log.info(
        "silver.class_funds.quality_report",
        path=str(out),
        rows=rows,
        duplicates=dups,
    )
    return out


def run(settings: Settings, as_of: date) -> Path:
    df_classe, df_fundo, df_subclasse = _read_registro_classe_zip(settings)

    df_classe = _apply_subclass_filter(df_classe, df_subclasse)

    base = _build_classe_dim(df_classe, df_fundo)

    anbima = _read_anbima(settings)

    taxa_adm = read_cad_fi_hist_latest(
        settings,
        member_name="cad_fi_hist_taxa_adm.csv",
        value_col="TAXA_ADM",
        date_col="DT_INI_TAXA_ADM",
        output_alias="taxa_adm",
        divide_by_100=True,
    )
    taxa_perform = read_cad_fi_hist_latest(
        settings,
        member_name="cad_fi_hist_taxa_perfm.csv",
        value_col="VL_TAXA_PERFM",
        date_col="DT_INI_TAXA_PERFM",
        output_alias="taxa_perform",
        divide_by_100=True,
    )
    rentab = read_cad_fi_hist_latest(
        settings,
        member_name="cad_fi_hist_rentab.csv",
        value_col="RENTAB_FUNDO",
        date_col="DT_INI_RENTAB",
        output_alias="benchmark",
        cast_str=True,
    )

    out_df = (
        base.join(anbima, on=["cnpj_fundo", "cnpj_classe"], how="left")
        .join(taxa_adm, on="cnpj_fundo", how="left")
        .join(taxa_perform, on="cnpj_fundo", how="left")
        .join(rentab, on="cnpj_fundo", how="left")
    )

    # Ensure final column order and presence (even when joins return empty frames)
    for col in OUTPUT_COLUMNS:
        if col not in out_df.columns:
            out_df = out_df.with_columns(pl.lit(None).alias(col))
    out_df = out_df.select(OUTPUT_COLUMNS)

    # Final defensive dedup: 1 row per cnpj_classe, no exceptions.
    before = out_df.height
    out_df = out_df.unique(subset=["cnpj_classe"], keep="first", maintain_order=True)
    if before != out_df.height:
        log.info(
            "silver.class_funds.final_dedup",
            before=before,
            after=out_df.height,
            removed=before - out_df.height,
        )

    out_path = silver_path(settings, "class_funds", as_of.isoformat())
    write_parquet(out_df, out_path)
    log.info(
        "silver.class_funds.written",
        path=str(out_path),
        rows=len(out_df),
        cols=len(out_df.columns),
    )

    _write_quality_report(out_df, as_of, settings)
    return out_path
