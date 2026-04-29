"""silver/subclass_funds — typed dimension at SUBCLASSE level (1 row per ID_Subclasse).

Population is the complement of class_funds: every row of registro_subclasse.csv.

Joins:
  - registro_subclasse → registro_classe (ID_Registro_Classe)
  - registro_classe → registro_fundo (ID_Registro_Fundo)
  - ANBIMA FUNDOS-175 xlsx (Estrutura == "Subclasse"):
      pass 1: ID_Subclasse <-> Código CVM Subclasse (precise, ~33% of ANBIMA rows)
      pass 2: (cnpj_fundo, cnpj_classe) fallback (rest, with first-row-wins dedup)
  - cad_fi_hist_taxa_adm / taxa_perfm / rentab (most-recent per CNPJ_Fundo)

Output (17 cols): cnpj_fundo, cnpj_classe, id_subclasse_cvm, denom_social_subclasse,
situacao, data_de_inicio, exclusivo, publico_alvo, condominio, classificacao_anbima,
composicao_fundos, tributacao_alvo, aplicacao_minima, prazo_de_resgate, taxa_adm,
taxa_perform, benchmark.

Side effect: writes a markdown quality report at
reports/as_of=YYYY-MM-DD/subclass_funds_quality.md including a breakdown of how
many subclasses matched each ANBIMA pass.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.bronze.manifest import latest_partition_dir
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import (
    cnpj_clean_expr,
    read_csv_from_zip,
    silver_path,
    write_parquet,
)
from fund_rank.silver.build_class_funds import _read_cad_fi_hist_latest

log = get_logger(__name__)


OUTPUT_COLUMNS: list[str] = [
    "cnpj_fundo",
    "cnpj_classe",
    "id_subclasse_cvm",
    "denom_social_subclasse",
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

ANBIMA_COLS: list[str] = [
    "classificacao_anbima",
    "composicao_fundos",
    "tributacao_alvo",
    "aplicacao_minima",
    "prazo_de_resgate",
]


@dataclass
class _AnbimaJoinBreakdown:
    pass1_matched: int
    pass2_matched: int
    unmatched: int
    ambiguity_dropped: int


def _read_registro_files(
    settings: Settings,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    part = latest_partition_dir(settings.bronze_root, "cvm_registro_classe")
    if part is None:
        raise FileNotFoundError(
            "No bronze partition for cvm_registro_classe; run `ingest` first."
        )
    zip_path = part / "raw.zip"
    df_subclasse = read_csv_from_zip(zip_path, "registro_subclasse.csv")
    df_classe = read_csv_from_zip(zip_path, "registro_classe.csv")
    df_fundo = read_csv_from_zip(zip_path, "registro_fundo.csv")
    log.info(
        "silver.subclass_funds.read_registro",
        subclasses=len(df_subclasse),
        classes=len(df_classe),
        fundos=len(df_fundo),
    )
    return df_subclasse, df_classe, df_fundo


def _build_base(
    df_subclasse: pl.DataFrame,
    df_classe: pl.DataFrame,
    df_fundo: pl.DataFrame,
) -> pl.DataFrame:
    """Chain registro_subclasse → registro_classe → registro_fundo and produce
    the 9-column base (CVM-only) for subclass_funds.
    """
    sub = df_subclasse.select(
        pl.col("ID_Registro_Classe").cast(pl.Int64, strict=False),
        pl.col("ID_Subclasse").cast(pl.Utf8, strict=False).alias("id_subclasse_cvm"),
        pl.col("Denominacao_Social")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .alias("denom_social_subclasse"),
        pl.col("Situacao").cast(pl.Utf8, strict=False).alias("situacao"),
        pl.col("Data_Inicio_Situacao")
        .str.to_date(format="%Y-%m-%d", strict=False)
        .alias("data_de_inicio"),
        pl.col("Exclusivo").cast(pl.Utf8, strict=False).alias("exclusivo"),
        pl.col("Publico_Alvo").cast(pl.Utf8, strict=False).alias("publico_alvo"),
        pl.col("Forma_Condominio").cast(pl.Utf8, strict=False).alias("condominio"),
    )

    classe = df_classe.select(
        pl.col("ID_Registro_Classe").cast(pl.Int64, strict=False),
        pl.col("ID_Registro_Fundo").cast(pl.Int64, strict=False),
        cnpj_clean_expr("CNPJ_Classe", "cnpj_classe"),
    ).unique(subset=["ID_Registro_Classe"], keep="first", maintain_order=True)

    fundo = (
        df_fundo.select(
            pl.col("ID_Registro_Fundo").cast(pl.Int64, strict=False),
            cnpj_clean_expr("CNPJ_Fundo", "cnpj_fundo"),
        )
        # CVM registro_fundo.csv occasionally ships duplicate rows for the same
        # ID_Registro_Fundo. Dedupe to avoid join inflation.
        .unique(subset=["ID_Registro_Fundo"], keep="first", maintain_order=True)
    )

    base = (
        sub.join(classe, on="ID_Registro_Classe", how="left")
        .join(fundo, on="ID_Registro_Fundo", how="left")
        .drop("ID_Registro_Classe", "ID_Registro_Fundo")
    )
    return base


def _read_anbima_subclasse(settings: Settings) -> pl.DataFrame:
    """Read ANBIMA xlsx, filter Estrutura == 'Subclasse', return a frame with
    ``codigo_cvm_subclasse`` + ``cnpj_fundo`` + ``cnpj_classe`` + 5 ANBIMA cols.

    Returns an empty frame with the expected schema if the bronze partition is
    missing — caller continues; quality report reflects high null rate.
    """
    expected_schema: dict[str, pl.DataType] = {
        "codigo_cvm_subclasse": pl.Utf8,
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
        log.warning("silver.subclass_funds.anbima_missing")
        return pl.DataFrame(schema=expected_schema)

    xlsx_path = part / "raw.xlsx"
    if not xlsx_path.exists():
        log.warning("silver.subclass_funds.anbima_xlsx_missing", path=str(xlsx_path))
        return pl.DataFrame(schema=expected_schema)

    try:
        df = pl.read_excel(xlsx_path, engine="calamine")
    except Exception as e:
        log.error("silver.subclass_funds.anbima_read_failed", error=str(e))
        return pl.DataFrame(schema=expected_schema)

    def _find(*candidates: str) -> str | None:
        for cand in candidates:
            for c in df.columns:
                if c.strip().lower() == cand.strip().lower():
                    return c
        return None

    cnpj_fundo_col = _find("CNPJ Fundo", "CNPJ_Fundo", "CNPJ do Fundo")
    cnpj_classe_col = _find("CNPJ Classe", "CNPJ_Classe", "CNPJ da Classe")
    estrutura_col = _find("Estrutura")
    codigo_sub_col = _find("Código CVM Subclasse", "Codigo CVM Subclasse")
    tipo_col = _find("Tipo ANBIMA")
    composicao_col = _find("Composição do Fundo", "Composição dos Fundos", "Composicao do Fundo")
    trib_col = _find("Tributação Alvo", "Tributacao Alvo")
    aplic_col = _find("Aplicação Inicial Mínima", "Aplicacao Inicial Minima")
    prazo_col = _find(
        "Prazo Pagamento Resgate em dias",
        "Prazo de Pagamento Resgate em dias",
        "Prazo Pagamento Resgate (dias)",
    )

    if not cnpj_fundo_col or not cnpj_classe_col or not codigo_sub_col:
        log.error(
            "silver.subclass_funds.anbima_missing_keys",
            cols=df.columns,
            cnpj_fundo_col=cnpj_fundo_col,
            cnpj_classe_col=cnpj_classe_col,
            codigo_sub_col=codigo_sub_col,
        )
        return pl.DataFrame(schema=expected_schema)

    if estrutura_col:
        before = df.height
        df = df.filter(
            pl.col(estrutura_col).cast(pl.Utf8, strict=False).str.strip_chars().str.to_lowercase()
            == "subclasse"
        )
        log.info(
            "silver.subclass_funds.anbima_estrutura_filter",
            before=before,
            after=df.height,
        )
    else:
        log.warning("silver.subclass_funds.anbima_no_estrutura_col", cols=df.columns)

    out = df.select(
        pl.col(codigo_sub_col).cast(pl.Utf8, strict=False).str.strip_chars().alias(
            "codigo_cvm_subclasse"
        ),
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
    log.info("silver.subclass_funds.anbima_loaded", rows=len(out))
    return out


def _two_pass_anbima_join(
    base: pl.DataFrame,
    anbima_sub: pl.DataFrame,
) -> tuple[pl.DataFrame, _AnbimaJoinBreakdown]:
    """Apply two-pass ANBIMA join: precise by codigo_cvm_subclasse first,
    then fallback by (cnpj_fundo, cnpj_classe). Coalesce per ANBIMA column.
    """
    a_id = anbima_sub.filter(pl.col("codigo_cvm_subclasse").is_not_null())
    a_cnpj_raw = anbima_sub.filter(pl.col("codigo_cvm_subclasse").is_null())

    a_id_dedup = a_id.unique(subset=["codigo_cvm_subclasse"], keep="first", maintain_order=True)
    a_cnpj_dedup = a_cnpj_raw.unique(
        subset=["cnpj_fundo", "cnpj_classe"], keep="first", maintain_order=True
    )
    ambiguity_dropped = a_cnpj_raw.height - a_cnpj_dedup.height

    # Pass 1 — precise match. Bring ANBIMA columns aliased as _p1_*.
    p1 = (
        a_id_dedup.select(
            pl.col("codigo_cvm_subclasse"),
            *[pl.col(c).alias(f"_p1_{c}") for c in ANBIMA_COLS],
        )
    )
    m1 = base.join(
        p1, left_on="id_subclasse_cvm", right_on="codigo_cvm_subclasse", how="left"
    )

    # Pass 2 — fallback by (cnpj_fundo, cnpj_classe). Bring as _p2_*.
    p2 = a_cnpj_dedup.select(
        "cnpj_fundo",
        "cnpj_classe",
        *[pl.col(c).alias(f"_p2_{c}") for c in ANBIMA_COLS],
    )
    m2 = m1.join(p2, on=["cnpj_fundo", "cnpj_classe"], how="left")

    # Breakdown counts.
    pass1_matched = m2.filter(pl.col("_p1_classificacao_anbima").is_not_null()).height
    pass2_matched = m2.filter(
        pl.col("_p1_classificacao_anbima").is_null()
        & pl.col("_p2_classificacao_anbima").is_not_null()
    ).height
    unmatched = m2.filter(
        pl.col("_p1_classificacao_anbima").is_null()
        & pl.col("_p2_classificacao_anbima").is_null()
    ).height

    # Coalesce — pass-1 wins.
    out = m2.with_columns(
        [
            pl.coalesce(pl.col(f"_p1_{c}"), pl.col(f"_p2_{c}")).alias(c)
            for c in ANBIMA_COLS
        ]
    ).drop([f"_p1_{c}" for c in ANBIMA_COLS] + [f"_p2_{c}" for c in ANBIMA_COLS])

    breakdown = _AnbimaJoinBreakdown(
        pass1_matched=pass1_matched,
        pass2_matched=pass2_matched,
        unmatched=unmatched,
        ambiguity_dropped=ambiguity_dropped,
    )
    log.info(
        "silver.subclass_funds.anbima_join",
        pass1=pass1_matched,
        pass2=pass2_matched,
        unmatched=unmatched,
        ambiguity_dropped=ambiguity_dropped,
    )
    return out, breakdown


def _write_quality_report(
    df: pl.DataFrame,
    as_of: date,
    settings: Settings,
    breakdown: _AnbimaJoinBreakdown,
) -> Path:
    rows = len(df)
    distinct = df["id_subclasse_cvm"].n_unique() if rows else 0
    dups = rows - distinct

    lines: list[str] = []
    lines.append(f"# subclass_funds — quality report (as_of={as_of.isoformat()})\n")
    lines.append(f"- Rows: **{rows}**")
    lines.append(f"- Distinct id_subclasse_cvm: **{distinct}**")
    lines.append(f"- Duplicates by id_subclasse_cvm: **{dups}**\n")

    lines.append("## ANBIMA join breakdown\n")
    lines.append(f"- Matched by Código CVM Subclasse (pass 1): **{breakdown.pass1_matched}**")
    lines.append(f"- Matched by (cnpj_fundo, cnpj_classe) fallback (pass 2): **{breakdown.pass2_matched}**")
    lines.append(f"- Unmatched (no ANBIMA data): **{breakdown.unmatched}**")
    lines.append(
        f"- Ambiguous fallbacks dropped (multiple ANBIMA candidates per "
        f"cnpj_fundo+cnpj_classe with Código null): **{breakdown.ambiguity_dropped}**\n"
    )

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
            df.group_by("id_subclasse_cvm")
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") > 1)
            .sort("n", descending=True)
            .head(20)
        )
        lines.append("## Duplicate id_subclasse_cvm (top 20)\n")
        lines.append("| id_subclasse_cvm | n |")
        lines.append("|---|---|")
        for r in dup_rows.iter_rows(named=True):
            lines.append(f"| {r['id_subclasse_cvm']} | {r['n']} |")
        lines.append("")

    out = settings.pipeline.reports_root / f"as_of={as_of.isoformat()}" / "subclass_funds_quality.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    log.info(
        "silver.subclass_funds.quality_report",
        path=str(out),
        rows=rows,
        duplicates=dups,
    )
    return out


def run(settings: Settings, as_of: date) -> Path:
    df_sub, df_classe, df_fundo = _read_registro_files(settings)

    base = _build_base(df_sub, df_classe, df_fundo)

    anbima_sub = _read_anbima_subclasse(settings)
    enriched, breakdown = _two_pass_anbima_join(base, anbima_sub)

    taxa_adm = _read_cad_fi_hist_latest(
        settings,
        member_name="cad_fi_hist_taxa_adm.csv",
        value_col="TAXA_ADM",
        date_col="DT_INI_TAXA_ADM",
        output_alias="taxa_adm",
        divide_by_100=True,
    )
    taxa_perform = _read_cad_fi_hist_latest(
        settings,
        member_name="cad_fi_hist_taxa_perfm.csv",
        value_col="VL_TAXA_PERFM",
        date_col="DT_INI_TAXA_PERFM",
        output_alias="taxa_perform",
        divide_by_100=True,
    )
    rentab = _read_cad_fi_hist_latest(
        settings,
        member_name="cad_fi_hist_rentab.csv",
        value_col="RENTAB_FUNDO",
        date_col="DT_INI_RENTAB",
        output_alias="benchmark",
        cast_str=True,
    )

    out_df = (
        enriched.join(taxa_adm, on="cnpj_fundo", how="left")
        .join(taxa_perform, on="cnpj_fundo", how="left")
        .join(rentab, on="cnpj_fundo", how="left")
    )

    for col in OUTPUT_COLUMNS:
        if col not in out_df.columns:
            out_df = out_df.with_columns(pl.lit(None).alias(col))
    out_df = out_df.select(OUTPUT_COLUMNS)

    # Final defensive dedup: 1 row per id_subclasse_cvm.
    before = out_df.height
    out_df = out_df.unique(subset=["id_subclasse_cvm"], keep="first", maintain_order=True)
    if before != out_df.height:
        log.info(
            "silver.subclass_funds.final_dedup",
            before=before,
            after=out_df.height,
            removed=before - out_df.height,
        )

    out_path = silver_path(settings, "subclass_funds", as_of.isoformat())
    write_parquet(out_df, out_path)
    log.info(
        "silver.subclass_funds.written",
        path=str(out_path),
        rows=len(out_df),
        cols=len(out_df.columns),
    )

    _write_quality_report(out_df, as_of, settings, breakdown)
    return out_path
