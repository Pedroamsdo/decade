"""silver/class_funds_fixed_income — RF subset of class_funds.

Filters class_funds to rows whose `classificacao_anbima` starts with
"Renda Fixa". Excludes Previdência RF (different ANBIMA category).
Writes a quality report mirroring class_funds (nulls + duplicates).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._benchmark_mapping import apply_benchmark_mapping
from fund_rank.silver._io import silver_path, write_parquet
from fund_rank.silver._taxa_imputation import apply_taxa_imputation, compute_taxa_stats

log = get_logger(__name__)

RF_PREFIX = "Renda Fixa"

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


def _write_quality_report(df: pl.DataFrame, as_of: date, settings: Settings) -> Path:
    rows = df.height
    distinct = df["cnpj_classe"].n_unique() if rows else 0
    dups = rows - distinct

    lines: list[str] = []
    lines.append(
        f"# class_funds_fixed_income — quality report (as_of={as_of.isoformat()})\n"
    )
    lines.append(f"- Rows: **{rows:,}**")
    lines.append(f"- Distinct cnpj_classe: **{distinct:,}**")
    lines.append(f"- Duplicates by cnpj_classe: **{dups:,}**\n")
    lines.append("## Nulls by column\n")
    lines.append("| column | nulls | pct |")
    lines.append("|---|---|---|")
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            lines.append(f"| {col} | n/a | n/a |")
            continue
        nulls = int(df[col].null_count())
        pct = (nulls / rows * 100.0) if rows else 0.0
        lines.append(f"| {col} | {nulls:,} | {pct:.2f}% |")
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

    out = (
        settings.pipeline.reports_root
        / f"as_of={as_of.isoformat()}"
        / "class_funds_fixed_income_quality.md"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    log.info(
        "silver.class_funds_fixed_income.quality_report",
        path=str(out),
        rows=rows,
        duplicates=dups,
    )
    return out


def run(settings: Settings, as_of: date) -> Path:
    in_path = silver_path(settings, "class_funds", as_of.isoformat())
    if not in_path.exists():
        raise FileNotFoundError(
            f"silver/class_funds not found at {in_path}; run build_class_funds first."
        )

    df = pl.read_parquet(in_path)
    before = df.height
    df_rf = df.filter(
        pl.col("classificacao_anbima")
        .cast(pl.Utf8, strict=False)
        .str.starts_with(RF_PREFIX)
    )
    log.info(
        "silver.class_funds_fixed_income.filtered",
        before=before,
        after=df_rf.height,
        excluded=before - df_rf.height,
    )

    df_rf = apply_benchmark_mapping(df_rf)

    # Impute taxa_adm and taxa_perform with mode (also replaces |z|>3 outliers).
    # Stats computed from this same RF-filtered class table (its non-null subset).
    stats_adm = compute_taxa_stats(df_rf, "taxa_adm")
    stats_perf = compute_taxa_stats(df_rf, "taxa_perform")
    df_rf = apply_taxa_imputation(df_rf, "taxa_adm", stats_adm)
    df_rf = apply_taxa_imputation(df_rf, "taxa_perform", stats_perf)
    log.info(
        "silver.class_funds_fixed_income.imputed",
        taxa_adm_mode=stats_adm.mode,
        taxa_adm_bounds=(stats_adm.lo, stats_adm.hi),
        taxa_perform_mode=stats_perf.mode,
        taxa_perform_bounds=(stats_perf.lo, stats_perf.hi),
    )

    out_path = silver_path(settings, "class_funds_fixed_income", as_of.isoformat())
    write_parquet(df_rf, out_path)
    log.info(
        "silver.class_funds_fixed_income.written",
        path=str(out_path),
        rows=df_rf.height,
    )

    _write_quality_report(df_rf, as_of, settings)
    return out_path
