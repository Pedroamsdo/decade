"""silver/subclass_funds_fixed_income_treated — treated RF subset of subclass_funds.

Reads `silver/subclass_funds_fixed_income` (filter-only RF subset) and applies:

  - Benchmark mapping (CVM raw strings → 10 canonical codes; nulls → "CDI").
  - Mode-based imputation of `taxa_adm` and `taxa_perform`. Stats are sourced
    from `silver/class_funds_fixed_income` (raw RF, pre-imputation) — the
    canonical reference per spec, so subclass and class share the same
    imputation distribution.
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


def _write_quality_report(df: pl.DataFrame, as_of: date, settings: Settings) -> Path:
    rows = df.height
    distinct = df["id_subclasse_cvm"].n_unique() if rows else 0
    dups = rows - distinct

    lines: list[str] = []
    lines.append(
        f"# subclass_funds_fixed_income_treated — quality report (as_of={as_of.isoformat()})\n"
    )
    lines.append(f"- Rows: **{rows:,}**")
    lines.append(f"- Distinct id_subclasse_cvm: **{distinct:,}**")
    lines.append(f"- Duplicates by id_subclasse_cvm: **{dups:,}**\n")
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

    out = (
        settings.pipeline.reports_root
        / f"as_of={as_of.isoformat()}"
        / "subclass_funds_fixed_income_treated_quality.md"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    log.info(
        "silver.subclass_funds_fixed_income_treated.quality_report",
        path=str(out),
        rows=rows,
        duplicates=dups,
    )
    return out


def run(settings: Settings, as_of: date) -> Path:
    in_path = silver_path(settings, "subclass_funds_fixed_income", as_of.isoformat())
    if not in_path.exists():
        raise FileNotFoundError(
            f"silver/subclass_funds_fixed_income not found at {in_path}; "
            "run build_subclass_funds_fixed_income first."
        )

    df = pl.read_parquet(in_path)

    df = apply_benchmark_mapping(df)

    class_path = silver_path(settings, "class_funds_fixed_income", as_of.isoformat())
    if not class_path.exists():
        raise FileNotFoundError(
            f"silver/class_funds_fixed_income not found at {class_path}; "
            "needed as taxa stats reference."
        )
    class_rf = pl.read_parquet(class_path)
    stats_adm = compute_taxa_stats(class_rf, "taxa_adm")
    stats_perf = compute_taxa_stats(class_rf, "taxa_perform")
    df = apply_taxa_imputation(df, "taxa_adm", stats_adm)
    df = apply_taxa_imputation(df, "taxa_perform", stats_perf)
    log.info(
        "silver.subclass_funds_fixed_income_treated.imputed",
        ref="class_funds_fixed_income",
        taxa_adm_mode=stats_adm.mode,
        taxa_adm_bounds=(stats_adm.lo, stats_adm.hi),
        taxa_perform_mode=stats_perf.mode,
        taxa_perform_bounds=(stats_perf.lo, stats_perf.hi),
    )

    out_path = silver_path(
        settings, "subclass_funds_fixed_income_treated", as_of.isoformat()
    )
    write_parquet(df, out_path)
    log.info(
        "silver.subclass_funds_fixed_income_treated.written",
        path=str(out_path),
        rows=df.height,
    )

    _write_quality_report(df, as_of, settings)
    return out_path
