"""silver/quota_series_fixed_income — RF subset of quota_series + quality report.

Filters quota_series to rows whose underlying fund (class without subclasses
OR subclass) appears in the RF dimension tables. Writes:

  - silver/quota_series_fixed_income/as_of=YYYY-MM-DD/data.parquet
  - reports/as_of=YYYY-MM-DD/quota_series_fixed_income_quality.md

Filter semantics (id_subclasse is the discriminator):
  - id_subclasse IS NULL   → cnpj_fundo_classe must be in class_funds_fixed_income.cnpj_classe
  - id_subclasse NOT NULL  → id_subclasse must be in subclass_funds_fixed_income.id_subclasse_cvm
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import silver_path, write_parquet

log = get_logger(__name__)


QUOTA_COLUMNS: list[str] = [
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


def _write_quality_report(df: pl.DataFrame, as_of: date, settings: Settings) -> Path:
    rows = df.height
    distinct = (
        df.select("cnpj_fundo_classe", "id_subclasse", "dt_comptc").unique().height
        if rows
        else 0
    )
    dups = rows - distinct

    distinct_cnpj = df["cnpj_fundo_classe"].n_unique() if rows else 0
    distinct_subclasse = (
        df.filter(pl.col("id_subclasse").is_not_null())["id_subclasse"].n_unique() if rows else 0
    )
    dt_min = df["dt_comptc"].min() if rows else None
    dt_max = df["dt_comptc"].max() if rows else None

    classe_rows = df.filter(pl.col("id_subclasse").is_null()).height
    sub_rows = df.filter(pl.col("id_subclasse").is_not_null()).height

    lines: list[str] = []
    lines.append(f"# quota_series_fixed_income — quality report (as_of={as_of.isoformat()})\n")
    lines.append(f"- Rows: **{rows:,}**")
    lines.append(
        f"- Distinct (cnpj_fundo_classe, id_subclasse, dt_comptc): **{distinct:,}**"
    )
    lines.append(f"- Duplicates by composite key: **{dups:,}**\n")
    lines.append("## Coverage\n")
    lines.append(f"- Distinct cnpj_fundo_classe: **{distinct_cnpj:,}**")
    lines.append(f"- Distinct id_subclasse (non-null): **{distinct_subclasse:,}**")
    lines.append(f"- Classe-level rows (id_subclasse null): **{classe_rows:,}**")
    lines.append(f"- Subclasse-level rows (id_subclasse filled): **{sub_rows:,}**")
    lines.append(f"- dt_comptc range: **{dt_min}** → **{dt_max}**\n")

    lines.append("## Nulls by column\n")
    lines.append("| column | nulls | pct |")
    lines.append("|---|---|---|")
    for col in QUOTA_COLUMNS:
        n = int(df[col].null_count()) if col in df.columns else 0
        pct = (n / rows * 100.0) if rows else 0.0
        lines.append(f"| {col} | {n:,} | {pct:.2f}% |")
    lines.append("")

    out = (
        settings.pipeline.reports_root
        / f"as_of={as_of.isoformat()}"
        / "quota_series_fixed_income_quality.md"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    log.info(
        "silver.quota_series_fixed_income.quality_report",
        path=str(out),
        rows=rows,
        duplicates=dups,
    )
    return out


def run(settings: Settings, as_of: date) -> Path:
    qs_path = silver_path(settings, "quota_series", as_of.isoformat())
    cf_rf_path = silver_path(settings, "class_funds_fixed_income", as_of.isoformat())
    sf_rf_path = silver_path(settings, "subclass_funds_fixed_income", as_of.isoformat())

    for p, name in [(qs_path, "quota_series"), (cf_rf_path, "class_funds_fixed_income"), (sf_rf_path, "subclass_funds_fixed_income")]:
        if not p.exists():
            raise FileNotFoundError(
                f"silver/{name} not found at {p}; run the upstream build steps first."
            )

    qs = pl.read_parquet(qs_path)

    cf_rf_keys = (
        pl.read_parquet(cf_rf_path)
        .select(pl.col("cnpj_classe").alias("cnpj_fundo_classe"))
        .unique()
    )
    sf_rf_keys = (
        pl.read_parquet(sf_rf_path)
        .select(pl.col("id_subclasse_cvm").alias("id_subclasse"))
        .unique()
    )

    classe_rows = qs.filter(pl.col("id_subclasse").is_null()).join(
        cf_rf_keys, on="cnpj_fundo_classe", how="inner"
    )
    sub_rows = qs.filter(pl.col("id_subclasse").is_not_null()).join(
        sf_rf_keys, on="id_subclasse", how="inner"
    )
    rf_qs = pl.concat([classe_rows, sub_rows], how="vertical_relaxed").select(QUOTA_COLUMNS)

    log.info(
        "silver.quota_series_fixed_income.filtered",
        total_input=qs.height,
        classe_kept=classe_rows.height,
        sub_kept=sub_rows.height,
        total_output=rf_qs.height,
    )

    out_path = silver_path(settings, "quota_series_fixed_income", as_of.isoformat())
    write_parquet(rf_qs, out_path)
    log.info(
        "silver.quota_series_fixed_income.written",
        path=str(out_path),
        rows=rf_qs.height,
    )

    _write_quality_report(rf_qs, as_of, settings)
    return out_path
