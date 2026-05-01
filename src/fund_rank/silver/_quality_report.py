"""Consolidated data-quality report.

Renders a single Markdown file at ``reports_root/as_of=<date>/data_quality.md``
covering every silver and gold table produced by the pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import silver_path
from fund_rank.gold._io import gold_path

log = get_logger(__name__)


@dataclass
class _Table:
    name: str
    parquet_path: Path
    distinct_keys: list[str]
    null_columns: list[str] = field(default_factory=list)


def _distinct_count(df: pl.DataFrame, keys: list[str]) -> int:
    if df.height == 0:
        return 0
    if len(keys) == 1:
        return df[keys[0]].n_unique()
    return df.select(keys).unique().height


def _null_section(df: pl.DataFrame, columns: list[str], rows: int) -> list[str]:
    out = ["| column | nulls | pct |", "|---|---|---|"]
    for col in columns:
        if col not in df.columns:
            out.append(f"| {col} | n/a | n/a |")
            continue
        nulls = int(df[col].null_count())
        pct = (nulls / rows * 100.0) if rows else 0.0
        out.append(f"| {col} | {nulls:,} | {pct:.2f}% |")
    return out


def _dup_examples(df: pl.DataFrame, key: str, top: int = 10) -> list[str]:
    dup_rows = (
        df.group_by(key)
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)
        .sort("n", descending=True)
        .head(top)
    )
    if dup_rows.height == 0:
        return []
    out = [f"Top {top} duplicates by `{key}`:", "", f"| {key} | n |", "|---|---|"]
    for r in dup_rows.iter_rows(named=True):
        out.append(f"| {r[key]} | {r['n']} |")
    return out


def _render_table(t: _Table) -> list[str]:
    lines = [f"## {t.name}", ""]
    if not t.parquet_path.exists():
        lines.append(f"_missing parquet: `{t.parquet_path}`_")
        lines.append("")
        return lines

    df = pl.read_parquet(t.parquet_path)
    rows = df.height
    distinct = _distinct_count(df, t.distinct_keys)
    keys_lbl = ", ".join(t.distinct_keys)
    lines.append(f"- Rows: **{rows:,}**")
    lines.append(f"- Distinct ({keys_lbl}): **{distinct:,}**")
    lines.append(f"- Duplicates: **{rows - distinct:,}**")
    lines.append("")

    if t.null_columns:
        lines.append("### Nulls by column")
        lines.append("")
        lines.extend(_null_section(df, t.null_columns, rows))
        lines.append("")

    if len(t.distinct_keys) == 1 and (rows - distinct) > 0:
        lines.extend(_dup_examples(df, t.distinct_keys[0]))
        lines.append("")

    return lines


def _render_index_series(parquet_path: Path) -> list[str]:
    lines = ["## index_series", ""]
    if not parquet_path.exists():
        lines.append(f"_missing parquet: `{parquet_path}`_")
        lines.append("")
        return lines
    df = pl.read_parquet(parquet_path)
    lines.append(f"- Rows: **{df.height:,}**")
    if df.height:
        lines.append(f"- Date range: **{df['data'].min()}** → **{df['data'].max()}**")
    lines.append("")
    lines.append("### Coverage by index")
    lines.append("")
    lines.append("| index | non-null | first | last |")
    lines.append("|---|---|---|---|")
    for col in [c for c in df.columns if c != "data"]:
        nn = df.filter(pl.col(col).is_not_null())
        if nn.height:
            lines.append(
                f"| {col} | {nn.height:,} | {nn['data'].min()} | {nn['data'].max()} |"
            )
        else:
            lines.append(f"| {col} | 0 | n/a | n/a |")
    lines.append("")
    return lines


def _render_score_buckets(parquet_path: Path) -> list[str]:
    lines: list[str] = []
    if not parquet_path.exists():
        return lines
    df = pl.read_parquet(parquet_path)
    if "score" not in df.columns:
        return lines
    s = df["score"].drop_nulls()
    if s.len() == 0:
        return lines
    lines.append("### Score distribution (eligible universe)")
    lines.append("")
    lines.append(f"- Eligible: **{s.len():,}** funds")
    lines.append(
        f"- min/median/mean/max: **{s.min():.2f}** / **{s.median():.2f}** / "
        f"**{s.mean():.2f}** / **{s.max():.2f}**"
    )
    lines.append("")
    lines.append("| bucket | n | pct |")
    lines.append("|---|---|---|")
    for lo, hi in [(0, 20), (20, 40), (40, 60), (60, 80), (80, 100.01)]:
        n = int(s.filter((s >= lo) & (s < hi)).len())
        pct = n / s.len() * 100.0
        hi_str = "100" if hi > 100 else f"{hi:g}"
        lines.append(f"| {lo:g}-{hi_str} | {n:,} | {pct:.2f}% |")
    lines.append("")
    return lines


_CLASS_NULL_COLS = [
    "cnpj_fundo", "cnpj_classe", "denom_social_fundo", "denom_social_classe",
    "situacao", "data_de_inicio", "exclusivo", "publico_alvo", "condominio",
    "classificacao_anbima", "composicao_fundos", "tributacao_alvo",
    "aplicacao_minima", "prazo_de_resgate", "taxa_adm", "taxa_perform", "benchmark",
]
_SUBCLASS_NULL_COLS = [
    "cnpj_fundo", "cnpj_classe", "id_subclasse_cvm", "denom_social_subclasse",
    "situacao", "data_de_inicio", "exclusivo", "publico_alvo", "condominio",
    "classificacao_anbima", "composicao_fundos", "tributacao_alvo",
    "aplicacao_minima", "prazo_de_resgate", "taxa_adm", "taxa_perform", "benchmark",
]
_QUOTA_NULL_COLS = [
    "tp_fundo_classe", "cnpj_fundo_classe", "id_subclasse", "dt_comptc",
    "vl_total", "vl_quota", "vl_patrim_liq", "captc_dia", "resg_dia", "nr_cotst",
]
_FUND_METRICS_NULL_COLS = [
    "cnpj_classe", "id_subclasse_cvm", "benchmark",
    "information_ratio", "equity", "nr_cotst", "existing_time", "score",
]


def write_consolidated_quality_report(as_of: date, settings: Settings) -> Path:
    as_of_s = as_of.isoformat()

    tables: list[_Table] = [
        _Table("class_funds", silver_path(settings, "class_funds", as_of_s),
               ["cnpj_classe"], _CLASS_NULL_COLS),
        _Table("subclass_funds", silver_path(settings, "subclass_funds", as_of_s),
               ["id_subclasse_cvm"], _SUBCLASS_NULL_COLS),
        _Table("class_funds_fixed_income",
               silver_path(settings, "class_funds_fixed_income", as_of_s),
               ["cnpj_classe"], _CLASS_NULL_COLS),
        _Table("subclass_funds_fixed_income",
               silver_path(settings, "subclass_funds_fixed_income", as_of_s),
               ["id_subclasse_cvm"], _SUBCLASS_NULL_COLS),
        _Table("class_funds_fixed_income_treated",
               silver_path(settings, "class_funds_fixed_income_treated", as_of_s),
               ["cnpj_classe"], _CLASS_NULL_COLS),
        _Table("subclass_funds_fixed_income_treated",
               silver_path(settings, "subclass_funds_fixed_income_treated", as_of_s),
               ["id_subclasse_cvm"], _SUBCLASS_NULL_COLS),
        _Table("quota_series_fixed_income",
               silver_path(settings, "quota_series_fixed_income", as_of_s),
               ["cnpj_fundo_classe", "id_subclasse", "dt_comptc"], _QUOTA_NULL_COLS),
        _Table("gold/validacao", gold_path(settings, "validacao", as_of_s),
               ["cnpj_classe", "id_subclasse_cvm"], ["retorno_2025"]),
    ]

    lines: list[str] = [
        f"# Data quality report — as_of={as_of_s}",
        "",
    ]

    for t in tables:
        lines.extend(_render_table(t))

    lines.extend(_render_index_series(silver_path(settings, "index_series", as_of_s)))

    fm_path = gold_path(settings, "fund_metrics", as_of_s)
    lines.extend(_render_table(_Table(
        "gold/fund_metrics", fm_path,
        ["cnpj_classe", "id_subclasse_cvm"], _FUND_METRICS_NULL_COLS,
    )))
    lines.extend(_render_score_buckets(fm_path))

    out = settings.pipeline.reports_root / f"as_of={as_of_s}" / "data_quality.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    log.info("quality.consolidated.written", path=str(out))
    return out
