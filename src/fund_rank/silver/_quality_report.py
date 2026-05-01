"""Shared quality-report helper for silver/gold tables.

Replaces ~6 byte-identical `_write_quality_report` functions across silver and
gold builders. Writes Markdown to `reports_root/as_of=<date>/<table>_quality.md`.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings

log = get_logger(__name__)


def _distinct_count(df: pl.DataFrame, keys: list[str]) -> int:
    if df.height == 0:
        return 0
    if len(keys) == 1:
        return df[keys[0]].n_unique()
    return df.select(keys).unique().height


def _null_section(df: pl.DataFrame, columns: list[str], rows: int) -> list[str]:
    out = ["## Nulls by column\n", "| column | nulls | pct |", "|---|---|---|"]
    for col in columns:
        if col not in df.columns:
            out.append(f"| {col} | n/a | n/a |")
            continue
        nulls = int(df[col].null_count())
        pct = (nulls / rows * 100.0) if rows else 0.0
        out.append(f"| {col} | {nulls:,} | {pct:.2f}% |")
    out.append("")
    return out


def _dup_examples(df: pl.DataFrame, key: str, top: int = 20) -> list[str]:
    dup_rows = (
        df.group_by(key)
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)
        .sort("n", descending=True)
        .head(top)
    )
    if dup_rows.height == 0:
        return []
    out = [f"## Duplicate {key} (top {top})\n", f"| {key} | n |", "|---|---|"]
    for r in dup_rows.iter_rows(named=True):
        out.append(f"| {r[key]} | {r['n']} |")
    out.append("")
    return out


def write_quality_report(
    df: pl.DataFrame,
    as_of: date,
    settings: Settings,
    *,
    table_name: str,
    distinct_keys: list[str],
    null_columns: list[str],
    extra_sections: list[str] | None = None,
    log_namespace: str | None = None,
) -> Path:
    """Write a Markdown quality report. Returns the output path.

    Args:
        table_name: used for filename + Markdown header (e.g. "class_funds_fixed_income").
        distinct_keys: columns forming the uniqueness key. If a single column, a
            top-20 list of duplicates is appended.
        null_columns: columns to include in the "Nulls by column" table.
        extra_sections: optional Markdown lines inserted between the header
            block and the nulls table (e.g. coverage stats).
        log_namespace: structured-log event name. Defaults to
            f"silver.{table_name}.quality_report".
    """
    rows = df.height
    distinct = _distinct_count(df, distinct_keys)
    dups = rows - distinct
    keys_repr = ", ".join(distinct_keys) if len(distinct_keys) > 1 else distinct_keys[0]
    keys_label = f"({keys_repr})" if len(distinct_keys) > 1 else keys_repr

    lines: list[str] = []
    lines.append(f"# {table_name} — quality report (as_of={as_of.isoformat()})\n")
    lines.append(f"- Rows: **{rows:,}**")
    lines.append(f"- Distinct {keys_label}: **{distinct:,}**")
    lines.append(f"- Duplicates by {keys_label}: **{dups:,}**\n")

    if extra_sections:
        lines.extend(extra_sections)

    lines.extend(_null_section(df, null_columns, rows))

    if len(distinct_keys) == 1 and dups > 0:
        lines.extend(_dup_examples(df, distinct_keys[0]))

    out = (
        settings.pipeline.reports_root
        / f"as_of={as_of.isoformat()}"
        / f"{table_name}_quality.md"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    log.info(
        log_namespace or f"silver.{table_name}.quality_report",
        path=str(out),
        rows=rows,
        duplicates=dups,
    )
    return out
