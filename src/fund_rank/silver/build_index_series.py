"""silver/index_series — wide table de índices (CDI, SELIC, IPCA, INPC, IGP-M, IMAs, IRF-M).

Lê todas as partições bronze para cada índice e faz outer join por `data` produzindo:

  silver/index_series/as_of=YYYY-MM-DD/data.parquet

Schema: `data: Date` + 11 colunas Float64 em ordem fixa. Valores nulos onde a
série não observa (granularidades distintas: diária para CDI/SELIC/IMAs/IRF-M;
mensal para IPCA/INPC/IGP-M).

Fontes:
  - BCB SGS (CDI/SELIC/IPCA/INPC/IGP-M): ingerido via HTTP em chunks de 10 anos,
    raw.json `[{"data":"DD/MM/YYYY","valor":"X.XXXX"}, ...]`
  - ANBIMA (IMA-B/5/5+/Geral/S, IRF-M): drop manual de XLS no portal
    `data.anbima.com.br/indices`, ingerido como `anbima_indices/competence=<ima_*>/raw.xlsx`.
    Cada XLS tem colunas `Data de Referência` (Date) + `Número Índice` (Float64).

Também produz `reports/as_of=YYYY-MM-DD/index_series_quality.md` comparando a
cobertura de cada índice com o range de `silver/quota_series`.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from fund_rank.bronze.manifest import latest_partition_dir
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import all_partitions_for, silver_path, write_parquet

log = get_logger(__name__)


# Ordem do schema final (data + 11 índices).
# Para BCB: cada coluna mapeia 1:1 a uma source bronze.
# Para ANBIMA: source única (`anbima_indices`) com competence == column name.
INDEX_SOURCES: dict[str, str] = {
    "cdi": "bcb_cdi",
    "selic": "bcb_selic",
    "ipca": "bcb_ipca",
    "inpc": "bcb_inpc",
    "igpm": "bcb_igpm",
    "ima_b": "anbima_indices",
    "ima_b_5": "anbima_indices",
    "ima_b_5plus": "anbima_indices",
    "ima_geral": "anbima_indices",
    "ima_s": "anbima_indices",
    "irf_m": "anbima_indices",
}

ANBIMA_SOURCE = "anbima_indices"

_EMPTY_FRAME_SCHEMA = {"data": pl.Date, "valor": pl.Float64}


# ---- BCB JSON parser ---------------------------------------------------------


def _bcb_partitions_to_frame(bronze_root: Path, source: str) -> pl.DataFrame:
    """Lê todos `raw.json` de partitions BCB e retorna frame `(data, valor)`.

    BCB SGS retorna `[{"data": "DD/MM/YYYY", "valor": "0.007469"}, ...]`.
    Chunks têm bordas overlapping → dedup por `data`.
    """
    parts: list[pl.DataFrame] = []
    for part in all_partitions_for(bronze_root, source):
        f = part / "raw.json"
        if not f.exists():
            continue
        try:
            raw = json.loads(f.read_text())
        except json.JSONDecodeError as e:
            log.warning("silver.index_series.bcb_decode_error", source=source, err=str(e))
            continue
        if not isinstance(raw, list) or not raw:
            continue
        try:
            df = pl.DataFrame(raw)
        except Exception as e:
            log.warning("silver.index_series.bcb_frame_error", source=source, err=str(e))
            continue
        parts.append(df)

    if not parts:
        return pl.DataFrame(schema=_EMPTY_FRAME_SCHEMA)

    df = pl.concat(parts, how="vertical_relaxed")
    if "data" not in df.columns or "valor" not in df.columns:
        log.warning("silver.index_series.bcb_unexpected_keys", source=source, cols=df.columns)
        return pl.DataFrame(schema=_EMPTY_FRAME_SCHEMA)

    return (
        df.select(
            pl.col("data").cast(pl.Utf8, strict=False).str.to_date(format="%d/%m/%Y", strict=False).alias("data"),
            pl.col("valor").cast(pl.Utf8, strict=False).cast(pl.Float64, strict=False).alias("valor"),
        )
        .drop_nulls("data")
        .unique(subset=["data"], keep="first")
    )


# ---- ANBIMA XLS parser -------------------------------------------------------


_ANBIMA_DATE_COL = "Data de Referência"
_ANBIMA_VALUE_COL = "Número Índice"


def _anbima_xlsx_to_frame(bronze_root: Path, competence: str) -> pl.DataFrame:
    """Lê o `raw.xlsx` da partition `anbima_indices/competence=<col>/`.

    Cada XLS publicado pela ANBIMA tem colunas:
      'Índice', 'Data de Referência' (Date), 'Número Índice' (Float64), ...
    Retorna frame `(data, valor)` com tipos coerced; ignora linhas sem data.
    """
    part = latest_partition_dir(bronze_root, ANBIMA_SOURCE, competence=competence)
    if part is None:
        return pl.DataFrame(schema=_EMPTY_FRAME_SCHEMA)
    f = part / "raw.xlsx"
    if not f.exists():
        log.warning("silver.index_series.anbima_xlsx_missing", competence=competence, path=str(f))
        return pl.DataFrame(schema=_EMPTY_FRAME_SCHEMA)

    try:
        df = pl.read_excel(f)
    except Exception as e:
        log.warning("silver.index_series.anbima_xlsx_read_error", competence=competence, err=str(e))
        return pl.DataFrame(schema=_EMPTY_FRAME_SCHEMA)

    if _ANBIMA_DATE_COL not in df.columns or _ANBIMA_VALUE_COL not in df.columns:
        log.warning(
            "silver.index_series.anbima_unexpected_columns",
            competence=competence,
            cols=df.columns,
        )
        return pl.DataFrame(schema=_EMPTY_FRAME_SCHEMA)

    return (
        df.select(
            pl.col(_ANBIMA_DATE_COL).cast(pl.Date, strict=False).alias("data"),
            pl.col(_ANBIMA_VALUE_COL).cast(pl.Float64, strict=False).alias("valor"),
        )
        .drop_nulls("data")
        .unique(subset=["data"], keep="first")
        .sort("data")
    )


# ---- Quality report ----------------------------------------------------------


def _months_between(d_min: date, d_max: date) -> int:
    return (d_max.year - d_min.year) * 12 + (d_max.month - d_min.month) + 1


def _assess_index(
    col_name: str,
    df: pl.DataFrame,
    qs_dates: set[date],
    granularity: str,
) -> dict[str, Any]:
    """Assess coverage of a single index series against the quota_series range.

    `granularity` é declarado em `configs/benchmarks.yaml` (daily | monthly).
    Threshold de cobertura difere por granularidade.
    """
    if df.is_empty():
        return {
            "name": col_name,
            "n_obs": 0,
            "data_min": None,
            "data_max": None,
            "granularity": granularity,
            "coverage_pct": 0.0,
            "status": "FAIL",
            "note": "no data",
        }

    dates_list = df["data"].to_list()
    d_min = min(dates_list)
    d_max = max(dates_list)

    note = ""
    if granularity == "daily":
        qs_in_range = {d for d in qs_dates if d_min <= d <= d_max}
        if not qs_in_range:
            coverage = 0.0
        else:
            obs = set(dates_list)
            coverage = len(obs & qs_in_range) / len(qs_in_range) * 100.0
        if qs_dates and d_min > min(qs_dates):
            note = f"late_start (vs quota {min(qs_dates)})"
    elif granularity == "monthly":
        n_months = _months_between(d_min, d_max)
        coverage = (df.height / n_months * 100.0) if n_months > 0 else 0.0
    else:
        coverage = float("nan")

    if df.height == 0:
        status = "FAIL"
    elif granularity in ("daily", "monthly") and coverage < 95.0:
        status = "WARN"
    elif granularity not in ("daily", "monthly"):
        status = "WARN"
    else:
        status = "OK"

    return {
        "name": col_name,
        "n_obs": df.height,
        "data_min": d_min,
        "data_max": d_max,
        "granularity": granularity,
        "coverage_pct": coverage,
        "status": status,
        "note": note,
    }


def _quota_series_dates(settings: Settings, as_of: date) -> set[date]:
    qs_path = silver_path(settings, "quota_series", as_of.isoformat())
    if not qs_path.exists():
        log.warning("silver.index_series.quota_series_missing", path=str(qs_path))
        return set()
    return set(
        pl.read_parquet(qs_path)
        .select("dt_comptc")
        .drop_nulls()
        .unique()
        .to_series()
        .to_list()
    )


def _format_coverage_md(
    as_of: date,
    qs_dates: set[date],
    assessments: list[dict[str, Any]],
) -> str:
    qs_min = min(qs_dates) if qs_dates else None
    qs_max = max(qs_dates) if qs_dates else None
    qs_n = len(qs_dates)
    qs_months = _months_between(qs_min, qs_max) if qs_min and qs_max else 0

    lines: list[str] = []
    lines.append(f"# index_series — quality (as_of={as_of.isoformat()})")
    lines.append("")
    if qs_dates:
        lines.append(
            f"Range esperado (quota_series): {qs_min} → {qs_max} "
            f"({qs_n} dias úteis, {qs_months} meses)"
        )
    else:
        lines.append("Range esperado (quota_series): **NÃO DISPONÍVEL** (parquet ausente)")
    lines.append("")
    lines.append("## Cobertura por índice")
    lines.append("")
    lines.append("| índice        | n_obs | data_min   | data_max   | granularidade | cobertura % | status | nota |")
    lines.append("|---------------|------:|------------|------------|---------------|------------:|--------|------|")
    for a in assessments:
        d_min = a["data_min"].isoformat() if a["data_min"] else "-"
        d_max = a["data_max"].isoformat() if a["data_max"] else "-"
        cov = "-" if a["coverage_pct"] != a["coverage_pct"] else f"{a['coverage_pct']:.1f}"
        lines.append(
            f"| {a['name']:<13} | {a['n_obs']:>5} | {d_min:<10} | {d_max:<10} "
            f"| {a['granularity']:<13} | {cov:>11} | {a['status']:<6} | {a['note']} |"
        )
    lines.append("")

    warns = [a for a in assessments if a["status"] in ("WARN", "FAIL")]
    if warns:
        lines.append("## Alertas")
        lines.append("")
        for a in warns:
            extra = f" — {a['note']}" if a["note"] else ""
            lines.append(
                f"- **{a['name']}** ({a['status']}): {a['n_obs']} observações, "
                f"granularidade {a['granularity']}{extra}"
            )
        lines.append("")
    else:
        lines.append("## Alertas")
        lines.append("")
        lines.append("Nenhum — todas as séries cobrem ≥95% do range esperado.")
        lines.append("")

    return "\n".join(lines)


def _write_quality_report(
    settings: Settings,
    as_of: date,
    qs_dates: set[date],
    assessments: list[dict[str, Any]],
) -> Path:
    out_dir = settings.pipeline.reports_root / f"as_of={as_of.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "index_series_quality.md"
    out_path.write_text(_format_coverage_md(as_of, qs_dates, assessments))
    return out_path


# ---- Main --------------------------------------------------------------------


def run(settings: Settings, as_of: date) -> Path:
    bronze_root = settings.bronze_root
    benchmarks_cfg = settings.benchmarks

    series: dict[str, pl.DataFrame] = {}
    assessments: list[dict[str, Any]] = []
    qs_dates = _quota_series_dates(settings, as_of)

    for col, source in INDEX_SOURCES.items():
        if source == ANBIMA_SOURCE:
            df = _anbima_xlsx_to_frame(bronze_root, competence=col)
        else:
            df = _bcb_partitions_to_frame(bronze_root, source)

        if df.is_empty():
            log.warning("silver.index_series.empty_source", col=col, source=source)
        else:
            log.info(
                "silver.index_series.source_loaded",
                col=col,
                source=source,
                n_obs=df.height,
            )

        series[col] = df
        cfg = benchmarks_cfg.get(col, {}) if isinstance(benchmarks_cfg, dict) else {}
        granularity = cfg.get("granularity", "unknown") if isinstance(cfg, dict) else "unknown"
        assessments.append(_assess_index(col, df, qs_dates, granularity))

    essentials = {"cdi", "selic", "ipca"}
    missing_essentials = [c for c in essentials if series[c].is_empty()]
    if missing_essentials:
        raise RuntimeError(
            f"Essential index sources empty: {missing_essentials}. "
            "Run `ingest` first or check bronze partitions."
        )

    base: pl.DataFrame | None = None
    for col, df in series.items():
        if df.is_empty():
            continue
        df_renamed = df.rename({"valor": col})
        base = df_renamed if base is None else base.join(df_renamed, on="data", how="full", coalesce=True)

    if base is None:
        raise RuntimeError("No index series loaded — bronze appears empty for all sources.")

    for col in INDEX_SOURCES:
        if col not in base.columns:
            base = base.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))

    out = (
        base.select(["data"] + list(INDEX_SOURCES))
        .filter(pl.col("data") <= as_of)
        .sort("data")
    )

    out_path = silver_path(settings, "index_series", as_of.isoformat())
    write_parquet(out, out_path)

    report_path = _write_quality_report(settings, as_of, qs_dates, assessments)

    log.info(
        "silver.index_series.written",
        path=str(out_path),
        rows=out.height,
        n_indices=len(INDEX_SOURCES),
        report=str(report_path),
        dt_min=str(out["data"].min()),
        dt_max=str(out["data"].max()),
    )
    return out_path
