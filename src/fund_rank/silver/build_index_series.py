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
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

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
        raw = json.loads(f.read_text())
        if not isinstance(raw, list) or not raw:
            continue
        parts.append(pl.DataFrame(raw))

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


def _anbima_xlsx_to_frame(
    bronze_root: Path, competence: str, drop_filename: str | None
) -> pl.DataFrame:
    """Lê o XLS dropado em `bronze/anbima_indices/dropped/<drop_filename>`.

    O nome do arquivo vem de `benchmarks.yaml` (campo `drop_filename`).
    Cada XLS publicado pela ANBIMA tem colunas:
      'Índice', 'Data de Referência' (Date), 'Número Índice' (Float64), ...
    Retorna frame `(data, valor)` com tipos coerced; ignora linhas sem data.
    """
    if not drop_filename:
        log.warning("silver.index_series.anbima_drop_filename_missing", competence=competence)
        return pl.DataFrame(schema=_EMPTY_FRAME_SCHEMA)
    f = bronze_root / ANBIMA_SOURCE / "dropped" / drop_filename
    if not f.exists():
        log.warning("silver.index_series.anbima_xlsx_missing", competence=competence, path=str(f))
        return pl.DataFrame(schema=_EMPTY_FRAME_SCHEMA)

    # ANBIMA serves OOXML files with `.xls` extension. Calamine picks the
    # legacy-XLS sub-engine from the extension and chokes on the OLE/CFB
    # mismatch. If the file is actually OOXML (magic = "PK"), feed calamine via
    # an in-memory BytesIO so it sniffs the format from content, not extension.
    raw = f.read_bytes()
    if raw[:2] == b"PK":
        import io
        df = pl.read_excel(io.BytesIO(raw))
    else:
        df = pl.read_excel(f)

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


# ---- Main --------------------------------------------------------------------


def run(settings: Settings, as_of: date) -> Path:
    bronze_root = settings.bronze_root
    benchmarks_cfg = settings.benchmarks

    series: dict[str, pl.DataFrame] = {}

    for col, source in INDEX_SOURCES.items():
        if source == ANBIMA_SOURCE:
            cfg = benchmarks_cfg.get(col, {}) if isinstance(benchmarks_cfg, dict) else {}
            drop_filename = cfg.get("drop_filename") if isinstance(cfg, dict) else None
            df = _anbima_xlsx_to_frame(bronze_root, competence=col, drop_filename=drop_filename)
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

    log.info(
        "silver.index_series.written",
        path=str(out_path),
        rows=out.height,
        n_indices=len(INDEX_SOURCES),
        dt_min=str(out["data"].min()),
        dt_max=str(out["data"].max()),
    )
    return out_path
