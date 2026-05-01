"""Common I/O for the silver layer.

Reads bronze partitions (often ZIP-compressed CVM CSVs in latin-1) into
polars DataFrames using CVM-tolerant settings (no quote interpretation,
truncate ragged lines).
"""
from __future__ import annotations

import io
import re
import unicodedata
import zipfile
from pathlib import Path
from typing import Iterable, Iterator

import polars as pl

from fund_rank.bronze.manifest import partition_dir
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings

log = get_logger(__name__)


# ---- Bronze partition discovery ----------------------------------------------


def all_partitions_for(
    bronze_root: Path,
    source: str,
    competence: str | None = None,
) -> list[Path]:
    """All canonical partition dirs under ``bronze_root/source``.

    With ``competence``, returns ``[partition]`` if it has a manifest, else ``[]``.
    Without it, returns the source root (if snapshot) plus every
    ``competence=*`` subdir that has a manifest.
    """
    if competence is not None:
        p = partition_dir(bronze_root, source, competence=competence)
        return [p] if (p / "_manifest.json").exists() else []

    src_dir = bronze_root / source
    if not src_dir.exists():
        return []

    out: list[Path] = []
    if (src_dir / "_manifest.json").exists():
        out.append(src_dir)
    for sub in sorted(src_dir.iterdir()):
        if sub.is_dir() and sub.name.startswith("competence=") and (sub / "_manifest.json").exists():
            out.append(sub)
    return out


# ---- CSV reading from zip / file ---------------------------------------------


CVM_CSV_OPTS = dict(
    separator=";",
    encoding="latin-1",
    infer_schema_length=0,
    truncate_ragged_lines=True,
    quote_char=None,        # CVM CSVs are NOT properly quoted; treat ; as the only delimiter
    null_values=["", "N/A", "NA"],
)


def read_csv_from_path(path: Path, **overrides) -> pl.DataFrame:
    opts = {**CVM_CSV_OPTS, **overrides}
    return pl.read_csv(path, **opts)


def read_csv_from_zip(zip_path: Path, member: str, **overrides) -> pl.DataFrame:
    opts = {**CVM_CSV_OPTS, **overrides}
    with zipfile.ZipFile(zip_path) as z:
        with z.open(member) as f:
            data = f.read()
    return pl.read_csv(io.BytesIO(data), **opts)


def list_zip_members(zip_path: Path) -> list[str]:
    with zipfile.ZipFile(zip_path) as z:
        return z.namelist()


# ---- Text normalization ------------------------------------------------------


_ACCENT_TRANS = str.maketrans({
    "á": "a", "à": "a", "ã": "a", "â": "a", "ä": "a",
    "Á": "A", "À": "A", "Ã": "A", "Â": "A", "Ä": "A",
    "é": "e", "è": "e", "ê": "e", "ë": "e",
    "É": "E", "È": "E", "Ê": "E", "Ë": "E",
    "í": "i", "ì": "i", "î": "i", "ï": "i",
    "Í": "I", "Ì": "I", "Î": "I", "Ï": "I",
    "ó": "o", "ò": "o", "õ": "o", "ô": "o", "ö": "o",
    "Ó": "O", "Ò": "O", "Õ": "O", "Ô": "O", "Ö": "O",
    "ú": "u", "ù": "u", "û": "u", "ü": "u",
    "Ú": "U", "Ù": "U", "Û": "U", "Ü": "U",
    "ç": "c", "Ç": "C",
})


def strip_accents(text: str | None) -> str | None:
    if text is None:
        return None
    s = text.translate(_ACCENT_TRANS)
    # Fallback for any char not in the table
    s = "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )
    return s


def normalize_text(text: str | None) -> str | None:
    """Strip accents, collapse whitespace, casefold for comparison."""
    if text is None:
        return None
    s = strip_accents(text)
    s = re.sub(r"\s+", " ", s).strip().casefold()
    return s


def normalize_cnpj(cnpj: str | None) -> str | None:
    """Return digits-only CNPJ (length 14) or None."""
    if cnpj is None:
        return None
    digits = re.sub(r"\D", "", cnpj)
    if len(digits) == 0:
        return None
    return digits.zfill(14)[:14]


# ---- Polars expressions ------------------------------------------------------


def cnpj_clean_expr(col: str, alias: str | None = None) -> pl.Expr:
    """Polars expr: strip non-digits, zfill 14. Vectorized CNPJ normalization."""
    return (
        pl.col(col)
        .str.replace_all(r"\D", "")
        .str.pad_start(14, "0")
        .str.slice(0, 14)
        .alias(alias or col)
    )


def normalize_text_expr(col: str, alias: str) -> pl.Expr:
    """Polars expr: NFKD strip-accents + lowercase + collapse whitespace."""
    return (
        pl.col(col)
        .str.normalize("NFKD")
        .str.replace_all(r"[̀-ͯ]", "")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.to_lowercase()
        .alias(alias)
    )


def text_strip_expr(col: str, alias: str | None = None) -> pl.Expr:
    """Polars expr: cast to Utf8 (non-strict) and strip surrounding whitespace."""
    return pl.col(col).cast(pl.Utf8, strict=False).str.strip_chars().alias(alias or col)


def date_iso_expr(col: str, alias: str | None = None) -> pl.Expr:
    """Polars expr: parse `YYYY-MM-DD` into Date (non-strict; bad rows -> null)."""
    return pl.col(col).str.to_date(format="%Y-%m-%d", strict=False).alias(alias or col)


def find_column(df: pl.DataFrame, *candidates: str) -> str | None:
    """Case-insensitive lookup of the first matching column name in df."""
    cols_norm = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        hit = cols_norm.get(cand.strip().lower())
        if hit is not None:
            return hit
    return None


# ---- Parquet output ----------------------------------------------------------


def write_parquet(df: pl.DataFrame, out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out, compression="zstd")
    return out


def silver_path(settings: Settings, table: str, as_of: str, *parts: str) -> Path:
    """data/silver/{table}/as_of=YYYY-MM-DD/{parts...}/data.parquet"""
    base = settings.silver_root / table / f"as_of={as_of}"
    if parts:
        base = base.joinpath(*parts)
    return base / "data.parquet"


# ---- cad_fi_hist helpers (shared by class/subclass builders) ----------------

_log_io = get_logger(__name__)


def _cad_fi_hist_zip_path(settings: Settings) -> Path | None:
    zip_path = partition_dir(settings.bronze_root, "cvm_cad_fi_hist") / "raw.zip"
    return zip_path if zip_path.exists() else None


def read_cad_fi_hist_latest(
    settings: Settings,
    member_name: str,
    value_col: str,
    date_col: str,
    output_alias: str,
    divide_by_100: bool = False,
    cast_str: bool = False,
) -> pl.DataFrame:
    """Read a member of cvm_cad_fi_hist/raw.zip and keep the most-recent row per CNPJ_Fundo.

    ``member_name`` is the CSV filename inside the zip (e.g. ``cad_fi_hist_taxa_adm.csv``).
    ``date_col`` is the file-specific start-of-validity column. Returns a 2-column
    frame: ``cnpj_fundo`` + ``output_alias``.
    """
    empty_schema = {
        "cnpj_fundo": pl.Utf8,
        output_alias: pl.Utf8 if cast_str else pl.Float64,
    }

    zip_path = _cad_fi_hist_zip_path(settings)
    if zip_path is None:
        _log_io.warning("silver.cad_fi_hist.zip_missing")
        return pl.DataFrame(schema=empty_schema)

    members = list_zip_members(zip_path)
    if member_name not in members:
        _log_io.warning(
            "silver.cad_fi_hist.member_missing",
            member=member_name,
            available=members,
        )
        return pl.DataFrame(schema=empty_schema)

    df = read_csv_from_zip(zip_path, member_name)
    cols = set(df.columns)

    cnpj_col = "CNPJ_Fundo" if "CNPJ_Fundo" in cols else "CNPJ_FUNDO"
    if cnpj_col not in cols or value_col not in cols or date_col not in cols:
        _log_io.warning(
            "silver.cad_fi_hist.unexpected_cols",
            member=member_name,
            cols=df.columns,
            wanted=(cnpj_col, value_col, date_col),
        )
        return pl.DataFrame(schema=empty_schema)

    if cast_str:
        value_expr = pl.col(value_col).cast(pl.Utf8, strict=False).alias(output_alias)
    else:
        v = pl.col(value_col).cast(pl.Float64, strict=False)
        if divide_by_100:
            v = v / 100.0
        value_expr = v.alias(output_alias)

    out = (
        df.select(
            cnpj_clean_expr(cnpj_col, "cnpj_fundo"),
            date_iso_expr(date_col, "_dt_ini"),
            value_expr,
        )
        .sort("_dt_ini", descending=True, nulls_last=True)
        .unique(subset=["cnpj_fundo"], keep="first", maintain_order=True)
        .select("cnpj_fundo", output_alias)
    )
    _log_io.info(
        "silver.cad_fi_hist.loaded",
        member=member_name,
        rows=len(out),
        col=output_alias,
    )
    return out
