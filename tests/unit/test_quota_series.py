"""Unit tests for silver/build_quota_series.

Targets the pure transform pieces:
- pre/post-CVM 175 column mapping to lowercase canonical schema
- type coercion (Date, Float64, Int64, zfill 14 CNPJ)
- dedup by (cnpj_fundo_classe, id_subclasse, dt_comptc)
"""
from __future__ import annotations

import polars as pl


def _post175_raw() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "TP_FUNDO_CLASSE": ["FIF", "FIF"],
            "CNPJ_FUNDO_CLASSE": ["00.123.456/0001-99", "12345678000199"],
            "ID_SUBCLASSE": ["MZMRC1747322915", None],
            "DT_COMPTC": ["2025-12-29", "2025-12-29"],
            "VL_TOTAL": ["1000000.50", "2500000"],
            "VL_QUOTA": ["1.234567890", "2.0"],
            "VL_PATRIM_LIQ": ["999999.99", "2400000"],
            "CAPTC_DIA": ["100", "0"],
            "RESG_DIA": ["50", "0"],
            "NR_COTST": ["10", "5"],
        }
    )


def _pre175_raw() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "CNPJ_FUNDO": ["00.123.456/0001-99"],
            "DT_COMPTC": ["2019-06-28"],
            "VL_TOTAL": ["500000"],
            "VL_QUOTA": ["1.5"],
            "VL_PATRIM_LIQ": ["499000"],
            "CAPTC_DIA": ["10"],
            "RESG_DIA": ["5"],
            "NR_COTST": ["3"],
        }
    )


def test_post175_mapping_preserves_all_fields():
    from fund_rank.silver.build_quota_series import _apply_types, _normalize_csv

    out = _apply_types(_normalize_csv(_post175_raw(), "post175"))

    assert out.columns == [
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
    row = out.row(0, named=True)
    assert row["tp_fundo_classe"] == "FIF"
    assert row["cnpj_fundo_classe"] == "00123456000199"
    assert row["id_subclasse"] == "MZMRC1747322915"
    assert row["vl_total"] == 1000000.50
    # row 1: id_subclasse may be null; cnpj zfilled 14
    row1 = out.row(1, named=True)
    assert row1["id_subclasse"] is None
    assert row1["cnpj_fundo_classe"] == "12345678000199"


def test_pre175_mapping_nulls_subclass_fields_keeps_vl_total():
    from fund_rank.silver.build_quota_series import _apply_types, _normalize_csv

    out = _apply_types(_normalize_csv(_pre175_raw(), "pre175"))

    row = out.row(0, named=True)
    assert row["tp_fundo_classe"] is None
    assert row["id_subclasse"] is None
    assert row["cnpj_fundo_classe"] == "00123456000199"
    assert row["vl_total"] == 500000.0
    assert row["vl_quota"] == 1.5


def test_columns_are_all_lowercase():
    from fund_rank.silver.build_quota_series import OUTPUT_COLUMNS, _apply_types, _normalize_csv

    out = _apply_types(_normalize_csv(_post175_raw(), "post175"))
    assert out.columns == OUTPUT_COLUMNS
    assert all(c == c.lower() for c in out.columns)


def test_types_coerced_correctly():
    from fund_rank.silver.build_quota_series import _apply_types, _normalize_csv

    out = _apply_types(_normalize_csv(_post175_raw(), "post175"))
    assert out.schema["dt_comptc"] == pl.Date
    assert out.schema["cnpj_fundo_classe"] == pl.Utf8
    assert out.schema["vl_total"] == pl.Float64
    assert out.schema["vl_quota"] == pl.Float64
    assert out.schema["vl_patrim_liq"] == pl.Float64
    assert out.schema["captc_dia"] == pl.Float64
    assert out.schema["resg_dia"] == pl.Float64
    assert out.schema["nr_cotst"] == pl.Int64


def test_dedup_by_composite_key():
    from fund_rank.silver.build_quota_series import _apply_types, _normalize_csv

    raw = pl.DataFrame(
        {
            "TP_FUNDO_CLASSE": ["FIF", "FIF"],
            "CNPJ_FUNDO_CLASSE": ["00123456000199", "00123456000199"],
            "ID_SUBCLASSE": [None, None],
            "DT_COMPTC": ["2025-12-29", "2025-12-29"],
            "VL_TOTAL": ["100", "100"],
            "VL_QUOTA": ["1.0", "1.0"],
            "VL_PATRIM_LIQ": ["100", "100"],
            "CAPTC_DIA": ["0", "0"],
            "RESG_DIA": ["0", "0"],
            "NR_COTST": ["1", "1"],
        }
    )
    out = _apply_types(_normalize_csv(raw, "post175"))
    deduped = out.unique(
        subset=["cnpj_fundo_classe", "id_subclasse", "dt_comptc"],
        keep="first",
        maintain_order=True,
    )
    assert deduped.height == 1
