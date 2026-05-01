"""Unit tests for silver/build_class_funds.

Targets the pure transform pieces:
- subclass anti-join filter
- most-recent row selection per CNPJ_Fundo from cad_fi_hist_*
- CNPJ normalization end-to-end through the hist loader
- taxa_adm scaling (divide by 100)
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _chdir_repo_root(monkeypatch):
    monkeypatch.chdir(REPO_ROOT)


def test_subclass_filter_excludes_classes_with_subclasses():
    from fund_rank.silver.build_class_funds import _apply_subclass_filter

    df_classe = pl.DataFrame(
        {
            "ID_Registro_Classe": [10, 20, 30],
            "CNPJ_Classe": ["00000000000010", "00000000000020", "00000000000030"],
        }
    )
    df_subclasse = pl.DataFrame({"ID_Registro_Classe": [20]})

    out = _apply_subclass_filter(df_classe, df_subclasse)

    assert out.height == 2
    assert sorted(out["ID_Registro_Classe"].to_list()) == [10, 30]


def test_subclass_filter_noop_when_empty():
    from fund_rank.silver.build_class_funds import _apply_subclass_filter

    df_classe = pl.DataFrame({"ID_Registro_Classe": [1, 2, 3]})
    df_subclasse = pl.DataFrame({"ID_Registro_Classe": []}, schema={"ID_Registro_Classe": pl.Int64})

    out = _apply_subclass_filter(df_classe, df_subclasse)

    assert out.height == 3


def _make_bronze_zip(
    bronze_root: Path,
    source: str,
    members: dict[str, str],
    today: date = date(2026, 4, 28),
) -> Path:
    """Write the canonical bronze artifact (raw.zip + manifest) for tests."""
    import json
    import zipfile

    part = bronze_root / source
    part.mkdir(parents=True, exist_ok=True)
    zip_path = part / "raw.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        for name, text in members.items():
            z.writestr(name, text.encode("latin-1"))
    manifest = {
        "source": source,
        "url": "https://example/test.zip",
        "competence": None,
        "etag": None,
        "last_modified": None,
        "sha256": "0" * 64,
        "byte_size": zip_path.stat().st_size,
        "row_count": None,
        "ingested_at": today.isoformat() + "T00:00:00Z",
        "status": "fetched",
    }
    (part / "_manifest.json").write_text(json.dumps(manifest))
    return part


def _settings_with_data_root(tmp_path: Path):
    """Build a fresh Settings pointed at tmp_path/data."""
    from fund_rank.settings import Settings

    s = Settings()
    _ = s.pipeline  # materialize from yaml
    s.pipeline.data_root = tmp_path / "data"
    s.pipeline.reports_root = tmp_path / "reports"
    return s


def test_hist_row_selection_keeps_most_recent_per_cnpj(tmp_path):
    from fund_rank.silver._io import read_cad_fi_hist_latest

    settings = _settings_with_data_root(tmp_path)
    csv_text = (
        "CNPJ_FUNDO;DT_INI_TAXA_ADM;TAXA_ADM\n"
        "12345678000199;2020-01-01;0.50\n"
        "12345678000199;2024-06-15;1.50\n"
        "12345678000199;2022-03-10;0.75\n"
        "98765432000100;2023-09-01;2.00\n"
    )
    _make_bronze_zip(
        settings.bronze_root,
        "cvm_cad_fi_hist",
        {"cad_fi_hist_taxa_adm.csv": csv_text},
    )

    out = read_cad_fi_hist_latest(
        settings,
        member_name="cad_fi_hist_taxa_adm.csv",
        value_col="TAXA_ADM",
        date_col="DT_INI_TAXA_ADM",
        output_alias="taxa_adm",
        divide_by_100=True,
    )

    rows = {r["cnpj_fundo"]: r["taxa_adm"] for r in out.iter_rows(named=True)}
    assert rows["12345678000199"] == pytest.approx(0.015)  # 1.50% = 0.015
    assert rows["98765432000100"] == pytest.approx(0.02)
    assert out.height == 2


def test_hist_handles_missing_partition(tmp_path):
    from fund_rank.silver._io import read_cad_fi_hist_latest

    settings = _settings_with_data_root(tmp_path)
    settings.bronze_root.mkdir(parents=True, exist_ok=True)

    out = read_cad_fi_hist_latest(
        settings,
        member_name="cad_fi_hist_taxa_adm.csv",
        value_col="TAXA_ADM",
        date_col="DT_INI_TAXA_ADM",
        output_alias="taxa_adm",
        divide_by_100=True,
    )

    assert out.is_empty()
    assert "cnpj_fundo" in out.columns
    assert "taxa_adm" in out.columns
