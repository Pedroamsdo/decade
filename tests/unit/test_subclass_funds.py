"""Unit tests for silver/build_subclass_funds.

Targets the pure transform pieces:
- chain join registro_subclasse → registro_classe → registro_fundo
- ANBIMA two-pass: precise by Código CVM Subclasse vs fallback by (cnpj_fundo, cnpj_classe)
- pass-1 priority over pass-2
- id_subclasse_cvm is preserved as Utf8 (alphanumeric token)
"""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _chdir_repo_root(monkeypatch):
    monkeypatch.chdir(REPO_ROOT)


def test_build_base_resolves_cnpj_fundo_and_classe():
    from fund_rank.silver.build_subclass_funds import _build_base

    df_subclasse = pl.DataFrame(
        {
            "ID_Registro_Classe": ["10", "20"],
            "ID_Subclasse": ["AAA111", "BBB222"],
            "Denominacao_Social": ["SUBCLASSE A", "SUBCLASSE B"],
            "Situacao": ["Ativo", "Ativo"],
            "Data_Inicio_Situacao": ["2024-01-01", "2024-02-01"],
            "Exclusivo": ["N", "S"],
            "Publico_Alvo": ["Geral", "Qualificado"],
            "Forma_Condominio": ["Aberto", "Fechado"],
        }
    )
    df_classe = pl.DataFrame(
        {
            "ID_Registro_Classe": ["10", "20", "30"],
            "ID_Registro_Fundo": ["100", "200", "300"],
            "CNPJ_Classe": ["11111111000111", "22222222000122", "33333333000133"],
        }
    )
    df_fundo = pl.DataFrame(
        {
            "ID_Registro_Fundo": ["100", "200"],
            "CNPJ_Fundo": ["10000000000001", "20000000000002"],
        }
    )

    base = _build_base(df_subclasse, df_classe, df_fundo)

    by_id = {r["id_subclasse_cvm"]: r for r in base.iter_rows(named=True)}
    assert by_id["AAA111"]["cnpj_classe"] == "11111111000111"
    assert by_id["AAA111"]["cnpj_fundo"] == "10000000000001"
    assert by_id["BBB222"]["cnpj_classe"] == "22222222000122"
    assert by_id["BBB222"]["cnpj_fundo"] == "20000000000002"
    assert by_id["AAA111"]["denom_social_subclasse"] == "SUBCLASSE A"
    assert by_id["AAA111"]["condominio"] == "Aberto"


def test_anbima_pass1_matches_by_codigo_cvm_subclasse():
    from fund_rank.silver.build_subclass_funds import _two_pass_anbima_join

    base = pl.DataFrame(
        {
            "id_subclasse_cvm": ["AAA111"],
            "cnpj_fundo": ["10000000000001"],
            "cnpj_classe": ["11111111000111"],
        }
    )
    anbima_sub = pl.DataFrame(
        {
            "codigo_cvm_subclasse": ["AAA111"],
            "cnpj_fundo": ["10000000000001"],
            "cnpj_classe": ["11111111000111"],
            "classificacao_anbima": ["Renda Fixa Simples"],
            "composicao_fundos": ["Composição X"],
            "tributacao_alvo": ["Curto Prazo"],
            "aplicacao_minima": ["R$ 100"],
            "prazo_de_resgate": [3],
        },
        schema={
            "codigo_cvm_subclasse": pl.Utf8,
            "cnpj_fundo": pl.Utf8,
            "cnpj_classe": pl.Utf8,
            "classificacao_anbima": pl.Utf8,
            "composicao_fundos": pl.Utf8,
            "tributacao_alvo": pl.Utf8,
            "aplicacao_minima": pl.Utf8,
            "prazo_de_resgate": pl.Int64,
        },
    )

    out, breakdown = _two_pass_anbima_join(base, anbima_sub)

    row = out.row(0, named=True)
    assert row["classificacao_anbima"] == "Renda Fixa Simples"
    assert row["prazo_de_resgate"] == 3
    assert breakdown.pass1_matched == 1
    assert breakdown.pass2_matched == 0


def test_anbima_pass2_fallback_by_cnpj():
    from fund_rank.silver.build_subclass_funds import _two_pass_anbima_join

    # Base subclasse whose codigo doesn't appear in ANBIMA precise side, but
    # the (cnpj_fundo, cnpj_classe) is in the fallback.
    base = pl.DataFrame(
        {
            "id_subclasse_cvm": ["BBB222"],
            "cnpj_fundo": ["20000000000002"],
            "cnpj_classe": ["22222222000122"],
        }
    )
    anbima_sub = pl.DataFrame(
        {
            "codigo_cvm_subclasse": [None],
            "cnpj_fundo": ["20000000000002"],
            "cnpj_classe": ["22222222000122"],
            "classificacao_anbima": ["Multimercados Livre"],
            "composicao_fundos": [None],
            "tributacao_alvo": [None],
            "aplicacao_minima": [None],
            "prazo_de_resgate": [None],
        },
        schema={
            "codigo_cvm_subclasse": pl.Utf8,
            "cnpj_fundo": pl.Utf8,
            "cnpj_classe": pl.Utf8,
            "classificacao_anbima": pl.Utf8,
            "composicao_fundos": pl.Utf8,
            "tributacao_alvo": pl.Utf8,
            "aplicacao_minima": pl.Utf8,
            "prazo_de_resgate": pl.Int64,
        },
    )

    out, breakdown = _two_pass_anbima_join(base, anbima_sub)

    row = out.row(0, named=True)
    assert row["classificacao_anbima"] == "Multimercados Livre"
    assert breakdown.pass1_matched == 0
    assert breakdown.pass2_matched == 1


def test_anbima_pass1_priority_over_pass2():
    """When both passes have a candidate, pass-1 (precise) wins."""
    from fund_rank.silver.build_subclass_funds import _two_pass_anbima_join

    base = pl.DataFrame(
        {
            "id_subclasse_cvm": ["AAA111"],
            "cnpj_fundo": ["10000000000001"],
            "cnpj_classe": ["11111111000111"],
        }
    )
    anbima_sub = pl.DataFrame(
        {
            # one row precise (matches by id), one row fallback (matches by cnpj)
            "codigo_cvm_subclasse": ["AAA111", None],
            "cnpj_fundo": ["10000000000001", "10000000000001"],
            "cnpj_classe": ["11111111000111", "11111111000111"],
            "classificacao_anbima": ["PRECISE", "FALLBACK"],
            "composicao_fundos": [None, None],
            "tributacao_alvo": [None, None],
            "aplicacao_minima": [None, None],
            "prazo_de_resgate": [None, None],
        },
        schema={
            "codigo_cvm_subclasse": pl.Utf8,
            "cnpj_fundo": pl.Utf8,
            "cnpj_classe": pl.Utf8,
            "classificacao_anbima": pl.Utf8,
            "composicao_fundos": pl.Utf8,
            "tributacao_alvo": pl.Utf8,
            "aplicacao_minima": pl.Utf8,
            "prazo_de_resgate": pl.Int64,
        },
    )

    out, breakdown = _two_pass_anbima_join(base, anbima_sub)

    assert out.row(0, named=True)["classificacao_anbima"] == "PRECISE"
    assert breakdown.pass1_matched == 1


def test_anbima_fallback_ambiguity_dropped_count():
    """Two ANBIMA rows with same (fundo, classe), null código → dedup keeps 1, 1 dropped."""
    from fund_rank.silver.build_subclass_funds import _two_pass_anbima_join

    base = pl.DataFrame(
        {
            "id_subclasse_cvm": ["BBB222"],
            "cnpj_fundo": ["20000000000002"],
            "cnpj_classe": ["22222222000122"],
        }
    )
    anbima_sub = pl.DataFrame(
        {
            "codigo_cvm_subclasse": [None, None],
            "cnpj_fundo": ["20000000000002", "20000000000002"],
            "cnpj_classe": ["22222222000122", "22222222000122"],
            "classificacao_anbima": ["FIRST", "SECOND"],
            "composicao_fundos": [None, None],
            "tributacao_alvo": [None, None],
            "aplicacao_minima": [None, None],
            "prazo_de_resgate": [None, None],
        },
        schema={
            "codigo_cvm_subclasse": pl.Utf8,
            "cnpj_fundo": pl.Utf8,
            "cnpj_classe": pl.Utf8,
            "classificacao_anbima": pl.Utf8,
            "composicao_fundos": pl.Utf8,
            "tributacao_alvo": pl.Utf8,
            "aplicacao_minima": pl.Utf8,
            "prazo_de_resgate": pl.Int64,
        },
    )

    out, breakdown = _two_pass_anbima_join(base, anbima_sub)

    assert out.height == 1
    assert out.row(0, named=True)["classificacao_anbima"] == "FIRST"
    assert breakdown.ambiguity_dropped == 1


def test_id_subclasse_cvm_is_string():
    from fund_rank.silver.build_subclass_funds import _build_base

    df_subclasse = pl.DataFrame(
        {
            "ID_Registro_Classe": ["10"],
            "ID_Subclasse": ["MZMRC1747322915"],
            "Denominacao_Social": ["S"],
            "Situacao": ["Ativo"],
            "Data_Inicio_Situacao": ["2024-01-01"],
            "Exclusivo": ["N"],
            "Publico_Alvo": ["Geral"],
            "Forma_Condominio": ["Aberto"],
        }
    )
    df_classe = pl.DataFrame(
        {
            "ID_Registro_Classe": ["10"],
            "ID_Registro_Fundo": ["100"],
            "CNPJ_Classe": ["11111111000111"],
        }
    )
    df_fundo = pl.DataFrame(
        {"ID_Registro_Fundo": ["100"], "CNPJ_Fundo": ["10000000000001"]}
    )

    base = _build_base(df_subclasse, df_classe, df_fundo)
    assert base.schema["id_subclasse_cvm"] == pl.Utf8
    assert base.row(0, named=True)["id_subclasse_cvm"] == "MZMRC1747322915"
