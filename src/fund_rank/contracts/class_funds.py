"""Schema contract for silver/class_funds — one row per CNPJ_Classe.

Excludes classes that have subclasses (those are in silver/subclass_funds).
Joins CVM `registro_classe`/`registro_fundo`, ANBIMA FUNDOS-175 xlsx, and
the latest record from `cad_fi_hist_taxa_adm`/`taxa_perfm` and `cad_fi_rentab`
per CNPJ_Fundo.
"""
from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class ClassFundRow(BaseModel):
    cnpj_fundo: str
    cnpj_classe: str
    denom_social_fundo: str | None = None
    denom_social_classe: str | None = None
    situacao: str | None = None
    data_de_inicio: date | None = None
    exclusivo: str | None = None
    publico_alvo: str | None = None
    condominio: str | None = None
    classificacao_anbima: str | None = None
    composicao_fundos: str | None = None
    tributacao_alvo: str | None = None
    aplicacao_minima: str | None = None
    prazo_de_resgate: int | None = None
    taxa_adm: float | None = None
    taxa_perform: float | None = None
    benchmark: str | None = None
