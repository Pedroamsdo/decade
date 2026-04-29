"""Schema contract for silver/subclass_funds — one row per ID_Subclasse.

Population: every row in CVM `registro_subclasse.csv` (i.e., the complement
of `silver/class_funds`, which excludes classes that have subclasses).

Joins:
  - registro_subclasse → registro_classe (ID_Registro_Classe) → CNPJ_Classe
  - registro_classe → registro_fundo (ID_Registro_Fundo) → CNPJ_Fundo
  - ANBIMA FUNDOS-175 xlsx (Estrutura == "Subclasse"):
      pass 1: precise match on ID_Subclasse <-> Código CVM Subclasse
      pass 2: fallback on (cnpj_fundo, cnpj_classe) for ANBIMA rows where
              Código CVM Subclasse is null
  - cad_fi_hist_taxa_adm.csv  (most-recent TAXA_ADM per CNPJ_Fundo)
  - cad_fi_hist_taxa_perfm.csv (most-recent VL_TAXA_PERFM per CNPJ_Fundo)
  - cad_fi_hist_rentab.csv     (most-recent RENTAB_FUNDO per CNPJ_Fundo)
"""
from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class SubclassFundRow(BaseModel):
    cnpj_fundo: str
    cnpj_classe: str
    id_subclasse_cvm: str
    denom_social_subclasse: str | None = None
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
