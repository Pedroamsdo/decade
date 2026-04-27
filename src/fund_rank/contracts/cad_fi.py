"""Schema contract for CVM CAD_FI (cad_fi.csv).

The bronze layer stores the raw CSV verbatim; this contract is the typed
silver-side projection. Field names match CVM's CSV headers (Portuguese).
"""
from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field


SituacaoFundo = Literal[
    "EM FUNCIONAMENTO NORMAL",
    "EM LIQUIDACAO",
    "EM LIQUIDACAO JUDICIAL",
    "PARALISADO",
    "PRE-OPERACIONAL",
    "CANCELADA",
]


class CadFiRow(BaseModel):
    """One row from cad_fi.csv. CVM's column names are upper-snake."""

    cnpj_fundo: str = Field(..., min_length=14, max_length=18)
    denom_social: str
    tp_fundo: str | None = None

    dt_reg: date | None = None
    dt_const: date | None = None
    dt_ini_ativ: date | None = None
    dt_cancel: date | None = None
    dt_ini_sit: date | None = None

    sit: str
    classe: str | None = None
    classe_anbima: str | None = None

    condom: str | None = None
    fundo_cotas: str | None = None      # "S"/"N"
    fundo_exclusivo: str | None = None  # "S"/"N"
    trib_lprazo: str | None = None      # "S"/"N"
    invest_qualif: str | None = None    # "S"/"N"

    publico_alvo: str | None = None

    taxa_adm: float | None = None
    taxa_perfm: str | None = None  # text in CVM, parsed in silver
    inf_taxa_adm: str | None = None
    inf_taxa_perfm: str | None = None

    cnpj_admin: str | None = None
    admin: str | None = None
    cnpj_gestor: str | None = None      # PF_PJ_GESTOR + CPF_CNPJ_GESTOR collapsed
    gestor: str | None = None

    vl_patrim_liq: float | None = None
    dt_patrim_liq: date | None = None
