"""Schema contract for CVM CDA (composição de carteira).

CDA zips contain multiple CSV blocks (CVM 175 era, observed in 202512):
  BLC_1: Títulos Públicos + Operações Compromissadas
  BLC_2: Cotas de Fundos              <-- key for master/feeder detection
  BLC_3: Derivativos (swaps)
  BLC_4: Ações e TVM (variant)
  BLC_5: Depósitos a prazo / títulos de IF (CDB, DPGE)
  BLC_6: Debêntures / Crédito Privado / Agro
  BLC_7: Investimento no Exterior
  BLC_8: Ações e TVM (variant)
  CONFID: posições confidenciais
  PL:    PL agregado por classe

For master/feeder we read BLC_2 (filter where TP_APLIC == "Cotas de Fundos") + PL.
The silver layer should always filter by TP_APLIC rather than relying on the BLC
number, since CVM has reordered blocks across regulatory regimes.
"""
from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class CdaCotasRow(BaseModel):
    """One row of BLC_5 (cotas de fundos)."""

    cnpj_fundo: str | None = None
    cnpj_classe: str | None = None
    dt_comptc: date

    cnpj_fundo_alvo: str | None = None     # the master fund CNPJ
    cnpj_classe_alvo: str | None = None    # post-CVM 175
    denom_social_fundo_alvo: str | None = None

    vl_merc_pos_final: float
    pct_pl: float | None = None


class CdaPlRow(BaseModel):
    """Aggregate PL row (PL.csv)."""

    cnpj_fundo: str | None = None
    cnpj_classe: str | None = None
    dt_comptc: date
    vl_patrim_liq: float
