"""Schema contract for CVM Registro Fundo/Classe (registro_fundo_classe.zip).

Post-CVM Resolution 175 (Oct 2023), the regulatory hierarchy is
fundo (umbrella) -> classe -> subclasse. This file is the source of truth
for the (CNPJ_FUNDO, CNPJ_CLASSE, CNPJ_SUBCLASSE) mapping and contains
the predecessor CNPJs needed to stitch pre-2024 history.
"""
from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class RegistroClasseRow(BaseModel):
    cnpj_fundo: str
    cnpj_classe: str | None = None
    cnpj_subclasse: str | None = None

    denom_social_fundo: str | None = None
    denom_social_classe: str | None = None

    cnpj_predecessor: str | None = None     # legacy CNPJ pre-CVM 175
    dt_migracao: date | None = None

    classe_anbima: str | None = None
    publico_alvo: str | None = None
    sit: str | None = None
