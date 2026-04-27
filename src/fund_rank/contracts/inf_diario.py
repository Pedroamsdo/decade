"""Schema contract for CVM INF_DIARIO.

Pre- and post-CVM 175 schemas are merged: silver-side normalization writes
both `cnpj_fundo` (legacy) and `cnpj_classe` (post-2024) — exactly one is
populated per row. See `silver/build_quota_series.py` for the merge logic.
"""
from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class InfDiarioRow(BaseModel):
    cnpj_fundo: str | None = None      # populated for pre-CVM 175 rows
    cnpj_classe: str | None = None     # populated for post-CVM 175 rows
    tp_fundo_classe: str | None = None
    dt_comptc: date

    vl_total: float | None = None
    vl_quota: float
    vl_patrim_liq: float
    captc_dia: float | None = None
    resg_dia: float | None = None
    nr_cotst: int | None = None
