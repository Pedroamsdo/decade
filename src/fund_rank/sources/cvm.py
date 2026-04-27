"""CVM Dados Abertos URL builders.

Reference: https://dados.cvm.gov.br/dados/FI/
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from fund_rank.settings import Settings


@dataclass(frozen=True)
class CvmEndpoint:
    name: str
    url: str
    competence: str | None  # "YYYY-MM" for monthly, None for snapshots


def cad_fi_url(settings: Settings) -> CvmEndpoint:
    return CvmEndpoint(
        name="cvm_cad_fi",
        url=settings.pipeline.sources.cvm_cad_fi.url or "",
        competence=None,
    )


def cad_fi_hist_url(settings: Settings) -> CvmEndpoint:
    return CvmEndpoint(
        name="cvm_cad_fi_hist",
        url=settings.pipeline.sources.cvm_cad_fi_hist.url or "",
        competence=None,
    )


def registro_classe_url(settings: Settings) -> CvmEndpoint:
    return CvmEndpoint(
        name="cvm_registro_classe",
        url=settings.pipeline.sources.cvm_registro_classe.url or "",
        competence=None,
    )


def inf_diario_url(settings: Settings, year: int, month: int) -> CvmEndpoint:
    yyyymm = f"{year:04d}{month:02d}"
    template = settings.pipeline.sources.cvm_inf_diario.url_template or ""
    return CvmEndpoint(
        name="cvm_inf_diario",
        url=template.format(yyyymm=yyyymm),
        competence=f"{year:04d}-{month:02d}",
    )


def inf_diario_hist_url(settings: Settings, year: int) -> CvmEndpoint | None:
    """Yearly historical zip — fallback for years older than CVM's monthly retention.

    Returns None if `cvm_inf_diario_hist` is not configured.
    """
    cfg = settings.pipeline.sources.cvm_inf_diario_hist
    if cfg is None or not cfg.url_template:
        return None
    return CvmEndpoint(
        name="cvm_inf_diario_hist",
        url=cfg.url_template.format(yyyy=f"{year:04d}"),
        competence=f"{year:04d}",
    )


def cda_url(settings: Settings, year: int, month: int) -> CvmEndpoint:
    yyyymm = f"{year:04d}{month:02d}"
    template = settings.pipeline.sources.cvm_cda.url_template or ""
    return CvmEndpoint(
        name="cvm_cda",
        url=template.format(yyyymm=yyyymm),
        competence=f"{year:04d}-{month:02d}",
    )


def months_between(start: date, end: date) -> list[tuple[int, int]]:
    """Return list of (year, month) inclusive between two dates."""
    out: list[tuple[int, int]] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append((y, m))
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out
