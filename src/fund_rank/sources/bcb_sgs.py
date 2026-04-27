"""BCB SGS (Sistema Gerenciador de Séries Temporais) helpers.

Series 12 = CDI daily (% per day).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from fund_rank.settings import Settings


@dataclass(frozen=True)
class SgsEndpoint:
    name: str
    url: str
    series_id: int
    date_range: tuple[date, date]


def cdi_url(settings: Settings, dt_ini: date, dt_fim: date) -> SgsEndpoint:
    template = settings.pipeline.sources.bcb_cdi.url_template or ""
    url = template.format(
        dt_ini=dt_ini.strftime("%d/%m/%Y"),
        dt_fim=dt_fim.strftime("%d/%m/%Y"),
    )
    return SgsEndpoint(
        name="bcb_cdi",
        url=url,
        series_id=12,
        date_range=(dt_ini, dt_fim),
    )
