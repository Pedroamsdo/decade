"""BCB SGS (Sistema Gerenciador de Séries Temporais) helpers.

Cada série tem código próprio (CDI=12, SELIC=11, IPCA=433, INPC=188, IGP-M=189).
A API limita ~10 anos por request — `chunk_decade()` divide janelas longas.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from fund_rank.settings import Settings


@dataclass(frozen=True)
class SgsEndpoint:
    name: str
    url: str
    series_id: int
    date_range: tuple[date, date]


def sgs_url(
    settings: Settings,
    name: str,
    series_id: int,
    dt_ini: date,
    dt_fim: date,
) -> SgsEndpoint:
    """Build an SGS endpoint for the given source name and date window.

    `name` must match a `_SourcesConfig` field (e.g. "bcb_cdi", "bcb_selic"),
    whose `url_template` already encodes the series id.
    """
    src = getattr(settings.pipeline.sources, name)
    template = src.url_template or ""
    url = template.format(
        dt_ini=dt_ini.strftime("%d/%m/%Y"),
        dt_fim=dt_fim.strftime("%d/%m/%Y"),
    )
    return SgsEndpoint(
        name=name,
        url=url,
        series_id=series_id,
        date_range=(dt_ini, dt_fim),
    )


def chunk_decade(
    dt_ini: date,
    dt_fim: date,
    chunk_years: int = 10,
) -> list[tuple[date, date]]:
    """Split [dt_ini, dt_fim] into inclusive non-overlapping windows of up to
    `chunk_years` years each. Final window may be shorter.

    BCB SGS rejects requests spanning more than ~10 years.
    """
    if dt_ini > dt_fim:
        return []
    out: list[tuple[date, date]] = []
    cur = dt_ini
    while cur <= dt_fim:
        try:
            jump = cur.replace(year=cur.year + chunk_years)
        except ValueError:
            # 29-Feb edge: fall back to 28-Feb
            jump = cur.replace(year=cur.year + chunk_years, day=28)
        nxt = min(jump - timedelta(days=1), dt_fim)
        out.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return out
