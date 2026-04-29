"""Prefect 3.x flow — daily bronze ingestion (idempotent, etag-aware).

Schedule (suggested cron, BRT business days at 06:00):
    0 9 * * 1-5   # 06:00 BRT == 09:00 UTC

Each task is independent and idempotent: a 304/sha-match yields no new partition.
A failure in one source does not block the others — partial successes are valid.

Run locally:
    python -m fund_rank.flows.daily_ingest
"""
from __future__ import annotations

from datetime import date

import httpx
from prefect import flow, task
from prefect.logging import get_run_logger

from fund_rank.bronze import (
    ingest_cdi,
    ingest_inf_diario,
    ingest_registro_classe,
)
from fund_rank.settings import Settings, get_settings
from fund_rank.sources.http import make_client


@task(retries=3, retry_delay_seconds=30)
def t_registro_classe(settings: Settings, client: httpx.Client, today: date) -> str:
    out = ingest_registro_classe.run(settings, client, today=today)
    return out.status


@task(retries=3, retry_delay_seconds=30)
def t_cdi(settings: Settings, client: httpx.Client, as_of: date, today: date) -> str:
    out = ingest_cdi.run(settings, client, as_of=as_of, today=today)
    return out.status


@task(retries=2, retry_delay_seconds=60)
def t_inf_diario(
    settings: Settings, client: httpx.Client, as_of: date, today: date, lookback_months: int = 2
) -> int:
    """Re-ingest current month + M-1 to pick up CVM corrections.

    For backfill of older months, run weekly_rank or use --inf-diario-months
    via the CLI entry point.
    """
    outs = ingest_inf_diario.run(
        settings, client, as_of=as_of, today=today, lookback_months=lookback_months
    )
    return sum(1 for o in outs if o.status == "fetched")


@flow(name="fund_rank.daily_ingest")
def daily_ingest(as_of: date | None = None, today: date | None = None) -> dict[str, object]:
    """Run all bronze ingestors. Use today's date if not overridden."""
    log = get_run_logger()
    settings = get_settings()
    today = today or date.today()
    as_of = as_of or today
    log.info("daily_ingest.start as_of=%s today=%s", as_of.isoformat(), today.isoformat())

    with make_client(
        timeout_seconds=settings.pipeline.http.timeout_seconds,
        user_agent=settings.pipeline.http.user_agent,
    ) as client:
        rc_status = t_registro_classe.submit(settings, client, today)
        cdi_status = t_cdi.submit(settings, client, as_of, today)
        inf_count = t_inf_diario.submit(settings, client, as_of, today)

        result = {
            "registro_classe": rc_status.result(),
            "bcb_cdi": cdi_status.result(),
            "inf_diario_fetched": inf_count.result(),
        }
    log.info("daily_ingest.done %s", result)
    return result


if __name__ == "__main__":
    daily_ingest()
