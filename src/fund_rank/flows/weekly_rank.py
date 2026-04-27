"""Prefect 3.x flow — weekly silver+gold+rank+report build.

Schedule (suggested cron, Monday at 07:00 BRT):
    0 10 * * 1   # 07:00 BRT == 10:00 UTC

`as_of` defaults to the last completed business-day end-of-month. Pass an explicit
date to backfill.

Run locally:
    python -m fund_rank.flows.weekly_rank
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from prefect import flow, task
from prefect.logging import get_run_logger

from fund_rank.gold import compute_metrics
from fund_rank.rank import report as rank_report
from fund_rank.rank import score as rank_score
from fund_rank.settings import Settings, get_settings
from fund_rank.silver import build_funds, build_quota_series, build_universe


@task
def t_silver_funds(settings: Settings, as_of: date) -> Path:
    return build_funds.run(settings, as_of)


@task
def t_silver_quota_series(settings: Settings, as_of: date) -> Path:
    return build_quota_series.run(settings, as_of)


@task
def t_silver_universe(settings: Settings, as_of: date) -> dict[str, Path]:
    return build_universe.run(settings, as_of)


@task
def t_gold_metrics(settings: Settings, as_of: date) -> dict[str, Path]:
    return compute_metrics.run(settings, as_of)


@task
def t_rank(settings: Settings, as_of: date) -> dict[str, Path]:
    return rank_score.run(settings, as_of)


@task
def t_report(settings: Settings, as_of: date) -> Path:
    return rank_report.run(settings, as_of)


def _last_business_day_of_completed_month(today: date) -> date:
    """Compute as_of = last business day of the *previous* completed month.

    Production-safe: never returns a date in the current month, since CVM
    corrections within the month would shift the ranking.
    """
    # First day of current month
    first_of_current = today.replace(day=1)
    # Step back one day to land in the previous month
    last_day_prev = first_of_current.replace(day=1)
    last_day_prev = (
        first_of_current.replace(day=1)
    )
    # Walk back day-by-day to land on a Mon-Fri (cheap heuristic; doesn't
    # account for Brazilian holidays, but those are also non-publishing days
    # in CVM, so the last published file in the prev month is the right pick).
    # We'll instead just ask CVM for the last weekday of prev month:
    from dateutil.relativedelta import relativedelta

    last_of_prev = first_of_current - relativedelta(days=1)
    while last_of_prev.weekday() >= 5:  # 5=Sat, 6=Sun
        last_of_prev -= relativedelta(days=1)
    return last_of_prev


@flow(name="fund_rank.weekly_rank")
def weekly_rank(as_of: date | None = None) -> dict[str, object]:
    log = get_run_logger()
    settings = get_settings()

    if as_of is None:
        as_of = _last_business_day_of_completed_month(date.today())
    log.info("weekly_rank.start as_of=%s", as_of.isoformat())

    funds_path = t_silver_funds.submit(settings, as_of).result()
    quota_path = t_silver_quota_series.submit(settings, as_of).result()
    universe = t_silver_universe.submit(settings, as_of).result()
    metrics = t_gold_metrics.submit(settings, as_of).result()
    ranking = t_rank.submit(settings, as_of).result()
    report = t_report.submit(settings, as_of).result()

    result = {
        "as_of": as_of.isoformat(),
        "silver_funds": str(funds_path),
        "silver_quota_series": str(quota_path),
        "silver_universe_segments": list(universe.keys()),
        "gold_metrics_segments": list(metrics.keys()),
        "ranking_segments": list(ranking.keys()),
        "report_path": str(report),
    }
    log.info("weekly_rank.done %s", result)
    return result


if __name__ == "__main__":
    weekly_rank()
