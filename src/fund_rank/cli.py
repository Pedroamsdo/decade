"""fund_rank CLI — ingest | build.

Each subcommand accepts --as-of=YYYY-MM-DD; pipelines are deterministic
for a given as_of (idempotent, replay-safe).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

import typer

from fund_rank.obs.logging import configure_logging, get_logger
from fund_rank.settings import get_settings

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Brazilian fixed-income fund ranking pipeline.",
)


def _parse_as_of(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


@app.command()
def ingest(
    as_of: str = typer.Option(..., "--as-of", help="Reference date YYYY-MM-DD."),
    today: Optional[str] = typer.Option(
        None, "--today", help="Override today's date (for replay/testing)."
    ),
    skip: List[str] = typer.Option(
        [],
        "--skip",
        help="Source names to skip (e.g. cvm_inf_diario, bcb_cdi).",
    ),
    inf_diario_months: Optional[int] = typer.Option(
        None,
        "--inf-diario-months",
        help="Override lookback for INF_DIARIO (default from configs/pipeline.yaml).",
    ),
    cdi_years: Optional[int] = typer.Option(
        None,
        "--cdi-years",
        help="Override lookback for CDI.",
    ),
) -> None:
    """Run bronze-layer ingestion (idempotent, etag-aware)."""
    configure_logging()
    log = get_logger("fund_rank.cli.ingest")
    settings = get_settings()
    as_of_d = _parse_as_of(as_of)
    today_d = _parse_as_of(today) if today else date.today()

    from fund_rank.bronze import (
        ingest_anbima_175,
        ingest_cad_fi,
        ingest_cad_fi_hist,
        ingest_cdi,
        ingest_inf_diario,
        ingest_registro_classe,
    )
    from fund_rank.sources.http import make_client

    log.info("ingest.start", as_of=as_of_d.isoformat(), today=today_d.isoformat(), skip=list(skip))

    with make_client(
        timeout_seconds=settings.pipeline.http.timeout_seconds,
        user_agent=settings.pipeline.http.user_agent,
    ) as client:
        if "cvm_cad_fi" not in skip:
            ingest_cad_fi.run(settings, client, today=today_d)
        if "cvm_cad_fi_hist" not in skip:
            ingest_cad_fi_hist.run(settings, client, today=today_d)
        if "cvm_registro_classe" not in skip:
            ingest_registro_classe.run(settings, client, today=today_d)
        if "bcb_cdi" not in skip:
            ingest_cdi.run(
                settings, client, as_of=as_of_d, today=today_d, lookback_years=cdi_years
            )
        if "cvm_inf_diario" not in skip:
            ingest_inf_diario.run(
                settings, client, as_of=as_of_d, today=today_d, lookback_months=inf_diario_months
            )

    if "anbima_175" not in skip:
        ingest_anbima_175.run(settings, today=today_d)

    log.info("ingest.done", as_of=as_of_d.isoformat())


@app.command()
def build(
    as_of: str = typer.Option(..., "--as-of"),
) -> None:
    """Build silver layer: class_funds + subclass_funds (+ RF subsets)."""
    configure_logging()
    log = get_logger("fund_rank.cli.build")
    settings = get_settings()
    as_of_d = _parse_as_of(as_of)

    from fund_rank.silver import (
        build_class_funds,
        build_class_funds_fixed_income,
        build_quota_series,
        build_subclass_funds,
        build_subclass_funds_fixed_income,
    )

    log.info("build.start", as_of=as_of_d.isoformat())
    build_class_funds.run(settings, as_of_d)
    build_subclass_funds.run(settings, as_of_d)
    build_class_funds_fixed_income.run(settings, as_of_d)
    build_subclass_funds_fixed_income.run(settings, as_of_d)
    build_quota_series.run(settings, as_of_d)
    log.info("build.done", as_of=as_of_d.isoformat())


if __name__ == "__main__":
    app()
