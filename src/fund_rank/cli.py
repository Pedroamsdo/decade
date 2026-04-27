"""fund_rank CLI — ingest | build | rank | report.

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
        help="Source names to skip (e.g. cvm_cda, cvm_inf_diario).",
    ),
    inf_diario_months: Optional[int] = typer.Option(
        None,
        "--inf-diario-months",
        help="Override lookback for INF_DIARIO (default from configs/pipeline.yaml).",
    ),
    cda_months: Optional[int] = typer.Option(
        None,
        "--cda-months",
        help="Override lookback for CDA.",
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
        ingest_cad_fi,
        ingest_cda,
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
        if "cvm_cda" not in skip:
            ingest_cda.run(
                settings, client, as_of=as_of_d, today=today_d, lookback_months=cda_months
            )

    log.info("ingest.done", as_of=as_of_d.isoformat())


@app.command()
def build(
    as_of: str = typer.Option(..., "--as-of"),
) -> None:
    """Build silver + gold layers (typed parquet, master/feeder graph, metrics)."""
    configure_logging()
    log = get_logger("fund_rank.cli.build")
    log.warning("build.not_implemented", as_of=as_of)
    raise typer.Exit(code=2)


@app.command()
def rank(
    as_of: str = typer.Option(..., "--as-of"),
) -> None:
    """Compute rankings into gold/ranking/."""
    configure_logging()
    log = get_logger("fund_rank.cli.rank")
    log.warning("rank.not_implemented", as_of=as_of)
    raise typer.Exit(code=2)


@app.command()
def report(
    as_of: str = typer.Option(..., "--as-of"),
) -> None:
    """Render reports/as_of=.../ranking.md."""
    configure_logging()
    log = get_logger("fund_rank.cli.report")
    log.warning("report.not_implemented", as_of=as_of)
    raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
