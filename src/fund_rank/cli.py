"""fund_rank CLI — single command that runs the full pipeline.

Usage:
    fund-rank --as-of 2025-12-31

Reads bronze (CVM + BCB + ANBIMA), builds silver/gold, writes ranking.md and
data_quality.md. Idempotent: bronze is fixed-path; re-running on the same
``--as-of`` overwrites the manifest's ``ingested_at`` and rebuilds silver/gold.
"""
from __future__ import annotations

from datetime import date

import typer

from fund_rank.obs.logging import configure_logging, get_logger
from fund_rank.settings import get_settings

app = typer.Typer(add_completion=False, help="Brazilian fixed-income fund ranking pipeline.")


@app.command()
def main(
    as_of: str = typer.Option(
        "2025-12-31", "--as-of", help="Reference date YYYY-MM-DD (calculation cutoff)."
    ),
) -> None:
    configure_logging()
    log = get_logger("fund_rank.cli")
    settings = get_settings()
    as_of_d = date.fromisoformat(as_of)

    from fund_rank.bronze import (
        ingest_anbima_175,
        ingest_anbima_indices,
        ingest_bcb_indices,
        ingest_cad_fi_hist,
        ingest_inf_diario,
        ingest_registro_classe,
    )
    from fund_rank.gold import build_fund_metrics, build_ranking_report, build_validacao
    from fund_rank.silver import (
        build_class_funds,
        build_class_funds_fixed_income,
        build_class_funds_fixed_income_treated,
        build_index_series,
        build_quota_series,
        build_quota_series_fixed_income,
        build_subclass_funds,
        build_subclass_funds_fixed_income,
        build_subclass_funds_fixed_income_treated,
    )
    from fund_rank.silver._quality_report import write_consolidated_quality_report
    from fund_rank.sources.http import make_client

    log.info("pipeline.start", as_of=as_of_d.isoformat())

    with make_client(
        timeout_seconds=settings.pipeline.http.timeout_seconds,
        user_agent=settings.pipeline.http.user_agent,
    ) as client:
        ingest_cad_fi_hist.run(settings, client)
        ingest_registro_classe.run(settings, client)
        ingest_bcb_indices.run(settings, client, as_of=as_of_d)
        ingest_inf_diario.run(settings, client, as_of=as_of_d)
    ingest_anbima_indices.run(settings)
    ingest_anbima_175.run(settings)

    build_class_funds.run(settings, as_of_d)
    build_subclass_funds.run(settings, as_of_d)
    build_class_funds_fixed_income.run(settings, as_of_d)
    build_subclass_funds_fixed_income.run(settings, as_of_d)
    build_class_funds_fixed_income_treated.run(settings, as_of_d)
    build_subclass_funds_fixed_income_treated.run(settings, as_of_d)
    build_quota_series.run(settings, as_of_d)
    build_quota_series_fixed_income.run(settings, as_of_d)
    build_index_series.run(settings, as_of_d)

    build_fund_metrics.run(settings, as_of_d)
    build_validacao.run(settings, as_of_d)
    build_ranking_report.run(settings, as_of_d)

    write_consolidated_quality_report(as_of_d, settings)
    log.info("pipeline.done", as_of=as_of_d.isoformat())


if __name__ == "__main__":
    app()
