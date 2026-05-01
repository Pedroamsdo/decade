# Scaling

`fund_rank` is built so the same code runs from a laptop to a multi-tenant production environment. This doc lists the cost / volume thresholds and what changes at each step.

## Today (laptop)

- Silver dimension tables (`class_funds`, `subclass_funds` and their RF subsets) sit around 41 k rows total — under 50 MB Parquet zstd-compressed.
- Each `INF_DIARIO` monthly zip is ~10 MB compressed; CAD ~18 MB.
- Build (silver + gold + rank + report) over 7 years runs in **~2 min** on an 8-core M-series Mac with 16 GB RAM (excluding bronze re-fetch).
- Bronze is canonical (single location per source); re-runs use sha256 to short-circuit unchanged files (ADR-008).
- The full ingest of 7 years (84 monthly INF_DIARIO + 2 yearly HIST + CAD + CDI + 6 ANBIMA index XLS drops) downloads ~1.5 GB and finishes in ~30–60 minutes on first run (network-bound).

This is the default `fund-rank --as-of 2025-12-31` flow. No external dependencies beyond Python 3.9 + venv. No CI, no scheduler, no orchestrator — see [ADR-012](decisions.md#adr-012--local-make-reproduce-as-the-canonical-run-path-no-ci-no-orchestrator-in-v1) for why this is the right v1 surface.

## 10× — daily ranking, S3-backed

When the cadence becomes daily and we want history to live longer than a laptop disk:

| Change | Effort |
|---|---|
| Set `data_root: s3://decade-fund-rank-prod/data` in `configs/pipeline.yaml` | 5 min |
| Configure AWS credentials (env vars or IAM role) | 5 min |
| Schedule `fund-rank --as-of <date>` on a worker (ECS / cron / Prefect / Airflow / GitHub Actions) | 1 hr |

The code does **not** change: `fsspec` abstracts the filesystem, and Polars / DuckDB read/write Parquet over s3 directly. The CLI entrypoint (`fund-rank`) is single-shot and idempotent — easy to wrap in any scheduler. Picking the scheduler is a deployment decision (cost, team familiarity, existing infra), not an architecture decision; ADR-012 explains why we don't pre-pick one in v1.

Suggested cadence:
- **daily** (06:00 BRT, M-F) — refetch CAD, registro_classe, CDI, INF_DIARIO (current month + M-1 for late corrections). Each source is idempotent via etag/sha256 (ADR-008).
- **weekly** (07:00 BRT, Mon) — pick `as_of = last completed end-of-month` and run the full pipeline; publish `gold/` + `ranking.md` + `reports/`.
- **backfill** — same CLI, called once per historical `as_of` from immutable bronze.

### Monitoring

- Alert on CLI exit code != 0.
- Data contract checks live in `silver/_quality_report.py` and feed the consolidated `reports/as_of=YYYY-MM-DD/data_quality.md`. Hook these into the scheduler:
  - `cad_fi.csv` ingested_at must be ≤ 48 h old.
  - `inf_diario` row count ≥ 80 % of the 30-day rolling median (alert on drop).
  - Coverage ≥ 95 % of active RF classes have quotas in the reference month.
- Schema contracts: pydantic v2 models in `src/fund_rank/contracts/` (currently `class_funds.py`, `subclass_funds.py`) — extend with a gold contract before exposing to downstream consumers.

## 100× — distributed compute

When the universe widens (e.g., add multimercado, ações, FII, FIDC) or the cadence becomes intraday for refreshable signals, the laptop / single-process ceiling is reached.

| Change | Notes |
|---|---|
| Treat `data/silver/*.parquet` as **external tables** in BigQuery, Athena, or Snowflake | Same Parquet, no rewrite |
| Move heavy aggregations (`gold/build_fund_metrics`) from Polars to SQL views | DuckDB → BigQuery dialect; mostly direct translations |
| Materialize `gold/fund_metrics` via `dbt` against the warehouse | Adds dependency surface; only worth it at 100× |
| Worker pool: Prefect / Airflow agents on Kubernetes, autoscaling per queue depth | |

The contracts in `src/fund_rank/contracts/` (currently silver dimensions) should grow a `gold.py` with `FundMetrics` + `RankingEntry` schemas before opening the gold tables to external consumers — those become the **stability layer** so downstream services consume the same schema regardless of where the build runs.

## What does not scale

- The CVM Dados Abertos web endpoint: rate-limit unknown, ~3 s per file. Parallelizing downloads helps but watch for 5xx.
- Pre-CVM 175 stitch: only as good as `registro_fundo_classe.zip` predecessor mapping. Multi-class umbrellas will always lose pre-2023 history under our policy (ADR-004) — no scaling fix.

## What we do *not* do today

- **Detection of fato relevante / suspended captures.** A fund flagged for liquidation between two daily ingests still appears until next CAD pull. Mitigated by `situacao_lookback_months`, but real-time detection would need scraping CVM RIs.
- **Tax regime modeling for `come-cotas`.** Reported in silver, not modeled in metrics.
- **Streaming ingestion.** All sources are batch (CVM publishes T+1).
