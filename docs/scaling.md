# Scaling

`fund_rank` is built so the same code runs from a laptop to a multi-tenant production environment. This doc lists the cost / volume thresholds and what changes at each step.

## Today (laptop)

- Silver dimension tables (`class_funds`, `subclass_funds` and their RF subsets) sit around 41 k rows total — under 50 MB Parquet zstd-compressed.
- Each `INF_DIARIO` monthly zip is ~10 MB compressed; CAD ~18 MB.
- Build (silver + gold + rank + report) over 7 years runs in **< 60 s** on an 8-core M-series Mac with 16 GB RAM.
- Bronze partitioning is additive: re-runs that hit a `304 Not Modified` write nothing.
- The full ingest of 7 years (84 monthly INF_DIARIO + 2 yearly HIST + CAD + CDI) downloads ~700 MB and finishes in ~5 minutes (network-bound).

This is the default `make all AS_OF=2025-12-31` flow. No external dependencies beyond Python 3.9 + venv.

## 10× — daily ranking, S3-backed

When the cadence becomes daily and we want history to live longer than a laptop disk:

| Change | Effort |
|---|---|
| Set `data_root: s3://decade-fund-rank-prod/data` in `configs/pipeline.yaml` | 5 min |
| Configure AWS credentials (env vars or IAM role) | 5 min |
| Run Prefect 3.x flows on a `process` work-pool inside ECS or k8s | 1 hr |

The code does **not** change: `fsspec` abstracts the filesystem, and Polars / DuckDB read/write Parquet over s3 directly.

The Prefect flows live in `src/fund_rank/flows/`:

- `daily_ingest` — cron `0 6 * * 1-5 BRT`. Tasks: download CAD, registro_classe, CDI, INF_DIARIO (current month + M-1 for late corrections). Each is idempotent (etag/sha256 check).
- `weekly_rank` — cron `0 7 * * 1 BRT`. Computes `as_of = last completed business-day end-of-month`, runs silver → gold → rank → report. Output published to `gold/` and `reports/` partitions.
- `backfill` — manual entry point that accepts `--from / --to` to reprocess gold from immutable bronze.

### Monitoring

- Prefect Cloud notifications (Slack) on flow failure.
- Data contracts (per ADR-008):
  - `cad_fi.csv` ingested_at must be ≤ 48 h old.
  - `inf_diario` row count ≥ 80 % of the 30-day rolling median (alert on drop).
  - Coverage ≥ 95 % of active RF classes have quotas in the reference month.
- Schema contracts: pydantic v2 models exported to `docs/contracts/*.json` on each build; CI fails if schema drifts without a `CONTRACT_VERSION` bump.

## 100× — distributed compute

When the universe widens (e.g., add multimercado, ações, FII, FIDC) or the cadence becomes intraday for refreshable signals, the laptop / single-process ceiling is reached.

| Change | Notes |
|---|---|
| Treat `data/silver/*.parquet` as **external tables** in BigQuery, Athena, or Snowflake | Same Parquet, no rewrite |
| Move heavy aggregations (`gold/compute_metrics`) from Polars to SQL views | DuckDB → BigQuery dialect; mostly direct translations |
| Materialize `gold/fund_metrics` via `dbt` against the warehouse | Adds dependency surface; only worth it at 100× |
| Worker pool: Prefect agents on Kubernetes Engine, autoscaling per queue depth | |

The contracts in `src/fund_rank/contracts/gold.py` (`FundMetrics`, `RankingEntry`) are the **stability layer** — downstream services consume the same schema regardless of where the build runs.

## What does not scale

- The CVM Dados Abertos web endpoint: rate-limit unknown, ~3 s per file. Parallelizing downloads helps but watch for 5xx.
- Pre-CVM 175 stitch: only as good as `registro_fundo_classe.zip` predecessor mapping. Multi-class umbrellas will always lose pre-2023 history under our policy (ADR-004) — no scaling fix.

## What we do *not* do today

- **Detection of fato relevante / suspended captures.** A fund flagged for liquidation between two daily ingests still appears until next CAD pull. Mitigated by `situacao_lookback_months`, but real-time detection would need scraping CVM RIs.
- **Tax regime modeling for `come-cotas`.** Reported in silver, not modeled in metrics.
- **Streaming ingestion.** All sources are batch (CVM publishes T+1).
