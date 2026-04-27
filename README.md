# fund_rank — Brazilian Fixed-Income Fund Ranking

Pipeline that ranks Brazilian fixed-income funds against a configurable reference date and produces a top-5 list per customer profile (Caixa / RF Geral / Qualificado).

Built for the **Decade PS** take-home case, sourcing data exclusively from public CVM Dados Abertos and BCB SGS. ANBIMA proprietary feeds are not required.

## For reviewers

- **[ranking.md](ranking.md)** — top 5 per segment for `as_of = 2025-12-31`, with the per-fund metrics that drove the rank.
- **[docs/methodology.md](docs/methodology.md)** — what each metric means, the segment weights, and the scoring formula.
- **[docs/decisions.md](docs/decisions.md)** — ADRs for the non-obvious choices (CNPJ_Classe ranking, feeders-not-masters, CVM 175 stitch, intra-segment z-scores, etc.).
- **[docs/scaling.md](docs/scaling.md)** — laptop → S3 → distributed migration path; the production cron is in `src/fund_rank/flows/`.

## Quickstart

```bash
# 1. Create venv with system Python 3.9+
python3 -m venv .venv
source .venv/bin/activate

# 2. Install
pip install -e ".[dev]"

# 3. End-to-end run against December 2025
make all AS_OF=2025-12-31

# Outputs:
#   data/bronze/...   # raw downloads, idempotent
#   data/silver/...   # typed parquet
#   data/gold/...     # fund_metrics + ranking parquet
#   reports/as_of=2025-12-31/ranking.md
```

Each stage is invocable independently:

```bash
make ingest AS_OF=2025-12-31
make build  AS_OF=2025-12-31
make rank   AS_OF=2025-12-31
make report AS_OF=2025-12-31
```

`AS_OF` is the **reference date** of the ranking — not the execution date — so re-running with the same `AS_OF` is idempotent.

The default lookback is **10 years** of `INF_DIARIO` history (with automatic fallback to CVM's yearly HIST zips for years older than the monthly retention window). On first run, expect ~700 MB of bronze downloads taking ~5 minutes; subsequent runs are no-ops where source content is unchanged (etag + sha256 idempotency).

To run with a smaller window for quick iteration:
```bash
.venv/bin/python -m fund_rank.cli ingest --as-of 2025-12-31 --inf-diario-months 14 --cdi-years 5
```

## Tests

```bash
make test
# or: .venv/bin/python -m pytest
```

27 unit tests cover normalization, CNPJ cleaning, scoring (z-score + direction handling), drawdown / annualization, jump detection, segment classification, and weights-sum-to-1.0 invariants.

## Layout

```
src/fund_rank/
  sources/      # HTTP + CVM + BCB downloaders (etag-aware)
  bronze/       # raw 1:1 ingestion with manifest sidecars
  silver/       # typed parquet, CVM 175 stitch, master/feeder graph
  gold/         # metrics + benchmarks
  rank/         # scoring + selection + report rendering
  flows/        # Prefect 3.x daily/weekly orchestration
  contracts/    # Pydantic v2 models (external contract under semver)
configs/        # YAML config per concern
tests/
docs/
```

See [docs/methodology.md](docs/methodology.md) for the metric definitions and per-segment weights.

## Data sources

| Source | URL | Granularity |
|---|---|---|
| CVM CAD | `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv` | snapshot, T+1 |
| CVM Registro Classe | `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip` | snapshot, T+1 |
| CVM INF_DIARIO | `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_YYYYMM.zip` | daily (zip mensal), T+1 |
| CVM CDA | `https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/cda_fi_YYYYMM.zip` | mensal, T+30 |
| BCB SGS série 12 | `https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json` | diária, T+1 |

## Production path

`flows/daily_ingest.py` and `flows/weekly_rank.py` are Prefect 3.x flows. See [docs/scaling.md](docs/scaling.md) for the local → S3 → distributed migration path. The bronze layer is additive and idempotent (etag check) — backfills are safe.
