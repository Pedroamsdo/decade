# fund_rank — Brazilian Fixed-Income Fund Ranking

Pipeline that ranks Brazilian fixed-income funds against a configurable reference date and produces a Top-10 list per investor profile (`publico_alvo`: Público Geral / Qualificado / Profissional).

Built for the **Decade PS** take-home case, sourcing data exclusively from public CVM Dados Abertos, BCB SGS and ANBIMA dropped XLS files.

## For reviewers

- **[ranking.md](ranking.md)** — Top-10 per profile for `as_of = 2025-12-31`, filtered to `situacao = "Em Funcionamento Normal"`, with per-fund metrics driving the rank.
- **[docs/data_contracts.md](docs/data_contracts.md)** — full schema of the silver and gold layers, including the `gold/fund_metrics` and `gold/ranking` tables.
- **[docs/methodology.md](docs/methodology.md)** — original metric definitions (the active scoring is described directly in `gold/build_ranking.py` and surfaced in `ranking.md`).
- **[docs/decisions.md](docs/decisions.md)** — ADRs for non-obvious choices (CVM 175 stitch, jump detection, _treated tables, no-IR scoring, etc.).
- **[docs/scaling.md](docs/scaling.md)** — laptop → S3 → distributed migration path; production cron in `src/fund_rank/flows/`.

## Score recipe (high level)

- **Numerator (`retorno_score`):** `hit_rate vs benchmark` + `1 − σ(Sharpe rolling 12m)` + `liquid_return_12m`. Each column is clipped at ±3σ, normalized 0–1, summed and re-normalized.
- **Denominator (`risco_score`):** geometric mean of three subgroups (Qualidade do veículo with PL/idade/captação inverted, Liquidez with ANBIMA risk weight + prazo de resgate, Volatilidade with σ anualizado + |max drawdown|), then re-normalized 0–1.
- **Score:** `score = minmax(retorno_score / (risco_score + 0.01)) * 100`. Range 0–100, sorted descending.

The full breakdown — null handling, outlier clipping, ANBIMA risk weights — is documented in `docs/data_contracts.md` (section "Gold layer").

## Quickstart

The repository is **clone-and-run**: the ANBIMA XLS files (which the source portal gates behind reCAPTCHA + paid OAuth) are committed under `data/bronze/anbima_*/dropped/`, so a fresh clone has every input the pipeline needs.

### One-command reproduction

```bash
git clone git@github.com:Pedroamsdo/decade.git
cd decade
make reproduce        # ~30–60 min on first run (CVM bronze download)
                      # ~5 min subsequent runs (ETag/sha256 idempotency)
```

`make reproduce` chains `setup` (creates `.venv`, installs deps) and `all` (`ingest + build + rank`), regenerating `ranking.md` for `AS_OF=2025-12-31` (the case-study cutoff).

For another reference date: `make reproduce AS_OF=2024-12-31`.

### Manual setup

```bash
# 1. Create venv with system Python 3.9+ (3.11 recommended)
python3 -m venv .venv
source .venv/bin/activate

# 2. Install
pip install -e ".[dev]"

# 3. End-to-end run against the case-study reference date
make all AS_OF=2025-12-31
```

Outputs:
- `data/bronze/...` — raw downloads, idempotent
- `data/silver/...` — typed parquet (class/subclass/quota/index series)
- `data/gold/fund_metrics/`, `data/gold/ranking/` — per-fund metrics + score 0–100
- `reports/as_of=2025-12-31/*_quality.md` — null + range reports per silver/gold table
- `ranking.md` — human-readable top-10 by profile

Each stage is invocable independently:

```bash
make ingest AS_OF=2025-12-31  # bronze (idempotent, etag-aware)
make build  AS_OF=2025-12-31  # silver
make rank   AS_OF=2025-12-31  # gold/fund_metrics + gold/ranking + ranking.md
```

`AS_OF` is the **reference date** of the ranking — not the execution date — so re-running with the same `AS_OF` is idempotent.

### `AS_OF` vs `INGEST_UNTIL`

- **`AS_OF`** (default `2025-12-31`) is the calculation cutoff: silver/gold filter `dt <= AS_OF` so metrics are deterministic for that reference date.
- **`INGEST_UNTIL`** (default `today`) bounds the bronze download window. Bronze can hold data beyond `AS_OF` without polluting downstream — silver/gold filter it out.

Because they're decoupled, you can keep bronze fresh (download new months as the CVM publishes them) while reproducing any historical `AS_OF` from the same dataset:

```bash
make ingest INGEST_UNTIL=2026-05-01     # bronze through 2026-04
make build rank AS_OF=2025-12-31         # ranking pinned to case-study date
make build rank AS_OF=2024-12-31         # ranking pinned to year-end 2024 (no re-ingest)
```

The default lookback is **30 years** of `INF_DIARIO` history (with automatic fallback to CVM's yearly HIST zips for years older than the monthly retention window). On first run, expect ~1.5 GB of CVM bronze downloads taking ~30–60 minutes; subsequent runs are no-ops where source content is unchanged (etag + sha256 idempotency).

To run with a smaller window for quick iteration:
```bash
.venv/bin/python -m fund_rank.cli ingest --as-of 2025-12-31 --inf-diario-months 14 --index-years 5
```

## Refreshing data

The pipeline is **fully local**. To keep `ranking.md` current as the CVM publishes new monthly `INF_DIARIO` zips:

```bash
make all                    # uses default AS_OF=2025-12-31, INGEST_UNTIL=today
git add ranking.md && git commit -m "refresh ranking" && git push
```

Bronze is incremental via ETag + sha256 (`src/fund_rank/bronze/_common.py`), so re-runs only download what changed.

**Refreshing ANBIMA**: when the portal updates IMA-B/IRF-M/Fundos 175, download the XLS, overwrite under `data/bronze/anbima_*/dropped/`, and re-run `make all`.

## Tests

```bash
make test
# or: .venv/bin/python -m pytest
```

68 unit tests cover normalization, CNPJ cleaning, taxa imputation, benchmark mapping, jump detection, drawdown / annualization, scoring helpers (clip 3σ, minmax, geometric mean, pipeline directions), and end-of-month aggregation.

## Layout

```
src/fund_rank/
  sources/      # HTTP + CVM + BCB downloaders (etag-aware)
  bronze/       # raw 1:1 ingestion with manifest sidecars
  silver/       # typed parquet, CVM 175 stitch, _treated tables
  gold/         # fund_metrics + ranking + ranking.md report
  flows/        # Prefect 3.x daily/weekly orchestration
  contracts/    # Pydantic v2 models (external contract under semver)
configs/        # YAML config per concern (pipeline / universe / scoring / benchmarks)
tests/
docs/
```

See [docs/methodology.md](docs/methodology.md) for the metric definitions and per-segment weights.

## Data sources

| Source | URL | Granularity |
|---|---|---|
| CVM CAD hist | `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi_hist.zip` | zip com `cad_fi_hist_taxa_adm.csv`, `cad_fi_hist_taxa_perfm.csv`, `cad_fi_hist_rentab.csv` (e outros) |
| CVM Registro Classe | `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip` | snapshot, T+1 |
| CVM INF_DIARIO | `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_YYYYMM.zip` | daily (zip mensal), T+1 |
| ANBIMA Fundos 175 | `https://data.anbima.com.br/datasets/fundos-175-caracteristicas-publico/detalhes` (xlsx, drop manual em `data/bronze/anbima_175/dropped/`) | snapshot |
| BCB SGS série 12 | `https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json` | diária, T+1 |

## Production path

For local orchestration, `flows/daily_ingest.py` is a Prefect 3.x flow covering the most-mutating subset (`registro_classe`, BCB CDI, `inf_diario`). The default reproduction path is `make reproduce` on the user's own machine — no CI infrastructure required, no maintainer in the loop. See [docs/scaling.md](docs/scaling.md) for the local → S3 → distributed migration path. The bronze layer is additive and idempotent (etag check), so backfills are safe.
