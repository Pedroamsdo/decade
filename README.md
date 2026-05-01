# fund_rank — Brazilian Fixed-Income Fund Ranking

Ranks Brazilian fixed-income funds against a configurable reference date and produces a Top-5 list per investor profile (`publico_alvo`: Público Geral / Qualificado / Profissional).

Built for the **Decade PS** take-home case. Sources: CVM Dados Abertos, BCB SGS, ANBIMA dropped XLS files.

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

fund-rank --as-of 2025-12-31
```

That single command runs the entire pipeline (bronze ingest → silver build → gold rank → quality report). On first run, expect ~30–60 minutes of CVM downloads (~1.5 GB). Re-runs are fast (etag/sha-aware bronze).

To pin to a different reference date: `fund-rank --as-of 2024-12-31`.

If you upgraded from the partitioned-bronze version, run `rm -rf data/bronze/` once before the next call so the canonical layout (without `ingested_at=…` directories) is rebuilt fresh.

## Outputs

- `ranking.md` — Top-5 per investor profile.
- `data/bronze/{source}[/competence=…]/raw.{ext}` + `_manifest.json` — canonical bronze (one location per source; the manifest's `ingested_at` records the run date).
- `data/silver/...` — typed parquet (class/subclass/quota/index series + RF subsets + treated).
- `data/gold/fund_metrics/`, `data/gold/validacao/` — per-fund metrics + score 0–100.
- `reports/as_of=YYYY-MM-DD/data_quality.md` — single consolidated quality report covering every silver and gold table.

## Score recipe (high level)

Composite of **three metrics**, aligned with the CFA L3 framework for fixed-income fund selection. Two measure risk-adjusted return vs the fund's canonical benchmark (CDI / IPCA / IMA-B / etc., mapped in `silver/_benchmark_mapping.py`); the third applies a deterministic haircut for the after-tax return the investor actually pockets at redemption.

```
excess[t]        = monthly_ret_fund[t] − monthly_ret_bench[t]

IR_anualizado    = mean(excess) / std(excess) × √12                  # weight 0.60
Sortino_anual    = mean(excess) × 12 / (std(min(excess, 0)) × √12)   # weight 0.25
tax_efficiency   = 1 − effective_ir_rate(tributacao_alvo)            # weight 0.15

composite        = 0.60 × z(IR) + 0.25 × z(Sortino) + 0.15 × z(tax_efficiency)
                   # z-scores over the eligible universe
score            = percentile_rank(composite) × 100
```

**Why three metrics.**
- **Information Ratio (0.60)** — consistency of active return. CFA-standard for active management.
- **Sortino Ratio (0.25)** — penalizes only negative excess returns, capturing the asymmetric tail risk that dominates fixed-income return distributions (credit events, duration shocks). Fills the gap left by IR's symmetric tracking error.
- **Tax efficiency (0.15)** — the redemption-time IR rate is deterministic per `tributacao_alvo` bucket (Isento → 0%, Longo Prazo → 15%, Curto Prazo → 20%, Previdenciário → 10%, …) and configurable in `scoring.yaml#tax`. Two funds with identical IR + Sortino but different tax buckets should not tie — the investor pockets different net returns.

The 60/25/15 split keeps risk-adjusted alpha as the primary thesis (85%), with tax efficiency as a deterministic modifier (15%, not enough to dominate the ranking but enough to break ties between similar funds). All weights and metrics are config-driven in `configs/scoring.yaml`.

Eligibility: `situacao = "Em Funcionamento Normal"`, `nr_cotst > 1,000`, `existing_time ≥ 252` dias, `equity ≥ R$ 50 M`. Funds outside the criteria get `score = null`. Funds with `tributacao_alvo` mapped to `null` (Não Aplicável / Outros / Indefinido) also get `score = null` — the tax bucket is required to evaluate the composite.

See `docs/methodology.md` and `docs/data_contracts.md` for the full breakdown.

## Tests

```bash
.venv/bin/python -m pytest
```

## Orchestration & scaling

The reproduction path is **local**: `fund-rank --as-of …` from your shell. There is no CI, no scheduler, no managed orchestrator on the reproduction path. An earlier version of the repo ran the same CLI in GitHub Actions (commit `8fcc367`) — it was removed in `628252e` once it became clear that the binding constraint is the ~1.5 GB / 30–60 min CVM download, not compute, and that coupling reproduction to a CI identity adds friction without analytical value for a take-home.

Dagster / Airflow / Prefect-deployed were considered and rejected for v1 — see [ADR-012](docs/decisions.md#adr-012--local-make-reproduce-as-the-canonical-run-path-no-ci-no-orchestrator-in-v1) for the full argument and [docs/scaling.md](docs/scaling.md) for the laptop → S3 → distributed migration path. Short version: the CLI is single-shot and idempotent (ADR-008), so wrapping it in *any* scheduler later is a deployment decision, not an architecture change.

## Layout

```
src/fund_rank/
  cli.py        # single entrypoint: fund-rank --as-of YYYY-MM-DD
  sources/      # HTTP + CVM + BCB endpoint builders (etag-aware)
  bronze/       # raw 1:1 ingestion with canonical paths + manifest sidecars
  silver/       # typed parquet, CVM 175 stitch, RF subsets, treated tables
  gold/         # fund_metrics + validacao + ranking_report
  contracts/    # Pydantic v2 models
configs/        # YAML config (pipeline / scoring / benchmarks)
tests/          # 45 unit + integration tests
docs/
```

## Data sources

| Source | URL | Granularity |
|---|---|---|
| CVM CAD hist | `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi_hist.zip` | snapshot zip with taxa_adm/taxa_perfm/rentab |
| CVM Registro Classe | `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip` | snapshot, T+1 |
| CVM INF_DIARIO | `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_YYYYMM.zip` | monthly zip, T+1 |
| ANBIMA Fundos 175 | `https://data.anbima.com.br/datasets/fundos-175-caracteristicas-publico/detalhes` (manual XLS drop in `data/bronze/anbima_175/dropped/`) | snapshot |
| BCB SGS | `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{12,11,433,188,189}/dados` | daily/monthly per series |
