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

Single metric: **Information Ratio (IR) anualizado** vs the fund's canonical benchmark (CDI / IPCA / IMA-B / etc., mapped in `silver/_benchmark_mapping.py`).

```
excess[t]      = monthly_ret_fund[t] − monthly_ret_bench[t]
IR_anualizado  = mean(excess) / std(excess) × √12
score          = percentile_rank(IR over eligible) × 100
```

Eligibility: `situacao = "Em Funcionamento Normal"`, `nr_cotst > 1,000`, `existing_time ≥ 252` dias, `equity ≥ R$ 50 M`. Funds outside the criteria get `score = null`.

See `docs/methodology.md` and `docs/data_contracts.md` for the full breakdown.

## Tests

```bash
.venv/bin/python -m pytest
```

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
