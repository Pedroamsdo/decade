# Methodology

This document specifies how `fund_rank` builds its top-5 fixed-income fund ranking. The pipeline operates in four stages — `ingest` → `build` → `rank` → `report` — each deterministic for a given `--as-of` reference date.

## 1. Universe

We start from every regulated fund-class registered with CVM that publishes a daily quota in `INF_DIARIO`. The structural unit ranked is the **classe** (`CNPJ_Classe`), not the umbrella fund (`CNPJ_Fundo`), because:

- Post-CVM Resolution 175 (Oct/2023), `INF_DIARIO` reports daily quotas at the *classe* level.
- A single umbrella fund may host classes with different fees, public, and policies. Ranking the umbrella conflates them.
- Each `CNPJ_Classe` is what a downstream system sells to investors.

### Global filters (applied before segmentation)

| Filter | Threshold | Source |
|---|---|---|
| `situacao` | "Em Funcionamento Normal" | `registro_classe.csv#Situacao` |
| `condominio` | "Aberto" | `registro_classe.csv#Forma_Condominio` |
| `exclusivo` | "N" (not exclusive) | `registro_classe.csv#Exclusivo` |
| `historico` | ≥ 252 business days of quota series | `INF_DIARIO` (with CVM-175 stitch) |
| `jump_flag` | exclude rows with \|z-score(daily log return)\| > 5σ in 60d window | derived |

Cotization/payment-window filters (`D+0`, `D+1`, etc.) are configured but inactive in v1: those fields require ingesting `extrato_fi_YYYYMM.zip` (CVM monthly extract), not part of the v1 minimal source set.

### CVM 175 stitch

For every fundo guarda-chuva that adapted to CVM Resolution 175 between 2023 and 2025, the legacy `CNPJ_FUNDO` reported pre-adaptation is mapped to the new `CNPJ_Classe` whenever there is exactly one classe per fundo. The history thus stitched is flagged `history_source = "stitched_cvm175"`. Multi-class umbrellas are not stitched (`history_source = "orphan_pre_cvm175"`) and lose pre-adaptation history.

## 2. Segments

Three customer profiles ranked separately. A class belongs to **at most one** segment, determined by ANBIMA classification (`Classificacao_Anbima` from `registro_classe.csv`) plus public-target filter (`Publico_Alvo`).

| Segment | Profile | ANBIMA prefixes (after accent-strip + lowercase) | Public | PL min | Cotistas min |
|---|---|---|---|---|---|
| **caixa** | Reserve-of-emergency, retail | `renda fixa simples`, `renda fixa duracao baixa soberano`, `renda fixa duracao baixa grau de invest` | Público Geral | R$ 50 mi | 100 |
| **rfgeral** | Strategic retail bucket | `renda fixa duracao media/alta/livre …`, `renda fixa indexados`, `renda fixa duracao baixa credito` | Público Geral | R$ 50 mi | 100 |
| **qualificado** | Qualified / professional, credit-tilted | `renda fixa duracao … credito`, `renda fixa divida externa`, `renda fixa invest. no exterior` | Qualificado / Profissional | R$ 30 mi | 5 |

Why three? A single ranking would let long-duration `Indexados` funds (high Sharpe in falling-rate regimes) dominate over short-duration referenciado-DI funds — the right answer for someone who wants reserve-of-cash is a different fund than someone willing to ride duration.

## 3. Metrics

Calculated against `as_of = 2025-12-31` over **business days** (252-day annualization). Source: `vl_quota` from CVM `INF_DIARIO`, which is **already net** of administration and realized performance fees — costs are not deducted again.

| Metric | Formula | Window |
|---|---|---|
| `retorno_acum_W` | `vl_quota[end] / vl_quota[start] − 1` | 12M / 24M / 36M |
| `retorno_anualizado_W` | `(1 + retorno_acum) ^ (252/N) − 1` | 12M / 36M |
| `pct_cdi_W` | `retorno_acum / (cdi_cum − 1)` | 12M / 24M / 36M |
| `vol_anualizada_W` | `std(log_returns) · sqrt(252)` | 12M / 36M |
| `max_drawdown_36m` | `min(cum_factor / running_max − 1)` over 36M | 36M |
| `drawdown_duration_days_36m` | days between peak and trough during the worst DD | 36M |
| `tracking_error_cdi_12m` | `std(fund_arith − cdi_arith) · sqrt(252)` | 12M |
| `sharpe_12m` | `mean(daily excess) · 252 / (vol × sqrt(252))` | 12M |
| `sortino_24m` | `mean(daily excess) · 252 / (downside_dev × sqrt(252))` | 24M |
| `info_ratio_12m` | `(annualized fund − annualized cdi) / tracking_error` | 12M |
| `excesso_retorno_24m` | `retorno_acum_24m − (cdi_cum − 1)` | 24M |
| `pl_mediano_12m` | median of daily `vl_patrim_liq` over the last 12M | 12M |
| `retorno_<event>` | total return during a named stress window | event-bound |
| `history_dias_uteis` | count of quota days available (after stitch) | all-time |

Stress events configured in v1:

- `covid_2020`: 2020-03-01 to 2020-04-30
- `marola_credito_2024`: 2024-05-01 to 2024-07-31

## 4. Scoring

For each segment we compute a per-fund score from a weighted z-score of metrics — z-scores are taken **within the segment**, never globally. Direction is encoded per metric (`positive` → higher is better; `negative` → invert sign).

```
z(m, fund) = (value(m, fund) − median(m, segment)) / (std(m, segment) + 1e-12)
score(fund) = Σ_m  weight(m, segment) × sign(direction(m)) × z(m, fund)
```

Weights are in `configs/scoring.yaml`. Top-N (default 5) is selected by descending score, with tie-breakers in `selection.tiebreak_columns`. Within a segment, weights sum to **1.00**.

Why z-scores intra-segment, not raw values? Because metrics like `tracking_error_cdi_12m` have very different scales for Caixa (basis points) versus Qualificado credit funds (whole percent). Cross-class normalization makes the weighted sum comparable while preserving direction.

## 5. Output contracts

`gold.fund_metrics` and `gold.RankingEntry` are versioned via `CONTRACT_VERSION = "1.0.0"` in `src/fund_rank/contracts/gold.py`. Schema changes require a major bump and a JSON-Schema export.

The case deliverable, `ranking.md`, is rendered from `gold/ranking/as_of=YYYY-MM-DD/segment=*/data.parquet`. A copy is pinned at the repo root as `ranking.md`; the historical-by-date copy lives at `reports/as_of=YYYY-MM-DD/ranking.md` so reruns don't clobber prior outputs.

## 6. Known limitations (v1)

- **Cotização/liquidação days** are not in CVM Dados Abertos open files; the `cotizacao_max_dias` filter is inactive until `extrato_fi` is added.
- **Performance-fee carry** is parsed from `INF_TAXA_PERFM` text only; not deducted (`vl_quota` is already net of *realized* perf carry, but **forward-looking** carry on excess return is just a flag).
- **Tributação (longo/curto prazo)** is reported, not scored, since the regime depends on the buyer.
- **Benchmark = CDI universal** for all segments. IMA-B 5 / IMA-B 5+ / IRF-M would be more honest for long-duration `Indexados` funds; ANBIMA's IMA-B series is paid-only and not in v1.
- **Master/feeder dedupe** keeps one feeder per master per segment based on `taxa_adm_pct`. If two feeders have null fees, dedupe is non-deterministic.
- **Multi-class umbrellas** lose pre-CVM 175 history (no clean attribution). Documented as `history_source = "orphan_pre_cvm175"` in silver.
