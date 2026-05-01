# Methodology

How `fund_rank` produces a Top-5 fixed-income fund ranking. Pipeline: `bronze` → `silver` → `gold` → `report`, deterministic for a given `--as-of` reference date.

## 1. Universe

Starting point: every CVM-registered fund with daily quotas in `INF_DIARIO`. The structural unit ranked is the **classe** (`CNPJ_Classe`) — or the **subclass** (`id_subclasse_cvm`) when one exists — *not* the umbrella fund (`CNPJ_Fundo`).

Why class/subclass and not umbrella:
- Post-CVM Resolution 175 (Oct/2023), `INF_DIARIO` reports daily quotas at the *classe* / *subclasse* level.
- A single umbrella fund may host classes with different fees, target public and policies. Ranking the umbrella conflates them.
- Each `CNPJ_Classe` (and each subclass) is what a downstream system actually sells to investors.

### Fixed-income subset

Silver tables `class_funds_fixed_income` and `subclass_funds_fixed_income` filter to RF using `silver/_fixed_income_filter.py` (ANBIMA classification prefixes mapped to canonical benchmarks in `silver/_benchmark_mapping.py`). Non-RF funds (equity, multimercado, FII, FIDC) are dropped before gold.

### CVM 175 stitch

For umbrellas that adapted to CVM 175, the legacy `CNPJ_FUNDO` reported pre-adaptation is mapped to the new `CNPJ_Classe` whenever there's exactly one class per fundo. History stitched this way is preserved in `silver/quota_series`. Multi-class umbrellas lose pre-adaptation history (no clean attribution).

### Outlier filter on daily returns

Daily log returns are flagged as jumps when `|z| > 5σ` in a 60-day rolling window (`gold/_metrics.flag_jumps`). Flagged days are excluded before aggregating to monthly returns. This removes obvious data errors (mis-published quotas) without biasing the metric against funds with legitimate large moves.

## 2. Eligibility (gold)

Eligibility is the single filter that separates "scored" from "not scored". Configured in `configs/scoring.yaml#eligibility`:

| Filter | Threshold | Source |
|---|---|---|
| `situacao` | `"Em Funcionamento Normal"` | CVM `registro_fundo_classe` |
| `nr_cotst` | `> 1,000` cotistas | latest non-null in `quota_series` |
| `existing_time` | `≥ 252` days (≈ 1 year) | `as_of − data_de_inicio` |
| `equity` | `≥ R$ 50,000,000` (PL mínimo) | latest non-null `vl_patrim_liq` |

Funds outside the criteria keep their raw metrics in `gold/fund_metrics` but get `score = null`. There are no per-segment carve-outs: the same threshold applies to all RF funds. The 4 thresholds are stricter than what most retail systems use, intentionally — the goal is "fund worth recommending", not "fund that exists".

## 3. Metrics

Two of the three metrics are computed from monthly returns, derived from daily log returns by taking the last quota of each calendar month and computing `pct_change` (`gold/_metrics.monthly_returns_from_daily`). Excess returns are vs the fund's **canonical benchmark** (mapped from ANBIMA classification — CDI / IPCA / IMA-B / IMA-B 5 / IRF-M / etc.). The third (`tax_efficiency`) is a deterministic lookup from `tributacao_alvo`.

| Metric | Formula | Direction |
|---|---|---|
| `information_ratio` | `mean(excess) / std(excess) × √12` | positive (higher is better) |
| `sortino_ratio` | `mean(excess) × 12 / (std(min(excess, 0)) × √12)` | positive |
| `tax_efficiency` | `1 − effective_ir_rate(tributacao_alvo)` | positive |

`monthly_bench_ret` is built by `gold/_benchmark_returns.py` from `silver/index_series`, with three granularity-aware compounding rules:
- CDI / SELIC (`percent_per_day`): `prod(1 + r_d/100) − 1` per month.
- IMA-* / IRF-M (`index_level`): `level[m] / level[m−1] − 1`.
- IPCA / INPC / IGP-M (`percent_per_month`): published value / 100.

### Tax-efficiency mapping

`tax_efficiency = 1 − effective_ir_rate(tributacao_alvo)`. Rates declared in `configs/scoring.yaml#tax.rates`:

| `tributacao_alvo` | Effective IR rate | `tax_efficiency` |
|---|---:|---:|
| Isento (LCI / LCA / debêntures incentivadas) | 0% | 1.00 |
| Previdenciário (PGBL / VGBL ≥ 10y) | 10% | 0.90 |
| Longo Prazo (regressiva > 720d) | 15% | 0.85 |
| Alíquota de 15% (FIP-like / fechado LP) | 15% | 0.85 |
| Curto Prazo (regressiva > 180d, ≤ 720d) | 20% | 0.80 |
| Não Aplicável / Outros / Indefinido | n/a | null |

The horizon assumed when collapsing the regressive table is `default_holding_period_years: 3.0` — investors with shorter horizons would face higher rates than these. Documented as a v1 limitation.

### Null behavior

- `information_ratio = null` when `std(excess) = 0` (perfect tracking) or fewer than 2 valid months.
- `sortino_ratio = null` when `std(min(excess, 0)) = 0` (no negative-excess months) or fewer than 2 valid months.
- `tax_efficiency = null` when `tributacao_alvo` is in a `null` bucket (Não Aplicável / Outros / Indefinido) or unmapped.
- A fund with **any** of the three metrics null gets `score = null`, even if eligible by other criteria.

## 4. Scoring

Single weighted composite, percentile-ranked over the eligible universe. No segments, no per-public buckets at the score level — the 3 profile views (Geral / Qualificado / Profissional) in `ranking.md` are pure post-filters on `publico_alvo`, applied to the same global score.

```
composite(fund) = 0.60 × z(IR, fund)
                + 0.25 × z(Sortino, fund)
                + 0.15 × z(tax_efficiency, fund)
                  # z-scores taken over the eligible universe so the three
                  # metrics enter on comparable scales

score(fund)     = round(percentile_rank(composite over eligible) × 100, 2)
                  # null for funds outside the eligible universe
```

Weights live in `configs/scoring.yaml#metrics`; engine is `gold/build_fund_metrics._compute_score`. Weights must sum to 1.0 (validated at config-load time in `settings.ScoringConfig`).

### Why this composite

- **IR (0.60)** measures consistency of active return — the CFA standard for active management. Penalizes funds whose alpha comes in soluços.
- **Sortino (0.25)** penalizes only negative excess — captures asymmetric downside risk, the gap that pure tracking error leaves open. Crucial in RF: distributions are asymmetric (fat left tail from credit events and duration shocks; capped right tail from rate spreads).
- **Tax efficiency (0.15)** discounts the IR rate paid at redemption. The investor pockets the net return, not the gross — two funds with identical IR + Sortino but different `tributacao_alvo` should not tie. Static modifier (deterministic per bucket), not a risk metric, hence the smaller weight.
- **60/25/15** keeps risk-adjusted alpha as the primary driver (85% combined) and treats tax as a tie-breaker (15%) — large enough to move the ranking among similar funds, not large enough to dominate it.

### Why percentile rank (not minmax)

- **Outlier-robust**: a single fund with extreme composite doesn't squash all others to the floor.
- **Uniform by construction**: 50% of eligible funds end below median by definition.
- **Interpretable**: score 87 means "beats 87% of peers on the IR + Sortino + tax composite".

### Why z-scores on each metric before weighting

The three metrics live on wildly different scales (IR ~ 0–5 in practice; Sortino can hit thousands when downside_dev → 0; tax_efficiency is a 3-level discrete in {0.80, 0.85, 0.90, 1.00}). Z-scoring each over the eligible universe puts them on a common axis before applying weights. Done by `_compute_score` automatically when more than one metric is configured.

Note: because `tax_efficiency` is essentially categorical, its z-score has only ~4 distinct values in the eligible universe — the metric resolves to "Isento gets a boost; Curto Prazo gets a penalty" rather than a smooth gradient. That's the intended behavior: tax is a regime, not a continuous variable.

## 5. Top-5 per profile

The 3 profiles in `ranking.md` reflect the standard CVM access hierarchy (Profissional ⊃ Qualificado ⊃ Geral):
- **Geral** — sees only `publico_alvo = "Público Geral"`.
- **Qualificado** — sees `"Público Geral"` + `"Qualificado"`.
- **Profissional** — sees all three.

Within each profile, the eligible universe is sorted by `score` descending and the top 5 are reported. Funds with `publico_alvo = null` are excluded from all three lists.

## 6. Adding a new metric

The score engine is config-driven. To add a metric `foo_ratio`:
1. Implement `attach_foo_ratio(dim_fund, monthly, bench_monthly) -> dim_fund_with_col` in `gold/_metrics.py`.
2. Add `"foo_ratio"` to `OUTPUT_COLUMNS` and to the `.pipe(...)` chain in `gold/build_fund_metrics.run`.
3. Add an entry under `metrics:` in `configs/scoring.yaml` with `direction` and `weight` (existing weights re-balanced so the sum is 1.0).
4. Add a unit test mirroring `test_attach_information_ratio_known_fixture` in `tests/unit/test_gold_metrics.py`.

The integration test `test_live_scoring_yaml_loads_and_is_consistent_with_gold_columns` automatically validates that every metric in YAML exists as a column in the gold parquet.

## 7. Known limitations

- **Cotização / liquidação days** are not in CVM Dados Abertos; cannot filter by D+0 / D+1.
- **Performance-fee carry** is implicit (`vl_quota` is net of *realized* perf carry; forward-looking carry on excess is not modeled).
- **Tax modeling is approximate** (ADR-013): single rate per `tributacao_alvo` bucket assuming a 3-year holding period; come-cotas (semestral IR antecipation in open-ended non-Isento / non-Previdência funds, ~0.5–1 p.p./yr drag) is not modeled; the benchmark side is not tax-adjusted (so Isento funds gain a relative boost over a strict net-of-tax-on-both-sides comparison).
- **Multi-class umbrellas** lose pre-CVM 175 history (no clean attribution).
- **No drawdown / Calmar metric.** A path-dependent metric would catch funds whose IR + Sortino look fine on monthly data but had a severe intra-month or short-window drawdown. Possible v2 addition.
