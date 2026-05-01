# Data contracts

This page enumerates the typed contracts the pipeline produces. The bronze layer is intentionally untyped (it stores raw CSV / ZIP / JSON 1:1 with the source). Silver and gold layers expose typed Parquet, validated against pydantic v2 models in `src/fund_rank/contracts/`.

## Silver layer

These are **internal** schemas — silver is consumed only by the gold layer and tests. Field names are lowercase Python conventions (`cnpj_classe`), not CVM's upper-snake (`CNPJ_FUNDO_CLASSE`).

### `silver/class_funds/as_of=YYYY-MM-DD/data.parquet`
Class-level dimension. **One row per `cnpj_classe`.** Excludes any classe that has subclasses (those go to `silver/subclass_funds`).

| column | type | source | note |
|---|---|---|---|
| `cnpj_fundo` | str (14 digits) | `registro_fundo.csv#CNPJ_Fundo` | umbrella, zfill(14) |
| `cnpj_classe` | str (14 digits) | `registro_classe.csv#CNPJ_Classe` | primary key, zfill(14) |
| `denom_social_fundo` | str | `registro_fundo.csv#Denominacao_Social` | umbrella display name |
| `denom_social_classe` | str | `registro_classe.csv#Denominacao_Social` | classe display name |
| `situacao` | str | `registro_classe.csv#Situacao` | active filter |
| `data_de_inicio` | date | `registro_classe.csv#Data_Inicio_Situacao` | |
| `exclusivo` | str | `registro_classe.csv#Exclusivo` | "S" / "N" |
| `publico_alvo` | str | `registro_classe.csv#Publico_Alvo` | "Público Geral" / "Qualificado" / "Profissional" |
| `condominio` | str | `registro_classe.csv#Forma_Condominio` | "Aberto" / "Fechado" |
| `classificacao_anbima` | str | ANBIMA xlsx `#Tipo ANBIMA` | |
| `composicao_fundos` | str | ANBIMA xlsx `#Composição dos Fundos` | |
| `tributacao_alvo` | str | ANBIMA xlsx `#Tributação Alvo` | |
| `aplicacao_minima` | str | ANBIMA xlsx `#Aplicação Inicial Mínima` | string (mantém formatos como "Sem mínimo") |
| `prazo_de_resgate` | int64 | ANBIMA xlsx `#Prazo Pagamento Resgate em dias` | |
| `taxa_adm` | float64 | `cad_fi_hist_taxa_adm.csv#TAXA_ADM` | most-recent row by DT_INI_VIGENCIA, divided by 100 (decimal) |
| `taxa_perform` | float64 | `cad_fi_hist_taxa_perfm.csv#VL_TAXA_PERFM` | most-recent row by DT_INI_VIGENCIA, divided by 100 (decimal) |
| `benchmark` | str | `cad_fi_rentab.csv#RENTAB_FUNDO` | most-recent row by DT_INI_VIGENCIA |

Quality report (nulls per column + duplicates by `cnpj_classe`) is written to `reports/as_of=YYYY-MM-DD/class_funds_quality.md`.

### `silver/subclass_funds/as_of=YYYY-MM-DD/data.parquet`
Subclass-level dimension. **One row per `id_subclasse_cvm`.** Population is the complement of `class_funds` (every row in `registro_subclasse.csv`).

| column | type | source | note |
|---|---|---|---|
| `cnpj_fundo` | str (14 digits) | `registro_fundo.csv#CNPJ_Fundo` (via chain) | umbrella |
| `cnpj_classe` | str (14 digits) | `registro_classe.csv#CNPJ_Classe` (via chain) | parent classe |
| `id_subclasse_cvm` | str | `registro_subclasse.csv#ID_Subclasse` | primary key (alphanumeric token, e.g. `MZMRC1747322915`) |
| `denom_social_subclasse` | str | `registro_subclasse.csv#Denominacao_Social` | |
| `situacao` | str | `registro_subclasse.csv#Situacao` | |
| `data_de_inicio` | date | `registro_subclasse.csv#Data_Inicio_Situacao` | |
| `exclusivo` | str | `registro_subclasse.csv#Exclusivo` | |
| `publico_alvo` | str | `registro_subclasse.csv#Publico_Alvo` | |
| `condominio` | str | `registro_subclasse.csv#Forma_Condominio` | |
| `classificacao_anbima` | str | ANBIMA xlsx `#Tipo ANBIMA` | two-pass (see below) |
| `composicao_fundos` | str | ANBIMA xlsx `#Composição do Fundo` | two-pass |
| `tributacao_alvo` | str | ANBIMA xlsx `#Tributação Alvo` | two-pass |
| `aplicacao_minima` | str | ANBIMA xlsx `#Aplicação Inicial Mínima` | two-pass |
| `prazo_de_resgate` | int64 | ANBIMA xlsx `#Prazo Pagamento Resgate em dias` | two-pass + cast int |
| `taxa_adm` | float64 | `cad_fi_hist_taxa_adm.csv#TAXA_ADM` (in `cad_fi_hist.zip`) | most-recent / 100; **shared by all subclasses of same CNPJ_Fundo** |
| `taxa_perform` | float64 | `cad_fi_hist_taxa_perfm.csv#VL_TAXA_PERFM` | most-recent / 100; same caveat |
| `benchmark` | str | `cad_fi_hist_rentab.csv#RENTAB_FUNDO` | most-recent; same caveat |

ANBIMA two-pass join: pass 1 by `id_subclasse_cvm` ↔ `Código CVM Subclasse` (precise, ~33% of ANBIMA Subclasse rows); pass 2 fallback by `(cnpj_fundo, cnpj_classe)` for ANBIMA rows where the código is null. Pass 1 takes priority on coalesce.

Quality report at `reports/as_of=YYYY-MM-DD/subclass_funds_quality.md` (nulls + duplicates + ANBIMA pass breakdown).

### `silver/class_funds_fixed_income/as_of=YYYY-MM-DD/data.parquet`
**Filter-only** RF subset of `class_funds`: rows with `classificacao_anbima` starting with `"Renda Fixa"`. Same 17-column schema. Nulls and outliers in `taxa_adm`, `taxa_perform` and the raw CVM `benchmark` strings are preserved for auditability — treatment lives in the `_treated` table below. Quality report at `reports/as_of=YYYY-MM-DD/class_funds_fixed_income_quality.md`.

### `silver/subclass_funds_fixed_income/as_of=YYYY-MM-DD/data.parquet`
**Filter-only** RF subset of `subclass_funds`. Same 17-column schema, same auditability guarantees as the class table. Quality report at `reports/as_of=YYYY-MM-DD/subclass_funds_fixed_income_quality.md`.

### `silver/class_funds_fixed_income_treated/as_of=YYYY-MM-DD/data.parquet`
Treated version of `class_funds_fixed_income` (same 17-column schema). Two transformations applied:

- **Benchmark mapping.** Raw CVM `RENTAB_FUNDO` strings → 10 canonical codes (`CDI`, `IPCA`, `INPC`, `IGP-M`, `IMA-B`, `IMA-B 5`, `IMA-B 5+`, `IMA-GERAL`, `IMA-S`, `IRF-M`). Nulls and unmapped strings collapse to `"CDI"` (the RF default per spec).
- **Taxa imputation.** `taxa_adm` and `taxa_perform`: nulls **and** outliers (|z|>3 against the non-null subset) are replaced with the column **mode**. Stats are computed on this same RF-filtered class table.

Quality report at `reports/as_of=YYYY-MM-DD/class_funds_fixed_income_treated_quality.md` — `taxa_adm`, `taxa_perform` and `benchmark` should have null counts of zero.

### `silver/subclass_funds_fixed_income_treated/as_of=YYYY-MM-DD/data.parquet`
Treated version of `subclass_funds_fixed_income` (same 17-column schema). Same benchmark mapping rule as the class table. Taxa imputation also follows the same mode/3σ rule, but **stats are sourced from `silver/class_funds_fixed_income`** (the raw RF class table, pre-imputation) — so subclass and class share the same canonical imputation distribution.

Quality report at `reports/as_of=YYYY-MM-DD/subclass_funds_fixed_income_treated_quality.md`.

### `silver/quota_series/as_of=YYYY-MM-DD/data.parquet`
Daily quota observations from CVM `INF_DIARIO`, unifying pre- and post-CVM 175 schemas into one canonical lowercase shape. Sourced from `cvm_inf_diario` (monthly post-175) + `cvm_inf_diario_hist` (yearly pre-175). 1 row per `(cnpj_fundo_classe, id_subclasse, dt_comptc)`.

| column | type | post-CVM 175 | pre-CVM 175 |
|---|---|---|---|
| `tp_fundo_classe` | str | `TP_FUNDO_CLASSE` | null (não existe na fonte) |
| `cnpj_fundo_classe` | str (14 digits) | `CNPJ_FUNDO_CLASSE`, zfill | `CNPJ_FUNDO`, zfill |
| `id_subclasse` | str | `ID_SUBCLASSE` (often null) | null (não existe na fonte) |
| `dt_comptc` | date | `DT_COMPTC` | `DT_COMPTC` |
| `vl_total` | float64 | `VL_TOTAL` | `VL_TOTAL` |
| `vl_quota` | float64 | `VL_QUOTA` | `VL_QUOTA` |
| `vl_patrim_liq` | float64 | `VL_PATRIM_LIQ` | `VL_PATRIM_LIQ` |
| `captc_dia` | float64 | `CAPTC_DIA` | `CAPTC_DIA` |
| `resg_dia` | float64 | `RESG_DIA` | `RESG_DIA` |
| `nr_cotst` | int64 | `NR_COTST` | `NR_COTST` |

## Gold layer

Two parquet tables: `gold/fund_metrics` (score) and `gold/validacao` (calendar-year 2025 return for cross-checking). The Markdown report `ranking.md` is generated by `gold/build_ranking_report.py` reading `fund_metrics` directly (filters + per-profile top-N).

### `gold/fund_metrics/as_of=YYYY-MM-DD/data.parquet`
One row per investable fund (class without subclasses **or** subclass). Granularity key is `(cnpj_classe, id_subclasse_cvm)` (null for classes). Returns are computed over the entire daily history in `silver/quota_series` up to `as_of`, after dropping daily returns flagged as jumps (|z|>5σ on a 60-day rolling window).

**10 columns:**

| column | type | description |
|---|---|---|
| `cnpj_classe` | str (14 digits) | class CNPJ |
| `id_subclasse_cvm` | str (nullable) | subclass id; null for classes |
| `situacao` | str | CVM status — **filter only** |
| `publico_alvo` | str | display only (used by `ranking.md` profiles) |
| `equity` | float64 | latest non-null `vl_patrim_liq` (≤ as_of) — **filter only** |
| `nr_cotst` | int64 | latest non-null cotistas; 0 if no quotes — **filter only** |
| `existing_time` | int64 | days between `data_de_inicio` and `as_of` — **filter only** |
| `information_ratio` | float64 | `mean(excess) / std(excess) × √12` (annualized, vs canonical benchmark) |
| `sortino_ratio` | float64 | `mean(excess) × 12 / (std(min(excess, 0)) × √12)` (annualized, downside-only) |
| `score` | float64 | percentile rank of the weighted composite (`0.7 × z(IR) + 0.3 × z(Sortino)`) over the eligible universe × 100 |

#### Score recipe

The score combines **two metrics** of excess return vs the fund's canonical benchmark — IR for consistency of alpha, Sortino for asymmetric downside risk. This is the CFA L3 framework for fixed-income fund selection: IR alone treats upside and downside vol symmetrically, which is wrong for RF distributions (fat left tails from credit events and duration shocks).

```
excess[t]        = monthly_ret_fund[t] − monthly_ret_bench[t]

IR_anualizado    = mean(excess) / std(excess) × √12              # weight 0.7
Sortino_anual    = mean(excess) × 12 / (std(min(excess, 0)) × √12)  # weight 0.3

composite        = 0.7 × z(IR) + 0.3 × z(Sortino)
                   # z-scores taken over the eligible universe so the two
                   # metrics enter on comparable scales

eligible         = situacao == "Em Funcionamento Normal"
                AND nr_cotst > 1000
                AND existing_time >= 252       (≈ 1 year of history)
                AND equity >= R$ 50_000_000

score            = round(percentile_rank(composite over eligible) × 100, 2)
                   # null for funds outside the eligible universe
```

`monthly_bench_ret` uses each fund's canonical benchmark code (CDI, IPCA, IMA-B, IRF-M, …), built by `gold/_benchmark_returns.py` from `silver/index_series` at monthly granularity:
- CDI/SELIC (`percent_per_day`): `prod(1 + r_d/100) − 1`.
- IMA-* / IRF-M (`index_level`): `level[m]/level[m−1] − 1`.
- IPCA/INPC/IGP-M (`percent_per_month`): published value / 100.

**Why Information Ratio (weight 0.7):**
- CFA standard for active management vs benchmark.
- Tracking error (denominator) naturally normalizes funds tightly coupled to the benchmark.
- Allows fair comparison across funds with different benchmarks (each rated against its own).
- Sign-preserving: funds losing to the benchmark get IR < 0 → low percentile.

**Why Sortino Ratio (weight 0.3):**
- Penalizes only negative excess returns — captures the asymmetric tail risk that defines RF (drawdowns from credit events, marcação a mercado shocks).
- Closes the gap left by IR's symmetric tracking error: two funds with the same IR can differ wildly in left-tail behavior.
- Lower weight than IR because Sortino is unstable when a fund has few months below benchmark (denominator → 0). Z-score normalization tames the scale, but the 30% cap prevents an isolated quiet period from dominating the ranking.

**Why percentile rank (not minmax):**
- Outlier-robust: a single fund with extreme composite doesn't squash all others to the floor.
- Uniform distribution by construction: 50% of eligible funds end below median by definition.
- Interpretable: score 87 means "beats 87% of peers on the IR + Sortino composite".

`information_ratio` is null when a fund has zero tracking error or fewer than 2 valid monthly observations. `sortino_ratio` is null when the fund has no negative excess months (downside_dev = 0) or fewer than 2 valid observations. Funds with **either** metric null get `score = null` (the weighted composite cannot be evaluated), even if eligible by other criteria.

Configuration of metrics, weights, and eligibility lives in `configs/scoring.yaml` — adding a new metric is a 3-step recipe (`attach_<name>` in `gold/_metrics.py`, append to `OUTPUT_COLUMNS`, list in YAML).

Quality coverage at `reports/as_of=YYYY-MM-DD/data_quality.md` (single consolidated report covering every silver and gold table).

### `gold/validacao/as_of=YYYY-MM-DD/data.parquet`
Auxiliary table for sanity-checking the score against the raw 2025 calendar-year return. One row per investable RF fund (5,849 = 5,623 classes + 226 subclasses), 5 columns:

| column | type | description |
|---|---|---|
| `cnpj_fundo` | str (14 digits) | umbrella CNPJ (from class/subclass treated tables) |
| `cnpj_classe` | str (14 digits) | class CNPJ |
| `id_subclasse_cvm` | str (nullable) | subclass id; `null` for classes without subclasses |
| `nome` | str | `denom_social_classe` for classes; `denom_social_subclasse` for subclasses |
| `retorno_2025` | float64 | `vl_quota[last <= 2025-12-31] / vl_quota[last <= 2024-12-31] − 1`; `null` when either anchor is missing |

"Last cota ≤ date" anchors tolerate holidays / gaps in the daily series. NaN/inf sanitized to null. Quality report at `reports/as_of=YYYY-MM-DD/validacao_quality.md` (rows, distinct, nulls, distribution percentiles).
