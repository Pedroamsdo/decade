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
Subset of `class_funds` with `classificacao_anbima` starting with `"Renda Fixa"`. Same 17-column schema.

### `silver/subclass_funds_fixed_income/as_of=YYYY-MM-DD/data.parquet`
Subset of `subclass_funds` with `classificacao_anbima` starting with `"Renda Fixa"`. Same 17-column schema.

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
One row per investable fund — i.e. per class without subclasses **or** per subclass. Granularity key is `(cnpj_classe, id_subclasse_cvm)` (`id_subclasse_cvm` is null for classes). All metrics use the **entire** daily history available in `silver/quota_series` up to `as_of`, after dropping daily returns flagged as jumps (|z|>5σ on a 60-day rolling window — uses `universe.yaml#jump_detection_sigma`).

| column | type | source / definition |
|---|---|---|
| `cnpj_classe` | str (14 digits) | from class/subclass treated tables |
| `id_subclasse_cvm` | str (nullable) | only for subclasses |
| `situacao` | str | from class/subclass treated tables |
| `publico_alvo` | str | from class/subclass treated tables |
| `equity` | float64 | latest non-null `vl_patrim_liq` from `quota_series` (≤ as_of) |
| `nr_cotst` | int64 | latest non-null `nr_cotst` from `quota_series`; 0 if the fund has no quotes attached |
| `existing_time` | int64 | days between `data_de_inicio` and `as_of` (≥ 0) |
| `hit_rate` | float64 | % of months where `monthly_return_fund > monthly_return_benchmark` over the entire history (uses each fund's canonical benchmark) |
| `cagr` | float64 | `(vl_quota_last/vl_quota_first)^(1/years) − 1` over the fund's full quote history |
| `cv_metric` | float64 | coefficient of variation of monthly returns: `std(monthly_ret) / abs(mean(monthly_ret))` |
| `max_drawdown` | float64 | minimum of `cum_quota / running_peak − 1` over the entire history (≤ 0) |
| `score` | float64 | final 0–100 score (recipe below) |

`cnpj_fundo`, `classificacao_anbima`, `anbima_risk_weight` and `redemption_days` are **not** propagated to this table — `cnpj_fundo` and `classificacao_anbima` remain available in `silver/class_funds_fixed_income_treated` / `silver/subclass_funds_fixed_income_treated` (joined back in `ranking.md` for display); `anbima_risk_weight` and `redemption_days` were dropped from the score for simplicity.

Hit rate uses **monthly** comparison so benchmarks in different units (daily CDI / IMAs vs monthly IPCA-like) are directly comparable. CDI/SELIC monthly = `prod(1 + r_d) − 1`; IMAs/IRF-M monthly = end-of-month `level[m]/level[m-1] − 1`; IPCA/INPC/IGP-M = published monthly variation.

#### Score recipe

Each input metric goes through `clip 3σ → minmax 0-1 → optionally invert (1−x) → fill_null`. Auxiliary columns (`*_n`, `qualidade`, `liquidez`, `volatilidade`, `risco_score`, `retorno_score`) are computed internally and **not** exposed in the parquet.

| metric | direction | null fill (after invert) |
|---|---|---|
| `hit_rate` | positive (high = good) | 0 |
| `cagr` | positive | 0 |
| `equity` | **negative** (high = good → invert) | 0 |
| `existing_time` | **negative** (idem) | 0 |
| `cv_metric` | positive (high CV = volatile) | 1 |
| `max_drawdown` | **negative** (more negative = worse → invert) | 0 |

Two subgroups (each = sum of two normalized metrics → minmax):

- **Qualidade (fragility):** `equity_inv + existing_time_inv` → minmax → ∈ [0,1].
- **Volatilidade:** `cv_metric_n + max_drawdown_inv` → minmax → ∈ [0,1].

Final:

```
retorno_score = minmax(hit_rate_n + cagr_n)
risco_score   = qualidade × volatilidade   # multiplicação direta dos 2 subgrupos
score_raw     = retorno_score / risco_score   when risco_score > 0; else 0

# Outliers do score_raw → 0 (mean ± 3·σ computados sobre os elegíveis)
eligible      = (situacao == "Em Funcionamento Normal")
score_raw'    = 0  if score_raw < mean − 3·σ  or  score_raw > mean + 3·σ  (else score_raw)

# Re-normalização SOMENTE dentro do universo elegível
score         = round(minmax(score_raw'[eligible]) × 100, 2)   # null para não-elegíveis
```

Three notable mechanics:
- The guard `risco_score == 0 ⇒ score_raw = 0` zeroes out funds that hit zero on any of the three subgroups (avoids the explosion the previous geometric-mean+epsilon formulation had).
- **Outliers no `score_raw` viram zero** antes do minmax: valores fora de `mean ± 3·σ` (computados nos elegíveis) são tratados como 0. Isso evita que poucos fundos com risco quase-zero comprimam todo o resto contra o piso na escala final.
- The final `minmax` for the score is computed **only over funds in active operation** (`situacao = "Em Funcionamento Normal"`). Funds outside this universe get `score = null` — they remain in `gold/fund_metrics` for auditability of the raw metrics, but they don't compete with eligible funds in the 0–100 scale.

Quality report at `reports/as_of=YYYY-MM-DD/fund_metrics_quality.md` (rows, distinct keys, score distribution, nulls and ranges per column).

#### Important caveats (per spec, not bugs)

- No segment filtering inside `fund_metrics` — all RF funds compete in the same pool. RF Simples vs Crédito Livre Livre share the same normalization. The Markdown report (`ranking.md`) applies `situacao = "Em Funcionamento Normal"` filter before showing top-N per profile.
- No IR deduction.
- Funds with no quotas in `silver/quota_series` for their `(cnpj_fundo_classe, id_subclasse)` key drop out of `gold/fund_metrics` entirely (inner join on quotas).

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
