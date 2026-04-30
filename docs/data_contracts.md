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

Two tables. `fund_metrics` is the heavy compute (rolling stats over 8M+ daily quotes); `ranking` re-uses it cheaply, so scoring tweaks don't require recomputing metrics.

### `gold/fund_metrics/as_of=YYYY-MM-DD/data.parquet`
One row per investable fund — i.e. per class without subclasses **or** per subclass. The synthetic `fund_key` is `"CLS_" + cnpj_classe` for classes, `"SUB_" + id_subclasse_cvm` for subclasses (kept in the parquet as a join key for `ranking`). All metrics use the **entire** daily history available in `silver/quota_series` up to `as_of`, after dropping daily returns flagged as jumps (|z|>5σ on a 60-day rolling window — uses `universe.yaml#jump_detection_sigma`).

| column | type | source / definition |
|---|---|---|
| `fund_key` | str | synthetic; `"CLS_<cnpj_classe>"` or `"SUB_<id_subclasse_cvm>"` |
| `cnpj_fundo` | str (14 digits) | from class/subclass treated tables |
| `cnpj_classe` | str (14 digits) | from class/subclass treated tables |
| `id_subclasse_cvm` | str (nullable) | only for subclasses |
| `situacao` | str | from class/subclass treated tables |
| `publico_alvo` | str | from class/subclass treated tables |
| `anbima_classification` | str | = `classificacao_anbima` |
| `anbima_risk_weight` | float64 | lookup from `scoring.yaml#classificacao_anbima_risk` (range 0.05–0.85) |
| `redemption_days` | int64 | = `prazo_de_resgate` |
| `equity` | float64 | latest non-null `vl_patrim_liq` from `quota_series` (≤ as_of) |
| `nr_cotst` | int64 | latest non-null `nr_cotst` from `quota_series`; 0 if the fund has no quotes attached |
| `existing_time` | int64 | days between `data_de_inicio` and `as_of` |
| `net_captation` | float64 | latest value of `rolling_mean(captc_dia − resg_dia, 252du)` |
| `hit_rate` | float64 | % of months where `monthly_return_fund > monthly_return_benchmark` over the entire history |
| `sharpe_rolling` | float64 | σ of the rolling-12m monthly Sharpe series (CDI as risk-free); high ⇒ inconsistent |
| `liquid_return_12m` | float64 | compounded monthly return of the last 12 months ending at `as_of`'s month |
| `standard_deviation_annualized` | float64 | `std(log_ret_diario) * sqrt(252)` over the entire history |
| `max_drawdown` | float64 | minimum of `cum_quota / running_peak − 1` over the entire history (≤ 0) |

Hit rate uses **monthly** comparison so benchmarks in different units (daily CDI / IMAs vs monthly IPCA-like) are directly comparable. CDI/SELIC monthly = `prod(1 + r_d) − 1`; IMAs/IRF-M monthly = end-of-month `level[m]/level[m-1] − 1`; IPCA/INPC/IGP-M = published monthly variation.

Quality report at `reports/as_of=YYYY-MM-DD/fund_metrics_quality.md` (rows, distinct funds, nulls and ranges per column).

### `gold/ranking/as_of=YYYY-MM-DD/data.parquet`
Reads `gold/fund_metrics`, applies the scoring pipeline, and writes one row per fund with all input metrics + auditability columns + the final 0–100 `score`.

**Score recipe**

Numerator (`retorno_score`) — sum then minmax of:

| metric | direction | null fill |
|---|---|---|
| `hit_rate` | positive (high = good) | 0 |
| `sharpe_rolling` | **negative** (high σ = inconsistent = bad) | 0 |
| `liquid_return_12m` | positive | 0 |

Each metric goes through `clip 3σ → minmax 0-1 → invert if negative → fill_null`.

Denominator (`risco_score`) — three subgroups, geometric mean:

- **Qualidade (fragility):** `equity`, `existing_time`, `net_captation` — clip 3σ → minmax → **invert (1−x)** so high PL/age/inflows reduce risk → fill_null=1 → sum → minmax. Subgroup represents *fragility* (high = bad).
- **Liquidez:** `anbima_risk_weight`, `redemption_days` — already "high = bad"; clip → minmax → fill_null=1 → sum → minmax.
- **Volatilidade:** `standard_deviation_annualized`, `|max_drawdown|` — `max_drawdown` is always ≤ 0 so we take the absolute value before clipping → fill_null=1 → sum → minmax.

Final: `risco_score = (qualidade × liquidez × volatilidade) ** (1/3)`. Geometric mean instead of pure product so a single near-zero subgroup doesn't collapse the risk to zero.

Score: `score = minmax(retorno_score / (risco_score + 0.01)) * 100`, rounded to 2 decimals. The 0.01 epsilon prevents division-by-zero blow-ups for funds with extreme low risk on every dimension.

**Final schema** (24 columns):

```
cnpj_fundo, cnpj_classe, id_subclasse_cvm, situacao, publico_alvo,
anbima_classification, anbima_risk_weight, redemption_days,
equity, nr_cotst, existing_time, net_captation,
hit_rate, sharpe_rolling, liquid_return_12m,
standard_deviation_annualized, max_drawdown,
retorno_score, qualidade, liquidez, volatilidade,
risco_score_geo, risco_score, score
```

Sorted by `score` descending. `nr_cotst` is exposed for downstream filters (the `ranking.md` report uses `nr_cotst > 100` to focus on larger funds) but does **not** participate in the score. Quality report at `reports/as_of=YYYY-MM-DD/ranking_quality.md` (5-bucket score distribution + nulls and ranges).

**Important caveats** (per spec, not bugs):

- No segment filtering — all RF funds compete in the same pool. RF Simples vs Crédito Livre Livre share the same normalization.
- No IR deduction; `liquid_return_12m` is gross of taxes (CVM `vl_quota` is already net of fund fees).
- No `universe.yaml` filter applied before scoring — funds with very short history get `null` on rolling metrics, which fall back to penalty fills (0 in numerator, 1 in denominator).
- Funds with no quotas in `silver/quota_series` for their `(cnpj_fundo_classe, id_subclasse)` key drop out of `gold/fund_metrics` entirely (inner join on quotas).
