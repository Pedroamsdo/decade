# Data contracts

This page enumerates the typed contracts the pipeline produces. The bronze layer is intentionally untyped (it stores raw CSV / ZIP / JSON 1:1 with the source). Silver and gold layers expose typed Parquet, validated against pydantic v2 models in `src/fund_rank/contracts/`.

## Silver layer

These are **internal** schemas — silver is consumed only by the gold layer and tests. Field names are lowercase Python conventions (`cnpj_classe`), not CVM's upper-snake (`CNPJ_FUNDO_CLASSE`).

### `silver/funds/as_of=YYYY-MM-DD/data.parquet`
Class-level dimension. One row per `cnpj_classe`.

| column | type | source | note |
|---|---|---|---|
| `cnpj_classe` | str (14 digits) | `registro_classe.csv#CNPJ_Classe` | primary key |
| `cnpj_fundo` | str (14 digits) | `registro_fundo.csv#CNPJ_Fundo` via `id_registro_fundo` | umbrella |
| `id_registro_fundo` | int | both | join key |
| `denom_social` | str | `registro_classe.csv#Denominacao_Social` | display name |
| `tipo_classe` | str | `registro_classe.csv#Tipo_Classe` | "Classes de Cotas de Fundos FIF" etc. |
| `situacao` | str | `registro_classe.csv#Situacao` | active filter |
| `condominio` | str | `registro_classe.csv#Forma_Condominio` | "Aberto" / "Fechado" |
| `exclusivo` | str | `registro_classe.csv#Exclusivo` | "S" / "N" |
| `publico_alvo` | str | `registro_classe.csv#Publico_Alvo` | "Público Geral" / "Qualificado" / "Profissional" |
| `trib_lprazo` | str | `registro_classe.csv#Tributacao_Longo_Prazo` | "S" / "N" |
| `classe_anbima_raw` | str | `registro_classe.csv#Classificacao_Anbima` | with accents/abbreviations |
| `classe_anbima_norm` | str | derived | strip-accents + lowercase + collapse-ws |
| `dt_inicio_classe` | str (date) | `registro_classe.csv#Data_Inicio` | |
| `tipo_fundo` | str | `registro_fundo.csv#Tipo_Fundo` | "FIF" / "FIDC" / etc. |
| `dt_adaptacao_175` | str (date) | `registro_fundo.csv#Data_Adaptacao_RCVM175` | stitch cut-off |
| `cnpj_administrador` | str (14 digits) | `registro_fundo.csv#CNPJ_Administrador` | |
| `cnpj_gestor` | str (14 digits) | `registro_fundo.csv#CPF_CNPJ_Gestor` | |
| `taxa_adm_pct` | float | `cad_fi.csv#TAXA_ADM` (avg over fundo) | a.a. % |
| `taxa_perfm_text` | str | `cad_fi.csv#TAXA_PERFM` | text — parser is in backlog |

### `silver/quota_series/as_of=YYYY-MM-DD/data.parquet`
Daily quota observations for every series.

| column | type | note |
|---|---|---|
| `cnpj_classe` | str | populated post-CVM 175 |
| `cnpj_fundo` | str | populated pre-CVM 175 |
| `dt_comptc` | date | trading day |
| `vl_quota` | float | quota value (12 decimals from CVM) |
| `vl_patrim_liq` | float | net asset value (BRL) |
| `captc_dia` | float | inflows |
| `resg_dia` | float | redemptions |
| `nr_cotst` | int | unit-holders count |
| `series_id` | str | the unit ranked: `coalesce(cnpj_classe, stitched_class, cnpj_fundo)` |
| `history_source` | str | `own` / `stitched_cvm175` / `orphan_pre_cvm175` |
| `log_return` | float | `log(vl_quota) − log(prev vl_quota)` within `series_id` |
| `jump_flag` | bool | True if abs z-score over 60d window > 5σ |

### `silver/universe/as_of=YYYY-MM-DD/segment={caixa,rfgeral,qualificado}/data.parquet`
Output of `build_universe`. Same schema as `silver/funds` plus `segment_id`, `pl_mediano`, `cotistas`, `dias_uteis`, `dias_captc_positivos`.

## Gold layer

These are the **external** contracts. Schema changes require a major bump of `CONTRACT_VERSION` (currently `"1.0.0"`) in `src/fund_rank/contracts/gold.py`.

### `gold/fund_metrics/as_of=YYYY-MM-DD/segment=*/data.parquet`
One row per `cnpj_classe` × segment. Defined by `FundMetrics` (see `contracts/gold.py`).

Selected fields:

| column | type | source |
|---|---|---|
| `schema_version` | str | constant, currently `1.0.0` |
| `cnpj_classe`, `cnpj_fundo` | str | identity |
| `dt_ref` | date | the `--as-of` parameter |
| `retorno_acum_{12,24,36}m` | float | gold/compute_metrics |
| `pct_cdi_{12,24,36}m` | float | ratio (1.07 = 107% CDI) |
| `vol_anualizada_{12,36}m` | float | `std(log_return) * sqrt(252)` |
| `max_drawdown_36m` | float | negative number |
| `sharpe_12m`, `sortino_24m`, `info_ratio_12m` | float | annualized |
| `tracking_error_cdi_12m` | float | annualized std of (fund − cdi) daily |
| `taxa_adm_pct_aa` | float | from CAD; not used in past returns |
| `pl_mediano_12m`, `cotistas` | float, int | liquidity proxy |
| `retorno_covid_2020`, `retorno_marola_credito_2024` | float | event windows |
| `history_source` | str | `own` / `stitched_cvm175` |
| `history_confidence` | str | `HIGH` (≥756 du) / `MED` (≥504 du) / `LOW` (≥252 du) |

### `gold/ranking/as_of=YYYY-MM-DD/segment=*/data.parquet`
The ranked output; `FundMetrics` columns plus:

| column | type | note |
|---|---|---|
| `score` | float | weighted z-score sum |
| `_z_<metric>` | float | per-metric z-score (transparency) |
| `rank` | int | 1-indexed, ascending |

The narrative `ranking.md` (case deliverable) is rendered from this parquet.

## Versioning policy

A non-breaking schema change (adding a nullable column with a default) is a **minor** bump. Removing a column or changing a type is a **major** bump and requires:

1. Update `CONTRACT_VERSION` in `contracts/gold.py`.
2. Re-export JSON Schema (TODO: wire into CI).
3. Coordinate with downstream consumers before the next deploy.
