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

## Gold layer

To be designed. The legacy gold/rank/report chain (built around the now-removed `silver/funds`/`quota_series`/`universe` tables) was retired; the new gold layer will be wired on top of `class_funds` + `subclass_funds`.
