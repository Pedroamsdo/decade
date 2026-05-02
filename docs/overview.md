# Visão geral do projeto — Decade PS Fund Ranking

Documento executivo que amarra as principais decisões do case. Para profundidade técnica, ver `docs/methodology.md`, `docs/data_contracts.md`, `docs/decisions.md` (ADRs) e `docs/scaling.md`.

---

## 1. Introdução

O case pede um ranking de fundos de renda fixa brasileiros a partir de dados públicos. A entrega é um pipeline reprodutível ponta a ponta — da coleta dos dados regulatórios à publicação de um relatório de Top-5 por perfil de investidor.

**Decisão central: empacotar tudo em uma CLI única.** O entrypoint é `fund-rank --as-of YYYY-MM-DD` (Typer, definido em `src/fund_rank/cli.py`), que executa o pipeline inteiro em uma chamada: ingestão → bronze → silver → gold → relatório.

Por que CLI, sem subcomandos:

- **Consumo por agente / automação.** Um único comando idempotente é trivial de envelopar em um agente, container ou DAG. Sem estado intermediário a gerenciar.
- **Superfície de erro mínima.** Subcomandos seriam atalhos para ferramentas internas; preferi expor um botão só e deixar o orquestrador (futuro) cuidar de granularidade.
- **Reprodutibilidade local.** Como o universo de dados cabe na máquina, otimizei para "clonou → roda → tem o mesmo resultado". Ver ADR-012.

---

## 2. Ingestão de dados

### Fontes escolhidas

| Fonte | Arquivo / endpoint | Para quê |
|---|---|---|
| CVM | `cad_fi_hist.zip` | Cadastro histórico de fundos (pré-CVM 175). |
| CVM | `registro_fundo_classe.zip` | Registro de classe/subclasse (pós-CVM 175). |
| CVM | `inf_diario_fi_YYYYMM.zip` (mensal) | Cotas diárias, PL, nº de cotistas. |
| BCB SGS | API pública | CDI, SELIC, IPCA, INPC, IGP-M. |
| ANBIMA | XLS drop manual | Histórico dos índices IMA-* e IRF-M. |

### Por que essas fontes

CVM é a fonte oficial e gratuita para o universo regulado — qualquer ranking sério parte daí. BCB SGS resolve macro indexadores com API estável. ANBIMA cobre os benchmarks de RF (família IMA) que não têm equivalente público confiável.

### Dores reais da CVM

Os dados da CVM são complicados na prática. Layouts mudam entre arquivos históricos e atuais; CSV vem em latin-1, separador `;`, schemas heterogêneos por safra. Há zips aninhados, particionamento mensal e missing data recorrente em campos como PL e nº de cotistas. O servidor tem rate limit prático de ~3s/arquivo, então a primeira execução baixa ~1.5 GB em 30–60 min. Reexecuções são cacheadas por hash.

Além disso, a CVM 175 mudou a unidade de análise de "fundo" para "classe/subclasse", então é preciso costurar cadastros pré e pós-norma sem duplicar nem perder histórico — ver ADR-004.

### ANBIMA paga

A ANBIMA tem uma API paga que parece ter os melhores dados disponíveis para esse problema: classificações curadas, taxas padronizadas, índices intra-day. Resolveria várias imputações que hoje faço no silver. Não foi adotada por estar fora do escopo de custo do case; fica como próximo passo natural.

### Estrutura de dados — Medallion (bronze / silver / gold)

A escolha por medallion isola o que é caro (download) do que é barato (transformação), permite auditoria por camada e habilita reexecução parcial.

- **Bronze** (`data/bronze/`): cópia 1:1 dos arquivos crus + manifest com `sha256` e `etag`. Idempotência por hash, sem partição por `ingested_at`.
- **Silver** (`data/silver/`): Parquet tipado, 8 tabelas:
  - `class_funds`, `subclass_funds` — dimensão CVM 175 costurada.
  - `class_funds_fixed_income[_treated]`, `subclass_funds_fixed_income[_treated]` — recorte RF com benchmark canônico mapeado.
  - `quota_series[_fixed_income]` — série diária unificada pré e pós CVM 175.
  - `index_series` — históricos de benchmarks (CDI, IPCA, IMA-*, IRF-M).
- **Gold** (`data/gold/`): `fund_metrics` (uma linha por fundo, com IR, Sortino, tax_efficiency, score) e `validacao` (cross-check de retorno YTD 2025).

Tudo é particionado por `as_of=YYYY-MM-DD` para tornar o run-by-run rastreável.

---

## 3. Ranking

### O que tentei antes

A primeira versão do score era 70% Information Ratio + 30% Sortino, com tributação aplicada como **haircut sobre os retornos** antes do cálculo das métricas. Pareceu natural — "comparar net-of-tax".

O problema apareceu quando comecei a inspecionar resultados: o haircut não preserva invariância de escala do z-score. Fundos com benchmarks diferentes acabam sendo comparados de forma enviesada porque a tributação afeta a magnitude bruta dos retornos, mas o z-score já normaliza isso na comparação inter-pares. Aplicar tributação aos retornos contamina a métrica de risco-ajustado em vez de medir a eficiência tributária por si só. Ver ADR-013.

### Versão final adotada

Score como combinação linear de três métricas padronizadas globalmente (z-score) e convertidas em percentile rank × 100:

| Métrica | Peso | Por quê |
|---|---|---|
| **Information Ratio** | 60% | Consistência de alfa anualizado vs benchmark canônico — núcleo do "stock picking" em RF. |
| **Sortino** | 25% | Penaliza só downside, captura risco de cauda assimétrico, mais adequado a RF do que Sharpe. |
| **Tax Efficiency** | 15% | `1 − alíquota_efetiva` por bucket: Isento=1.0, Previdenciário=0.90, Longo Prazo=0.85, Curto Prazo=0.80. |

Pesos validados em runtime contra `configs/scoring.yaml`.

**Universo único.** O score é calculado em um pool global; os filtros de Geral / Qualificado / Profissional são aplicados **depois**, sobre o ranking já produzido. Isso evita inflar artificialmente fundos pequenos em segmentos rasos. Ver ADR-005.

**Filtros de elegibilidade** (`configs/scoring.yaml`):

- `situacao = "Em Funcionamento Normal"`
- `nr_cotst > 1.000`
- idade ≥ 252 dias úteis
- PL ≥ R$ 50 MM

**Benchmark canônico** mapeado por classe ANBIMA (CDI, IPCA, IMA-B, IMA-B 5, IMA-B 5+, IMA-GERAL, IMA-S, IRF-M) — ADR-003.

---

## 4. Final — respondendo às perguntas do case

**Correctness.** Validação cruzada com retornos YTD 2025 em `gold/validacao`; relatório de qualidade por camada em `reports/as_of=*/data_quality.md`; 45 testes (unit + integration); contratos Pydantic v2 nas fronteiras silver/gold. Nulos são tratados explicitamente — fundo sem bucket tributário recebe score nulo em vez de imputação silenciosa.

**Exposability.** CLI única → fácil envelopar em API, agente ou DAG. Outputs em Parquet (silver/gold) + Markdown (`ranking.md`, `data_quality.md`). Schemas estáveis em `docs/data_contracts.md`.

**Scalability.** Stack atual (Polars sobre Parquet, com PyArrow como engine de I/O) escala vertical sem reescrita — Polars opera lazy/streaming e os arquivos Parquet já são colunarem comprimidos. Caminho de migração horizontal está em `docs/scaling.md`: local Parquet → S3 (~10×) → BigQuery/Snowflake (~100×). Como o medallion separa as camadas, a migração pode ser feita por etapa, sem big-bang.

**Robustness.** Idempotência por `sha256` no bronze, manifest por arquivo, particionamento por `as_of`, schemas tipados no silver, validação Pydantic na fronteira do gold. Reexecução do mesmo `--as-of` é determinística e barata.

---

## 5. Próximos passos

1. **Validação mais profunda dos dados.** Cross-check com fontes secundárias (consultas a sites de assets, comparação com rankings públicos), sanity checks de outliers, alertas para schema drift na CVM.
2. **Adotar a API paga da ANBIMA** para classificação, taxas e índices — elimina imputações manuais no silver e melhora a fidelidade do benchmark mapping.
3. **Orquestrador** (Airflow / Prefect / Dagster) para tirar a necessidade de rodar a CLI à mão. Hoje a execução é single-shot por design (ADR-012); o passo natural é envelopar a CLI em uma DAG mensal disparada pós-publicação CVM, com retry, alerta e versionamento de artefatos.
