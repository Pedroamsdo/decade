# Fund Ranking — Renda Fixa (as_of = 2025-12-31)

## Filtro de elegibilidade

O `score` em `gold/fund_metrics` é calculado **apenas** para fundos que passam pelos 4 critérios abaixo (os demais ficam com `score = null`):

- `situacao = "Em Funcionamento Normal"`
- `nr_cotst > 1,000` cotistas
- `existing_time ≥ 252` dias (≈ 1.0 ano de história)
- `equity ≥ R$ 50,000,000` (PL mínimo)

**Universo elegível: 406 de 5,849 fundos** (5,443 fora dos critérios).

## Como o score é calculado

Métrica única: **Information Ratio (IR) anualizado** vs benchmark canônico do fundo (CDI / IPCA / IMA-B / etc., mapeado em `silver/_benchmark_mapping`).

```
excess[t]      = monthly_ret_fund[t] − monthly_ret_bench[t]
IR_anualizado  = mean(excess) / std(excess) × √12
score          = percentile_rank(IR over eligible) × 100
```

Score 95 → o fundo bate 95% dos pares elegíveis em IR. Padrão CFA para gestão ativa em renda fixa.

Detalhes de tratamento de nulls e outliers em `docs/data_contracts.md` (seção Gold layer).

## Sumário do score (universo elegível)

- Min / Mediana / Média / Max: **0.25** / **50.12** / **50.12** / **100.00**

| Bucket | Fundos | % |
|---|---:|---:|
| 0–20 | 81 | 19.95% |
| 20–40 | 81 | 19.95% |
| 40–60 | 81 | 19.95% |
| 60–80 | 81 | 19.95% |
| 80–100 | 82 | 20.20% |

---

## Top-5 por perfil de investidor

Cada perfil enxerga o pool dos fundos cujo `publico_alvo` ele pode acessar (hierarquia CVM padrão — Profissional ⊃ Qualificado ⊃ Geral):

- **Geral** → só fundos com `publico_alvo = "Público Geral"`.
- **Qualificado** → fundos `"Público Geral"` + `"Qualificado"`.
- **Profissional** → todos os tipos (`"Público Geral"` + `"Qualificado"` + `"Profissional"`).

Fundos sem `publico_alvo` declarado (`null`) ficam fora das três listas.

## Perfil: **Geral**

- Pode investir em: "Público Geral".
- Universo elegível neste perfil: **361** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | IR (anual) | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | `59376795000180` | PRINZ LIQUIDEZ FUNDO DE INVESTIMENTO EM RENDA FIXA CRÉDITO PRIVADO | Renda Fixa Duração Baixa Crédito Livre | CDI | R$ 201.06M | 1,384 | 261 | 5.76 | **100.00** |
| 2 | `51253495000100` | MAPFRE CONFIANZA FIF RENDA FIXA REFERENCIADO DI CRÉDITO PRIVADO - RESP LTDA | Renda Fixa Duração Baixa Grau de Investimento | CDI | R$ 10.13B | 163,528 | 275 | 5.53 | **99.75** |
| 3 | `23970201000117` | OURO PRETO REAL FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO RENDA FIXA LONGO PRAZO | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 185.37M | 2,768 | 317 | 5.39 | **99.51** |
| 4 | `39586858000115` (sub `KB0OK1743799805`) | SUBCLASSE I DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 63.90M | 7,445 | 264 | 5.19 | **99.26** |
| 5 | `39586858000115` (sub `M0UTC1743800258`) | SUBCLASSE II DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 250.60M | 8,183 | 264 | 5.19 | **99.01** |

## Perfil: **Qualificado**

- Pode investir em: "Público Geral", "Qualificado".
- Universo elegível neste perfil: **395** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | IR (anual) | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | `59376795000180` | PRINZ LIQUIDEZ FUNDO DE INVESTIMENTO EM RENDA FIXA CRÉDITO PRIVADO | Renda Fixa Duração Baixa Crédito Livre | CDI | R$ 201.06M | 1,384 | 261 | 5.76 | **100.00** |
| 2 | `51253495000100` | MAPFRE CONFIANZA FIF RENDA FIXA REFERENCIADO DI CRÉDITO PRIVADO - RESP LTDA | Renda Fixa Duração Baixa Grau de Investimento | CDI | R$ 10.13B | 163,528 | 275 | 5.53 | **99.75** |
| 3 | `23970201000117` | OURO PRETO REAL FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO RENDA FIXA LONGO PRAZO | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 185.37M | 2,768 | 317 | 5.39 | **99.51** |
| 4 | `39586858000115` (sub `KB0OK1743799805`) | SUBCLASSE I DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 63.90M | 7,445 | 264 | 5.19 | **99.26** |
| 5 | `39586858000115` (sub `M0UTC1743800258`) | SUBCLASSE II DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 250.60M | 8,183 | 264 | 5.19 | **99.01** |

## Perfil: **Profissional**

- Pode investir em: "Público Geral", "Qualificado", "Profissional".
- Universo elegível neste perfil: **396** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | IR (anual) | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | `59376795000180` | PRINZ LIQUIDEZ FUNDO DE INVESTIMENTO EM RENDA FIXA CRÉDITO PRIVADO | Renda Fixa Duração Baixa Crédito Livre | CDI | R$ 201.06M | 1,384 | 261 | 5.76 | **100.00** |
| 2 | `51253495000100` | MAPFRE CONFIANZA FIF RENDA FIXA REFERENCIADO DI CRÉDITO PRIVADO - RESP LTDA | Renda Fixa Duração Baixa Grau de Investimento | CDI | R$ 10.13B | 163,528 | 275 | 5.53 | **99.75** |
| 3 | `23970201000117` | OURO PRETO REAL FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO RENDA FIXA LONGO PRAZO | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 185.37M | 2,768 | 317 | 5.39 | **99.51** |
| 4 | `39586858000115` (sub `KB0OK1743799805`) | SUBCLASSE I DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 63.90M | 7,445 | 264 | 5.19 | **99.26** |
| 5 | `39586858000115` (sub `M0UTC1743800258`) | SUBCLASSE II DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 250.60M | 8,183 | 264 | 5.19 | **99.01** |
