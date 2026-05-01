# Fund Ranking — Renda Fixa (as_of = 2025-12-31)

## Filtro de elegibilidade

Aplicados em sequência:

1. `situacao = "Em Funcionamento Normal"` → **5,812** de **5,849** (37 excluídos).
2. `nr_cotst > 1000` → **726** de **5,812** (5,086 excluídos por terem ≤ 1.000 cotistas).

**Universo final do ranking: 726 fundos.**

## Como o score é calculado

Lê `gold/fund_metrics` (a coluna `score` já vem calculada). Resumo da fórmula:

- **Numerador (retorno):** soma de `hit_rate` + `cagr` (ambos clipados a ±3σ e normalizados 0-1; nulls = 0).
- **Denominador (risco):** **multiplicação** de dois subgrupos, cada um soma normalizada de duas métricas:
  - **Qualidade do veículo (fragilidade):** `equity` e `existing_time` invertidos (`1 − x_norm`) — alto PL/idade reduz risco.
  - **Volatilidade:** `cv_metric` + `max_drawdown` invertido.
- **Score = `retorno / risco` → outliers (`|z| > 3` no `score_raw`) viram 0 → minmax → × 100**. Quando `risco == 0`, `score_raw = 0` (guard de divisão por zero).

Detalhes de tratamento de nulls e outliers em `docs/data_contracts.md` (seção Gold layer).

## Sumário do score (universo elegível)

- Min / Mediana / Média / Max: **0.00** / **4.43** / **11.55** / **100.00**

| Bucket | Fundos | % |
|---|---:|---:|
| 0–20 | 617 | 84.99% |
| 20–40 | 35 | 4.82% |
| 40–60 | 41 | 5.65% |
| 60–80 | 19 | 2.62% |
| 80–100 | 14 | 1.93% |

---

## Top-5 por perfil de investidor

Cada perfil enxerga o pool dos fundos cujo `publico_alvo` ele pode acessar (hierarquia CVM padrão — Profissional ⊃ Qualificado ⊃ Geral):

- **Geral** → só fundos com `publico_alvo = "Público Geral"`.
- **Qualificado** → fundos `"Público Geral"` + `"Qualificado"`.
- **Profissional** → todos os tipos (`"Público Geral"` + `"Qualificado"` + `"Profissional"`).

Fundos sem `publico_alvo` declarado (`null`) ficam fora das três listas.

## Perfil: **Geral**

- Pode investir em: "Público Geral".
- Universo elegível neste perfil: **660** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | CAGR | Hit rate | CV | Max drawdown | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | `50803936000129` | CAIXA AUTOMÁTICO POLIS FIC DE CLASSE DE FIF RENDA FIXA CURTO PRAZO - RESPONSABILIDADE LIMITADA | Renda Fixa Duração Baixa Soberano | CDI | R$ 28.02B | 11,663 | 243 | +9.11% | +0.00% | 0.20 | +0.00% | **100.00** |
| 2 | `43617343000102` | BB RENDA FIXA SIMPLES ÁGIL FUNDO DE INVESTIMENTO EM COTAS DE FIF RESP LIMITADA | Renda Fixa Simples | CDI | R$ 15.85B | 435,203 | 300 | +10.49% | +0.00% | 0.16 | +0.00% | **99.95** |
| 3 | `42592315000115` | BB RENDA FIXA CURTO PRAZO AUTOMÁTICO FIC FIF RESPONSABILIDADE LIMITADA | Renda Fixa Duração Baixa Soberano | CDI | R$ 172.52B | 82,804 | 231 | +9.38% | +0.00% | 0.19 | +0.00% | **97.19** |
| 4 | `26199519000134` | ITAÚ PRIVILÈGE RENDA FIXA REFERENCIADO DI FIF DA CIC RESP LIMITADA | Renda Fixa Duração Baixa Grau de Investimento | CDI | R$ 84.50B | 462,940 | 541 | +8.90% | +0.00% | 0.48 | -0.76% | **96.95** |
| 5 | `59376795000180` | PRINZ LIQUIDEZ FUNDO DE INVESTIMENTO EM RENDA FIXA CRÉDITO PRIVADO | Renda Fixa Duração Baixa Crédito Livre | CDI | R$ 201.06M | 1,384 | 261 | +15.37% | +0.00% | 0.07 | +0.00% | **91.15** |

## Perfil: **Qualificado**

- Pode investir em: "Público Geral", "Qualificado".
- Universo elegível neste perfil: **712** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | CAGR | Hit rate | CV | Max drawdown | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | `50803936000129` | CAIXA AUTOMÁTICO POLIS FIC DE CLASSE DE FIF RENDA FIXA CURTO PRAZO - RESPONSABILIDADE LIMITADA | Renda Fixa Duração Baixa Soberano | CDI | R$ 28.02B | 11,663 | 243 | +9.11% | +0.00% | 0.20 | +0.00% | **100.00** |
| 2 | `43617343000102` | BB RENDA FIXA SIMPLES ÁGIL FUNDO DE INVESTIMENTO EM COTAS DE FIF RESP LIMITADA | Renda Fixa Simples | CDI | R$ 15.85B | 435,203 | 300 | +10.49% | +0.00% | 0.16 | +0.00% | **99.95** |
| 3 | `42592315000115` | BB RENDA FIXA CURTO PRAZO AUTOMÁTICO FIC FIF RESPONSABILIDADE LIMITADA | Renda Fixa Duração Baixa Soberano | CDI | R$ 172.52B | 82,804 | 231 | +9.38% | +0.00% | 0.19 | +0.00% | **97.19** |
| 4 | `26199519000134` | ITAÚ PRIVILÈGE RENDA FIXA REFERENCIADO DI FIF DA CIC RESP LIMITADA | Renda Fixa Duração Baixa Grau de Investimento | CDI | R$ 84.50B | 462,940 | 541 | +8.90% | +0.00% | 0.48 | -0.76% | **96.95** |
| 5 | `61141240000109` | BB RENDA FIXA INFRA LINEAR FUNDO DE INVESTIMENTO EM COTAS DE FIF EM INFRAESTRUTURA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 3.24B | 7,924 | 173 | +14.63% | +0.00% | 0.07 | +0.00% | **91.73** |

## Perfil: **Profissional**

- Pode investir em: "Público Geral", "Qualificado", "Profissional".
- Universo elegível neste perfil: **714** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | CAGR | Hit rate | CV | Max drawdown | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | `50803936000129` | CAIXA AUTOMÁTICO POLIS FIC DE CLASSE DE FIF RENDA FIXA CURTO PRAZO - RESPONSABILIDADE LIMITADA | Renda Fixa Duração Baixa Soberano | CDI | R$ 28.02B | 11,663 | 243 | +9.11% | +0.00% | 0.20 | +0.00% | **100.00** |
| 2 | `43617343000102` | BB RENDA FIXA SIMPLES ÁGIL FUNDO DE INVESTIMENTO EM COTAS DE FIF RESP LIMITADA | Renda Fixa Simples | CDI | R$ 15.85B | 435,203 | 300 | +10.49% | +0.00% | 0.16 | +0.00% | **99.95** |
| 3 | `42592315000115` | BB RENDA FIXA CURTO PRAZO AUTOMÁTICO FIC FIF RESPONSABILIDADE LIMITADA | Renda Fixa Duração Baixa Soberano | CDI | R$ 172.52B | 82,804 | 231 | +9.38% | +0.00% | 0.19 | +0.00% | **97.19** |
| 4 | `26199519000134` | ITAÚ PRIVILÈGE RENDA FIXA REFERENCIADO DI FIF DA CIC RESP LIMITADA | Renda Fixa Duração Baixa Grau de Investimento | CDI | R$ 84.50B | 462,940 | 541 | +8.90% | +0.00% | 0.48 | -0.76% | **96.95** |
| 5 | `61141240000109` | BB RENDA FIXA INFRA LINEAR FUNDO DE INVESTIMENTO EM COTAS DE FIF EM INFRAESTRUTURA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 3.24B | 7,924 | 173 | +14.63% | +0.00% | 0.07 | +0.00% | **91.73** |
