# Fund Ranking — Renda Fixa (as_of = 2025-12-31)

## Filtro de elegibilidade

Aplicados em sequência:

1. `situacao = "Em Funcionamento Normal"` → **5,812** de **5,849** (37 excluídos).
2. `nr_cotst > 100` → **1,359** de **5,812** (4,453 excluídos por terem ≤ 100 cotistas, incluindo fundos sem cotas em quota_series com `nr_cotst = 0`).

**Universo final do ranking: 1,359 fundos.**

## Como o score é calculado

- **Numerador (retorno):** `hit_rate vs benchmark` + `1 − σ(Sharpe rolling 12m)` + `liquid_return_12m`. Cada coluna passa por clip 3σ → minmax 0-1 → soma → minmax.
- **Denominador (risco):** média geométrica de três subgrupos, **re-normalizada 0-1** antes da divisão:
  - **Qualidade do veículo (fragilidade):** `equity`, `existing_time`, `net_captation` invertidos (`1 − x_norm`) — alto PL/idade/captação reduzem risco.
  - **Liquidez (impedimento):** `anbima_risk_weight` + `redemption_days`.
  - **Volatilidade:** `standard_deviation_annualized` + `|max_drawdown|`.
- **Score = `retorno / (risco_norm + 0.01)` → minmax → × 100**, faixa 0–100.

Tratamento de nulls: zero no numerador (penaliza ausência), um no denominador (fragilidade máxima — evita premiar quem não tem dado).

## Sumário do score (universo elegível)

- Min / Mediana / Média / Max: **0.10** / **2.91** / **6.02** / **68.74**

| Bucket | Fundos | % |
|---|---:|---:|
| 0–20 | 1,275 | 93.82% |
| 20–40 | 31 | 2.28% |
| 40–60 | 46 | 3.38% |
| 60–80 | 7 | 0.52% |
| 80–100 | 0 | 0.00% |

---

## Top-5 por perfil de investidor

Cada perfil enxerga o pool dos fundos cujo `publico_alvo` ele pode acessar:

- **Geral** → só fundos com `publico_alvo = "Público Geral"`.
- **Profissional** → fundos `"Público Geral"` + `"Profissional"`.
- **Qualificado** → todos os tipos (`"Público Geral"` + `"Qualificado"` + `"Profissional"`).

_(Hierarquia conforme pedida no enunciado; difere da regra padrão CVM, em que Profissional é o topo.)_

Fundos sem `publico_alvo` declarado (`null`) ficam fora das três listas.

## Perfil: **Geral**

- Pode investir em: "Público Geral".
- Universo elegível neste perfil: **1,132** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | Retorno 12m | Hit rate | Max drawdown | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | `54996629000162` | BTG PACTUAL TESOURO SELIC BLACK FUNDO DE INVESTIMENTO RENDA FIXA - SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 1.33B | 5,517 | 603 | +14.39% | +0.00% | +0.00% | **64.65** |
| 2 | `55136061000172` | BRB RENDE MAIS TESOURO FUNDO DE INVESTIMENTO EM COTAS DE FUNDO DE INVESTIMENTOS EM RENDA FIXA SIMPLE | Renda Fixa Simples | CDI | R$ 83.51M | 1,433 | 551 | +13.40% | +0.00% | +0.00% | **63.98** |
| 3 | `54603259000156` | BB RENDA FIXA SIMPLES RESERVA FUNDO DE INVESTIMENTO EM COTAS DE FIF RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 856.54M | 242,248 | 555 | +13.41% | +0.00% | -0.20% | **63.97** |
| 4 | `51811576000170` | INTER SIMPLES FUNDO DE INVESTIMENTO RENDA FIXA RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 200.97M | 19,174 | 664 | +14.22% | +0.00% | +0.00% | **58.58** |
| 5 | `45088717000175` | TREND INB RICO FUNDO DE INVESTIMENTO EM COTAS RENDA FIXA SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 27.03M | 90,941 | 253 | +14.33% | +0.00% | +0.00% | **53.02** |

## Perfil: **Profissional**

- Pode investir em: "Público Geral", "Profissional".
- Universo elegível neste perfil: **1,169** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | Retorno 12m | Hit rate | Max drawdown | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | `54996629000162` | BTG PACTUAL TESOURO SELIC BLACK FUNDO DE INVESTIMENTO RENDA FIXA - SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 1.33B | 5,517 | 603 | +14.39% | +0.00% | +0.00% | **64.65** |
| 2 | `55136061000172` | BRB RENDE MAIS TESOURO FUNDO DE INVESTIMENTO EM COTAS DE FUNDO DE INVESTIMENTOS EM RENDA FIXA SIMPLE | Renda Fixa Simples | CDI | R$ 83.51M | 1,433 | 551 | +13.40% | +0.00% | +0.00% | **63.98** |
| 3 | `54603259000156` | BB RENDA FIXA SIMPLES RESERVA FUNDO DE INVESTIMENTO EM COTAS DE FIF RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 856.54M | 242,248 | 555 | +13.41% | +0.00% | -0.20% | **63.97** |
| 4 | `51811576000170` | INTER SIMPLES FUNDO DE INVESTIMENTO RENDA FIXA RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 200.97M | 19,174 | 664 | +14.22% | +0.00% | +0.00% | **58.58** |
| 5 | `45088717000175` | TREND INB RICO FUNDO DE INVESTIMENTO EM COTAS RENDA FIXA SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 27.03M | 90,941 | 253 | +14.33% | +0.00% | +0.00% | **53.02** |

## Perfil: **Qualificado**

- Pode investir em: "Público Geral", "Qualificado", "Profissional".
- Universo elegível neste perfil: **1,339** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | Retorno 12m | Hit rate | Max drawdown | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | `57341740000109` | TREND PE XXVII FUNDO DE INVESTIMENTO EM COTAS RENDA FIXA SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 77.20M | 5,528 | 413 | +14.32% | +0.00% | +0.00% | **68.74** |
| 2 | `55342176000113` | TREND PE XXVI FUNDO DE INVESTIMENTO EM COTAS RENDA FIXA SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 72.16M | 2,264 | 546 | +14.26% | +0.00% | -0.00% | **64.72** |
| 3 | `54996629000162` | BTG PACTUAL TESOURO SELIC BLACK FUNDO DE INVESTIMENTO RENDA FIXA - SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 1.33B | 5,517 | 603 | +14.39% | +0.00% | +0.00% | **64.65** |
| 4 | `55136061000172` | BRB RENDE MAIS TESOURO FUNDO DE INVESTIMENTO EM COTAS DE FUNDO DE INVESTIMENTOS EM RENDA FIXA SIMPLE | Renda Fixa Simples | CDI | R$ 83.51M | 1,433 | 551 | +13.40% | +0.00% | +0.00% | **63.98** |
| 5 | `54603259000156` | BB RENDA FIXA SIMPLES RESERVA FUNDO DE INVESTIMENTO EM COTAS DE FIF RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 856.54M | 242,248 | 555 | +13.41% | +0.00% | -0.20% | **63.97** |
