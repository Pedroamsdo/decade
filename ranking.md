# Fund Ranking — Renda Fixa (as_of = 2025-12-31)

## Filtro de elegibilidade

Apenas fundos com `situacao = "Em Funcionamento Normal"`. **5,812** elegíveis de **5,849** totais (37 excluídos por situação).

## Como o score é calculado

- **Numerador (retorno):** `hit_rate vs benchmark` + `1 − σ(Sharpe rolling 12m)` + `liquid_return_12m`. Cada coluna passa por clip 3σ → minmax 0-1 → soma → minmax.
- **Denominador (risco):** média geométrica de três subgrupos, **re-normalizada 0-1** antes da divisão:
  - **Qualidade do veículo (fragilidade):** `equity`, `existing_time`, `net_captation` invertidos (`1 − x_norm`) — alto PL/idade/captação reduzem risco.
  - **Liquidez (impedimento):** `anbima_risk_weight` + `redemption_days`.
  - **Volatilidade:** `standard_deviation_annualized` + `|max_drawdown|`.
- **Score = `retorno / (risco_norm + 0.01)` → minmax → × 100**, faixa 0–100.

Tratamento de nulls: zero no numerador (penaliza ausência), um no denominador (fragilidade máxima — evita premiar quem não tem dado).

## Sumário do score (universo elegível)

- Min / Mediana / Média / Max: **0.00** / **2.27** / **3.83** / **100.00**

| Bucket | Fundos | % |
|---|---:|---:|
| 0–20 | 5,681 | 97.75% |
| 20–40 | 40 | 0.69% |
| 40–60 | 78 | 1.34% |
| 60–80 | 12 | 0.21% |
| 80–100 | 1 | 0.02% |

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
- Universo elegível neste perfil: **2,304** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Idade (d) | Retorno 12m | Hit rate | Max drawdown | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| 1 | `57463732000135` | FIBRA LIQUIDEZ DI FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 108.57M | 405 | +14.52% | +0.00% | +0.00% | **68.81** |
| 2 | `54996629000162` | BTG PACTUAL TESOURO SELIC BLACK FUNDO DE INVESTIMENTO RENDA FIXA - SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 1.33B | 603 | +14.39% | +0.00% | +0.00% | **64.65** |
| 3 | `55136061000172` | BRB RENDE MAIS TESOURO FUNDO DE INVESTIMENTO EM COTAS DE FUNDO DE INVESTIMENTOS EM RENDA FIXA SIMPLE | Renda Fixa Simples | CDI | R$ 83.51M | 551 | +13.40% | +0.00% | +0.00% | **63.98** |
| 4 | `54603259000156` | BB RENDA FIXA SIMPLES RESERVA FUNDO DE INVESTIMENTO EM COTAS DE FIF RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 856.54M | 555 | +13.41% | +0.00% | -0.20% | **63.97** |
| 5 | `55372147000102` (sub `SG3VT1727814030`) | SUBCLASSE I DA CLASSE DO GUILU SIMPLES SELIC FIF DA CIC RENDA FIXA RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 53.09K | 456 | +14.34% | +0.00% | +0.00% | **63.44** |

## Perfil: **Profissional**

- Pode investir em: "Público Geral", "Profissional".
- Universo elegível neste perfil: **5,030** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Idade (d) | Retorno 12m | Hit rate | Max drawdown | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| 1 | `42922097000130` | NEST TOUCAN FUNDO DE INVESTIMENTO FINANCEIRO | Renda Fixa Simples | IPCA | R$ 3.05M | 188 | +14.68% | +89.36% | -0.07% | **100.00** |
| 2 | `57463732000135` | FIBRA LIQUIDEZ DI FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 108.57M | 405 | +14.52% | +0.00% | +0.00% | **68.81** |
| 3 | `54996629000162` | BTG PACTUAL TESOURO SELIC BLACK FUNDO DE INVESTIMENTO RENDA FIXA - SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 1.33B | 603 | +14.39% | +0.00% | +0.00% | **64.65** |
| 4 | `54487736000165` | SANTANDER HERA RENDA FIXA FUNDO INCENTIVADO DE INVESTIMENTO EM INFRAESTRUTURA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 19.31B | 579 | +12.67% | +0.00% | +0.00% | **64.00** |
| 5 | `55136061000172` | BRB RENDE MAIS TESOURO FUNDO DE INVESTIMENTO EM COTAS DE FUNDO DE INVESTIMENTOS EM RENDA FIXA SIMPLE | Renda Fixa Simples | CDI | R$ 83.51M | 551 | +13.40% | +0.00% | +0.00% | **63.98** |

## Perfil: **Qualificado**

- Pode investir em: "Público Geral", "Qualificado", "Profissional".
- Universo elegível neste perfil: **5,725** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Idade (d) | Retorno 12m | Hit rate | Max drawdown | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| 1 | `42922097000130` | NEST TOUCAN FUNDO DE INVESTIMENTO FINANCEIRO | Renda Fixa Simples | IPCA | R$ 3.05M | 188 | +14.68% | +89.36% | -0.07% | **100.00** |
| 2 | `57463732000135` | FIBRA LIQUIDEZ DI FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 108.57M | 405 | +14.52% | +0.00% | +0.00% | **68.81** |
| 3 | `57341740000109` | TREND PE XXVII FUNDO DE INVESTIMENTO EM COTAS RENDA FIXA SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 77.20M | 413 | +14.32% | +0.00% | +0.00% | **68.74** |
| 4 | `55342176000113` | TREND PE XXVI FUNDO DE INVESTIMENTO EM COTAS RENDA FIXA SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 72.16M | 546 | +14.26% | +0.00% | -0.00% | **64.72** |
| 5 | `54996629000162` | BTG PACTUAL TESOURO SELIC BLACK FUNDO DE INVESTIMENTO RENDA FIXA - SIMPLES RESPONSABILIDADE LIMITADA | Renda Fixa Simples | CDI | R$ 1.33B | 603 | +14.39% | +0.00% | +0.00% | **64.65** |
