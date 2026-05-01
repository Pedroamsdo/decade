# Fund Ranking — Renda Fixa (as_of = 2025-12-31)

## Como o score é calculado

Composto de **três métricas** alinhado ao framework CFA L3 para seleção de fundos de renda fixa: duas mensuram retorno ajustado a risco vs o benchmark canônico do fundo (CDI / IPCA / IMA-B / etc., mapeado em `silver/_benchmark_mapping`); a terceira ajusta pelo imposto de renda efetivo no resgate.

```
excess[t]        = monthly_ret_fund[t] − monthly_ret_bench[t]

IR_anualizado    = mean(excess) / std(excess) × √12                  # peso 0.60
Sortino_anual    = mean(excess) × 12 / (std(min(excess, 0)) × √12)   # peso 0.25
tax_efficiency   = 1 − alíquota_efetiva(tributacao_alvo)             # peso 0.15

composite        = 0.60 × z(IR) + 0.25 × z(Sortino) + 0.15 × z(tax_efficiency)
                   # z-score sobre o universo elegível
score            = percentile_rank(composite) × 100
```

**Por que três métricas.** O IR mede consistência de alpha, mas trata vol de upside e downside igualmente — ignora a assimetria típica de retornos de RF (eventos de crédito, choques de duration). O Sortino penaliza apenas vol negativa, capturando o risco de cauda esquerda. O `tax_efficiency` desconta o imposto de renda no resgate (Isento → 1.00, Longo Prazo → 0.85, Curto Prazo → 0.80, Previdenciário → 0.90, etc., configurável em `scoring.yaml#tax`), garantindo que o score reflita o retorno líquido que o investidor leva para casa, não o bruto. Pesos 60/25/15 priorizam alpha e downside como tese principal, com tributação como modificador determinístico (não risco).

**Elegibilidade.** Score só é calculado para fundos com `situacao = "Em Funcionamento Normal"`, `nr_cotst > 1,000`, `existing_time ≥ 252` dias e `equity ≥ R$ 50,000,000` (393 de 5,835 fundos passam).

---

## Top-5 por perfil de investidor

Hierarquia CVM padrão (Profissional ⊃ Qualificado ⊃ Geral): **Geral** vê só `"Público Geral"`; **Qualificado** vê `"Público Geral"` + `"Qualificado"`; **Profissional** vê todos os tipos.

## Perfil: **Geral**

- Pode investir em: "Público Geral".
- Universo elegível neste perfil: **348** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Tributação | Equity | Cotistas | Idade (d) | IR (anual) | Sortino (anual) | Tax eff. | Score |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | `39586858000115` (sub `KB0OK1743799805`) | SUBCLASSE I DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | Longo Prazo | R$ 63.90M | 7,445 | 264 | 5.19 | 2150.58 | 0.85 | **100.00** |
| 2 | `39586858000115` (sub `M0UTC1743800258`) | SUBCLASSE II DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | Longo Prazo | R$ 250.60M | 8,183 | 264 | 5.19 | 2060.16 | 0.85 | **99.75** |
| 3 | `57832914000136` | A1 HIGH GRADE PÓS FIXADO FEEDER 2 FIF DA CIC RENDA FIXA CRÉDITO PRIVADO RESPONSABILIDADE LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | Longo Prazo | R$ 384.99M | 2,120 | 397 | 3.49 | 911.82 | 0.85 | **99.49** |
| 4 | `23970201000117` | OURO PRETO REAL FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO RENDA FIXA LONGO PRAZO | Renda Fixa Duração Livre Crédito Livre | CDI | Longo Prazo | R$ 185.37M | 2,768 | 317 | 5.39 | 85.74 | 0.85 | **99.24** |
| 5 | `51112363000150` | ABSOLUTE HIDRA IPCA FI EM COTAS DE FUNDOS INCENTIVADOS DE INVESTIMENTO EM INFRA RENDA FIXA | Renda Fixa Duração Livre Crédito Livre | IPCA | Isento | R$ 228.41M | 3,503 | 439 | 2.69 | 8.14 | 1.00 | **98.98** |

## Perfil: **Qualificado**

- Pode investir em: "Público Geral", "Qualificado".
- Universo elegível neste perfil: **382** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Tributação | Equity | Cotistas | Idade (d) | IR (anual) | Sortino (anual) | Tax eff. | Score |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | `39586858000115` (sub `KB0OK1743799805`) | SUBCLASSE I DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | Longo Prazo | R$ 63.90M | 7,445 | 264 | 5.19 | 2150.58 | 0.85 | **100.00** |
| 2 | `39586858000115` (sub `M0UTC1743800258`) | SUBCLASSE II DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | Longo Prazo | R$ 250.60M | 8,183 | 264 | 5.19 | 2060.16 | 0.85 | **99.75** |
| 3 | `57832914000136` | A1 HIGH GRADE PÓS FIXADO FEEDER 2 FIF DA CIC RENDA FIXA CRÉDITO PRIVADO RESPONSABILIDADE LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | Longo Prazo | R$ 384.99M | 2,120 | 397 | 3.49 | 911.82 | 0.85 | **99.49** |
| 4 | `23970201000117` | OURO PRETO REAL FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO RENDA FIXA LONGO PRAZO | Renda Fixa Duração Livre Crédito Livre | CDI | Longo Prazo | R$ 185.37M | 2,768 | 317 | 5.39 | 85.74 | 0.85 | **99.24** |
| 5 | `51112363000150` | ABSOLUTE HIDRA IPCA FI EM COTAS DE FUNDOS INCENTIVADOS DE INVESTIMENTO EM INFRA RENDA FIXA | Renda Fixa Duração Livre Crédito Livre | IPCA | Isento | R$ 228.41M | 3,503 | 439 | 2.69 | 8.14 | 1.00 | **98.98** |

## Perfil: **Profissional**

- Pode investir em: "Público Geral", "Qualificado", "Profissional".
- Universo elegível neste perfil: **383** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Tributação | Equity | Cotistas | Idade (d) | IR (anual) | Sortino (anual) | Tax eff. | Score |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | `39586858000115` (sub `KB0OK1743799805`) | SUBCLASSE I DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | Longo Prazo | R$ 63.90M | 7,445 | 264 | 5.19 | 2150.58 | 0.85 | **100.00** |
| 2 | `39586858000115` (sub `M0UTC1743800258`) | SUBCLASSE II DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | Longo Prazo | R$ 250.60M | 8,183 | 264 | 5.19 | 2060.16 | 0.85 | **99.75** |
| 3 | `57832914000136` | A1 HIGH GRADE PÓS FIXADO FEEDER 2 FIF DA CIC RENDA FIXA CRÉDITO PRIVADO RESPONSABILIDADE LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | Longo Prazo | R$ 384.99M | 2,120 | 397 | 3.49 | 911.82 | 0.85 | **99.49** |
| 4 | `23970201000117` | OURO PRETO REAL FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO RENDA FIXA LONGO PRAZO | Renda Fixa Duração Livre Crédito Livre | CDI | Longo Prazo | R$ 185.37M | 2,768 | 317 | 5.39 | 85.74 | 0.85 | **99.24** |
| 5 | `51112363000150` | ABSOLUTE HIDRA IPCA FI EM COTAS DE FUNDOS INCENTIVADOS DE INVESTIMENTO EM INFRA RENDA FIXA | Renda Fixa Duração Livre Crédito Livre | IPCA | Isento | R$ 228.41M | 3,503 | 439 | 2.69 | 8.14 | 1.00 | **98.98** |
