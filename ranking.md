# Fund Ranking — Renda Fixa (as_of = 2025-12-31)

## Como o score é calculado

Composto de **duas métricas** vs o benchmark canônico do fundo (CDI / IPCA / IMA-B / etc., mapeado em `silver/_benchmark_mapping`), alinhado ao framework CFA L3 para seleção de fundos de renda fixa:

```
excess[t]        = monthly_ret_fund[t] − monthly_ret_bench[t]

IR_anualizado    = mean(excess) / std(excess) × √12              # peso 0.7
Sortino_anual    = mean(excess) × 12 / (std(min(excess, 0)) × √12)  # peso 0.3

composite        = 0.7 × z(IR) + 0.3 × z(Sortino)   # z-score sobre o universo elegível
score            = percentile_rank(composite) × 100
```

**Por que duas métricas.** O IR mede consistência de alpha, mas trata vol de upside e downside igualmente — ignora a assimetria típica de retornos de RF (eventos de crédito, choques de duration). O Sortino penaliza apenas vol negativa, capturando o risco de cauda esquerda. Os pesos (70/30) priorizam consistência de alpha, mas descontam fundos com drawdowns severos.

**Elegibilidade.** Score só é calculado para fundos com `situacao = "Em Funcionamento Normal"`, `nr_cotst > 1,000`, `existing_time ≥ 252` dias e `equity ≥ R$ 50,000,000` (404 de 5,835 fundos passam).

---

## Top-5 por perfil de investidor

Hierarquia CVM padrão (Profissional ⊃ Qualificado ⊃ Geral): **Geral** vê só `"Público Geral"`; **Qualificado** vê `"Público Geral"` + `"Qualificado"`; **Profissional** vê todos os tipos.

## Perfil: **Geral**

- Pode investir em: "Público Geral".
- Universo elegível neste perfil: **359** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | IR (anual) | Sortino (anual) | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| 1 | `39586858000115` (sub `KB0OK1743799805`) | SUBCLASSE I DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 63.90M | 7,445 | 264 | 5.19 | 2150.58 | **100.00** |
| 2 | `39586858000115` (sub `M0UTC1743800258`) | SUBCLASSE II DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 250.60M | 8,183 | 264 | 5.19 | 2060.16 | **99.75** |
| 3 | `57832914000136` | A1 HIGH GRADE PÓS FIXADO FEEDER 2 FIF DA CIC RENDA FIXA CRÉDITO PRIVADO RESPONSABILIDADE LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 384.99M | 2,120 | 397 | 3.49 | 911.82 | **99.50** |
| 4 | `23970201000117` | OURO PRETO REAL FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO RENDA FIXA LONGO PRAZO | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 185.37M | 2,768 | 317 | 5.39 | 85.74 | **99.26** |
| 5 | `49272086000109` | KILIMA BANCOS FUNDO DE INVESTIMENTO EM RENDA FIXA CRÉDITO PRIVADO RESPONSABILIDADE LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 331.82M | 1,399 | 631 | 2.87 | 179.24 | **99.01** |

## Perfil: **Qualificado**

- Pode investir em: "Público Geral", "Qualificado".
- Universo elegível neste perfil: **393** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | IR (anual) | Sortino (anual) | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| 1 | `39586858000115` (sub `KB0OK1743799805`) | SUBCLASSE I DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 63.90M | 7,445 | 264 | 5.19 | 2150.58 | **100.00** |
| 2 | `39586858000115` (sub `M0UTC1743800258`) | SUBCLASSE II DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 250.60M | 8,183 | 264 | 5.19 | 2060.16 | **99.75** |
| 3 | `57832914000136` | A1 HIGH GRADE PÓS FIXADO FEEDER 2 FIF DA CIC RENDA FIXA CRÉDITO PRIVADO RESPONSABILIDADE LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 384.99M | 2,120 | 397 | 3.49 | 911.82 | **99.50** |
| 4 | `23970201000117` | OURO PRETO REAL FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO RENDA FIXA LONGO PRAZO | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 185.37M | 2,768 | 317 | 5.39 | 85.74 | **99.26** |
| 5 | `49272086000109` | KILIMA BANCOS FUNDO DE INVESTIMENTO EM RENDA FIXA CRÉDITO PRIVADO RESPONSABILIDADE LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 331.82M | 1,399 | 631 | 2.87 | 179.24 | **99.01** |

## Perfil: **Profissional**

- Pode investir em: "Público Geral", "Qualificado", "Profissional".
- Universo elegível neste perfil: **394** fundos.

| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | IR (anual) | Sortino (anual) | Score |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| 1 | `39586858000115` (sub `KB0OK1743799805`) | SUBCLASSE I DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 63.90M | 7,445 | 264 | 5.19 | 2150.58 | **100.00** |
| 2 | `39586858000115` (sub `M0UTC1743800258`) | SUBCLASSE II DO KINEA IPCA DINÂMICO II FUNDO DE INVESTIMENTO FINANCEIRO RENDA FIXA RESP LIMITADA | Renda Fixa Duração Livre Crédito Livre | IPCA | R$ 250.60M | 8,183 | 264 | 5.19 | 2060.16 | **99.75** |
| 3 | `57832914000136` | A1 HIGH GRADE PÓS FIXADO FEEDER 2 FIF DA CIC RENDA FIXA CRÉDITO PRIVADO RESPONSABILIDADE LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 384.99M | 2,120 | 397 | 3.49 | 911.82 | **99.50** |
| 4 | `23970201000117` | OURO PRETO REAL FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO RENDA FIXA LONGO PRAZO | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 185.37M | 2,768 | 317 | 5.39 | 85.74 | **99.26** |
| 5 | `49272086000109` | KILIMA BANCOS FUNDO DE INVESTIMENTO EM RENDA FIXA CRÉDITO PRIVADO RESPONSABILIDADE LIMITADA | Renda Fixa Duração Livre Crédito Livre | CDI | R$ 331.82M | 1,399 | 631 | 2.87 | 179.24 | **99.01** |
