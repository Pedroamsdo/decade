"""Generate `ranking.md` (Markdown) from `gold/fund_metrics`.

Filters: `situacao = "Em Funcionamento Normal"` AND `nr_cotst > 100`.
Output: Top-N per `publico_alvo` profile (Geral, Profissional, Qualificado),
with the hierarchical eligibility rule (Geral sees only "Público Geral"; etc.).

The score column is already computed inside `gold/fund_metrics`. This module
only filters, joins denominação/benchmark from the silver treated tables,
sorts and renders Markdown.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.gold._io import gold_path
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings

log = get_logger(__name__)


# Hierarquia de elegibilidade (regra padrão CVM — Profissional ⊃ Qualificado ⊃ Geral):
#   - Investidor "Geral" pode investir SOMENTE em fundos Público Geral.
#   - Investidor "Qualificado" pode investir em Público Geral + Qualificado.
#   - Investidor "Profissional" pode investir em todos os tipos.
PROFILES: list[tuple[str, list[str]]] = [
    ("Geral", ["Público Geral"]),
    ("Qualificado", ["Público Geral", "Qualificado"]),
    ("Profissional", ["Público Geral", "Qualificado", "Profissional"]),
]


def _format_money(v: float | None) -> str:
    if v is None:
        return ""
    if abs(v) >= 1e9:
        return f"R$ {v / 1e9:.2f}B"
    if abs(v) >= 1e6:
        return f"R$ {v / 1e6:.2f}M"
    if abs(v) >= 1e3:
        return f"R$ {v / 1e3:.2f}K"
    return f"R$ {v:,.2f}"


def _format_pct(v: float | None) -> str:
    return "" if v is None else f"{v * 100:+.2f}%"


def _format_int(v: int | None) -> str:
    return "" if v is None else f"{v:,}"


def _format_score(v: float | None) -> str:
    return "" if v is None else f"{v:.2f}"


def _format_str(v: str | None) -> str:
    return v if v else ""


def _format_vol(v: float | None) -> str:
    return "" if v is None else f"{v * 100:.2f}%"


def _fund_label(row: dict) -> str:
    cnpj = row["cnpj_classe"]
    if row.get("id_subclasse_cvm"):
        return f"`{cnpj}` (sub `{row['id_subclasse_cvm']}`)"
    return f"`{cnpj}`"


def _fund_name(row: dict) -> str:
    return _format_str(row.get("nome"))


def _profile_section(
    label: str, accessible_values: list[str], eligible: pl.DataFrame, top_n: int
) -> list[str]:
    section: list[str] = []
    profile_df = eligible.filter(pl.col("publico_alvo").is_in(accessible_values))
    total = profile_df.height
    accessible_repr = ", ".join(f'"{v}"' for v in accessible_values)
    section.append(f"## Perfil: **{label}**\n")
    section.append(
        f"- Pode investir em: {accessible_repr}.\n"
        f"- Universo elegível neste perfil: **{total:,}** fundos."
    )
    section.append("")
    if total == 0:
        section.append("_Nenhum fundo elegível neste perfil._\n")
        return section

    top = profile_df.sort("score", descending=True).head(top_n)

    section.append(
        "| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Tributação | Equity | Cotistas | Idade (d) | IR (anual) | Sortino (anual) | Tax eff. | Score |"
    )
    section.append(
        "|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|"
    )
    for i, row in enumerate(top.iter_rows(named=True), start=1):
        section.append(
            "| {i} | {fund} | {name} | {anbima} | {bench} | {trib} | {eq} | {cot} | {age} | {ir} | {sortino} | {tax} | **{sc}** |".format(
                i=i,
                fund=_fund_label(row),
                name=_fund_name(row),
                anbima=_format_str(row.get("anbima_classification")),
                bench=_format_str(row.get("benchmark_canonico")),
                trib=_format_str(row.get("tributacao_alvo")),
                eq=_format_money(row["equity"]),
                cot=_format_int(row.get("nr_cotst")),
                age=_format_int(row["existing_time"]),
                ir=_format_score(row.get("information_ratio")),
                sortino=_format_score(row.get("sortino_ratio")),
                tax=_format_score(row.get("tax_efficiency")),
                sc=_format_score(row["score"]),
            )
        )
    section.append("")
    return section


def run(settings: Settings, as_of: date, top_n: int | None = None) -> Path:
    if top_n is None:
        top_n = settings.scoring.selection.top_n
    in_path = gold_path(settings, "fund_metrics", as_of.isoformat())
    if not in_path.exists():
        raise FileNotFoundError(
            f"gold/fund_metrics not found at {in_path}; run build_fund_metrics first."
        )

    df = pl.read_parquet(in_path)

    # Attach canonical benchmark + display name + classificação ANBIMA from silver treated tables.
    cls_path = settings.silver_root / "class_funds_fixed_income_treated" / f"as_of={as_of.isoformat()}" / "data.parquet"
    sub_path = settings.silver_root / "subclass_funds_fixed_income_treated" / f"as_of={as_of.isoformat()}" / "data.parquet"
    cls_attrs = pl.read_parquet(cls_path).select(
        pl.col("cnpj_classe"),
        pl.col("benchmark").alias("benchmark_canonico"),
        pl.col("denom_social_classe").alias("nome_classe"),
        pl.col("classificacao_anbima").alias("anbima_classification_cls"),
    )
    sub_attrs = pl.read_parquet(sub_path).select(
        pl.col("id_subclasse_cvm"),
        pl.col("benchmark").alias("benchmark_canonico_sub"),
        pl.col("denom_social_subclasse").alias("nome_subclasse"),
        pl.col("classificacao_anbima").alias("anbima_classification_sub"),
    )
    df = (
        df.join(cls_attrs, on="cnpj_classe", how="left")
        .join(sub_attrs, on="id_subclasse_cvm", how="left")
        .with_columns(
            benchmark_canonico=pl.coalesce(
                "benchmark_canonico_sub", "benchmark_canonico"
            ),
            nome=pl.coalesce("nome_subclasse", "nome_classe"),
            anbima_classification=pl.coalesce(
                "anbima_classification_sub", "anbima_classification_cls"
            ),
        )
        .drop(
            "benchmark_canonico_sub",
            "nome_subclasse",
            "nome_classe",
            "anbima_classification_sub",
            "anbima_classification_cls",
        )
    )

    total = df.height
    eligible = df.filter(pl.col("score").is_not_null())

    elig = settings.scoring.eligibility
    lines: list[str] = []
    lines.append(f"# Fund Ranking — Renda Fixa (as_of = {as_of.isoformat()})\n")

    lines.append("## Como o score é calculado\n")
    lines.append(
        "Composto de **três métricas** alinhado ao framework CFA L3 para seleção "
        "de fundos de renda fixa: duas mensuram retorno ajustado a risco vs o "
        "benchmark canônico do fundo (CDI / IPCA / IMA-B / etc., mapeado em "
        "`silver/_benchmark_mapping`); a terceira ajusta pelo imposto de renda "
        "efetivo no resgate."
    )
    lines.append("")
    lines.append("```")
    lines.append("excess[t]        = monthly_ret_fund[t] − monthly_ret_bench[t]")
    lines.append("")
    lines.append("IR_anualizado    = mean(excess) / std(excess) × √12                  # peso 0.60")
    lines.append("Sortino_anual    = mean(excess) × 12 / (std(min(excess, 0)) × √12)   # peso 0.25")
    lines.append("tax_efficiency   = 1 − alíquota_efetiva(tributacao_alvo)             # peso 0.15")
    lines.append("")
    lines.append("composite        = 0.60 × z(IR) + 0.25 × z(Sortino) + 0.15 × z(tax_efficiency)")
    lines.append("                   # z-score sobre o universo elegível")
    lines.append("score            = percentile_rank(composite) × 100")
    lines.append("```")
    lines.append("")
    lines.append(
        "**Por que três métricas.** O IR mede consistência de alpha, mas trata "
        "vol de upside e downside igualmente — ignora a assimetria típica de "
        "retornos de RF (eventos de crédito, choques de duration). O Sortino "
        "penaliza apenas vol negativa, capturando o risco de cauda esquerda. "
        "O `tax_efficiency` desconta o imposto de renda no resgate (Isento → 1.00, "
        "Longo Prazo → 0.85, Curto Prazo → 0.80, Previdenciário → 0.90, etc., "
        "configurável em `scoring.yaml#tax`), garantindo que o score reflita o "
        "retorno líquido que o investidor leva para casa, não o bruto. Pesos "
        "60/25/15 priorizam alpha e downside como tese principal, com tributação "
        "como modificador determinístico (não risco)."
    )
    lines.append("")
    lines.append(
        f"**Elegibilidade.** Score só é calculado para fundos com "
        f"`situacao = \"{elig.situacao}\"`, `nr_cotst > {elig.nr_cotst_min:,}`, "
        f"`existing_time ≥ {elig.existing_time_min_days}` dias e "
        f"`equity ≥ R$ {elig.equity_min_brl:,.0f}` "
        f"({eligible.height:,} de {total:,} fundos passam)."
    )
    lines.append("")

    lines.append("---\n")
    lines.append("## Top-5 por perfil de investidor\n")
    lines.append(
        "Hierarquia CVM padrão (Profissional ⊃ Qualificado ⊃ Geral): "
        "**Geral** vê só `\"Público Geral\"`; **Qualificado** vê `\"Público Geral\"` + `\"Qualificado\"`; "
        "**Profissional** vê todos os tipos."
    )
    lines.append("")
    for label, accessible in PROFILES:
        lines.extend(_profile_section(label, accessible, eligible, top_n=top_n))

    out = Path("ranking.md")
    out.write_text("\n".join(lines))
    log.info(
        "gold.ranking_report.written",
        path=str(out),
        eligible=eligible.height,
        excluded=total - eligible.height,
    )
    return out
