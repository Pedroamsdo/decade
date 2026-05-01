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


def _format_cv(v: float | None) -> str:
    return "" if v is None else f"{v:.2f}"


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
        "| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Cotistas | Idade (d) | CAGR | Hit rate | CV | Max drawdown | Score |"
    )
    section.append(
        "|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"
    )
    for i, row in enumerate(top.iter_rows(named=True), start=1):
        section.append(
            "| {i} | {fund} | {name} | {anbima} | {bench} | {eq} | {cot} | {age} | {cagr} | {hit} | {cv} | {dd} | **{sc}** |".format(
                i=i,
                fund=_fund_label(row),
                name=_fund_name(row),
                # `anbima_classification` e `benchmark_canonico` vêm do silver
                # (treated tables) por join no `run()` — não estão em fund_metrics.
                anbima=_format_str(row.get("anbima_classification")),
                bench=_format_str(row.get("benchmark_canonico")),
                eq=_format_money(row["equity"]),
                cot=_format_int(row.get("nr_cotst")),
                age=_format_int(row["existing_time"]),
                cagr=_format_pct(row.get("cagr")),
                hit=_format_pct(row.get("hit_rate")),
                cv=_format_cv(row.get("cv_metric")),
                dd=_format_pct(row.get("max_drawdown")),
                sc=_format_score(row["score"]),
            )
        )
    section.append("")
    return section


def _summary_section(eligible: pl.DataFrame) -> list[str]:
    section: list[str] = []
    s = eligible["score"].drop_nulls()
    if s.len() == 0:
        return ["_Sem dados de score._\n"]

    section.append("## Sumário do score (universo elegível)\n")
    section.append(
        f"- Min / Mediana / Média / Max: **{s.min():.2f}** / **{s.median():.2f}** / "
        f"**{s.mean():.2f}** / **{s.max():.2f}**"
    )
    buckets = [(0, 20), (20, 40), (40, 60), (60, 80), (80, 100.01)]
    section.append("")
    section.append("| Bucket | Fundos | % |")
    section.append("|---|---:|---:|")
    for lo, hi in buckets:
        n = int(s.filter((s >= lo) & (s < hi)).len())
        pct = n / s.len() * 100.0
        hi_str = "100" if hi > 100 else f"{hi:g}"
        section.append(f"| {lo:g}–{hi_str} | {n:,} | {pct:.2f}% |")
    section.append("")
    return section


def run(settings: Settings, as_of: date, top_n: int = 5) -> Path:
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
    sit_ok = df.filter(pl.col("situacao") == "Em Funcionamento Normal")
    excluded_situacao = total - sit_ok.height
    eligible = sit_ok.filter(pl.col("nr_cotst") > 1000)
    excluded_cotistas = sit_ok.height - eligible.height

    lines: list[str] = []
    lines.append(f"# Fund Ranking — Renda Fixa (as_of = {as_of.isoformat()})\n")
    lines.append("## Filtro de elegibilidade\n")
    lines.append("Aplicados em sequência:\n")
    lines.append(
        f"1. `situacao = \"Em Funcionamento Normal\"` → "
        f"**{sit_ok.height:,}** de **{total:,}** "
        f"({excluded_situacao:,} excluídos)."
    )
    lines.append(
        f"2. `nr_cotst > 1000` → "
        f"**{eligible.height:,}** de **{sit_ok.height:,}** "
        f"({excluded_cotistas:,} excluídos por terem ≤ 1.000 cotistas)."
    )
    lines.append("")
    lines.append(
        f"**Universo final do ranking: {eligible.height:,} fundos.**\n"
    )

    lines.append("## Como o score é calculado\n")
    lines.append(
        "Lê `gold/fund_metrics` (a coluna `score` já vem calculada). Resumo da fórmula:"
    )
    lines.append("")
    lines.append(
        "- **Numerador (retorno):** soma de `hit_rate` + `cagr` (ambos clipados a ±3σ "
        "e normalizados 0-1; nulls = 0)."
    )
    lines.append(
        "- **Denominador (risco):** **multiplicação** de dois subgrupos, cada um "
        "soma normalizada de duas métricas:"
    )
    lines.append(
        "  - **Qualidade do veículo (fragilidade):** `equity` e `existing_time` "
        "invertidos (`1 − x_norm`) — alto PL/idade reduz risco."
    )
    lines.append(
        "  - **Volatilidade:** `cv_metric` + `max_drawdown` invertido."
    )
    lines.append(
        "- **Score = `retorno / risco` → outliers (`|z| > 3` no `score_raw`) viram 0 → "
        "minmax → × 100**. Quando `risco == 0`, `score_raw = 0` (guard de divisão por zero)."
    )
    lines.append("")
    lines.append(
        "Detalhes de tratamento de nulls e outliers em `docs/data_contracts.md` "
        "(seção Gold layer)."
    )
    lines.append("")

    lines.extend(_summary_section(eligible))

    lines.append("---\n")
    lines.append("## Top-5 por perfil de investidor\n")
    lines.append(
        "Cada perfil enxerga o pool dos fundos cujo `publico_alvo` ele pode acessar "
        "(hierarquia CVM padrão — Profissional ⊃ Qualificado ⊃ Geral):\n\n"
        "- **Geral** → só fundos com `publico_alvo = \"Público Geral\"`.\n"
        "- **Qualificado** → fundos `\"Público Geral\"` + `\"Qualificado\"`.\n"
        "- **Profissional** → todos os tipos (`\"Público Geral\"` + `\"Qualificado\"` + `\"Profissional\"`).\n\n"
        "Fundos sem `publico_alvo` declarado (`null`) ficam fora das três listas."
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
        excluded_situacao=excluded_situacao,
        excluded_cotistas=excluded_cotistas,
    )
    return out
