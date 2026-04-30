"""Generate `ranking.md` (Markdown) from `gold/ranking`.

Filters the universe to `situacao = "Em Funcionamento Normal"` and produces
a Top-10 per `publico_alvo` profile (Público Geral, Qualificado, Profissional).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.gold._io import gold_path
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings

log = get_logger(__name__)


# Hierarquia de elegibilidade pedida pelo usuário:
#   - Investidor "Geral" pode investir SOMENTE em fundos Público Geral.
#   - Investidor "Profissional" pode investir em Público Geral + Profissional.
#   - Investidor "Qualificado" pode investir em todos (Geral + Qualificado + Profissional).
#
# Note que isso difere da regra padrão CVM (Profissional > Qualificado > Geral) —
# está literalmente como pedido na conversa.
PROFILES: list[tuple[str, list[str]]] = [
    ("Geral", ["Público Geral"]),
    ("Profissional", ["Público Geral", "Profissional"]),
    ("Qualificado", ["Público Geral", "Qualificado", "Profissional"]),
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
        "| # | Fundo | Nome | Classificação ANBIMA | Benchmark | Equity | Idade (d) | Retorno 12m | Hit rate | Max drawdown | Score |"
    )
    section.append(
        "|---|---|---|---|---|---:|---:|---:|---:|---:|---:|"
    )
    for i, row in enumerate(top.iter_rows(named=True), start=1):
        section.append(
            "| {i} | {fund} | {name} | {anbima} | {bench} | {eq} | {age} | {ret} | {hit} | {dd} | **{sc}** |".format(
                i=i,
                fund=_fund_label(row),
                name=_fund_name(row),
                anbima=row["anbima_classification"],
                bench=_format_str(row.get("benchmark_canonico")),
                eq=_format_money(row["equity"]),
                age=_format_int(row["existing_time"]),
                ret=_format_pct(row["liquid_return_12m"]),
                hit=_format_pct(row["hit_rate"]),
                dd=_format_pct(row["max_drawdown"]),
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
    in_path = gold_path(settings, "ranking", as_of.isoformat())
    if not in_path.exists():
        raise FileNotFoundError(
            f"gold/ranking not found at {in_path}; run build_ranking first."
        )

    df = pl.read_parquet(in_path)

    # Attach canonical benchmark + display name (class denom for classes, subclass denom for subs).
    cls_path = settings.silver_root / "class_funds_fixed_income_treated" / f"as_of={as_of.isoformat()}" / "data.parquet"
    sub_path = settings.silver_root / "subclass_funds_fixed_income_treated" / f"as_of={as_of.isoformat()}" / "data.parquet"
    cls_attrs = pl.read_parquet(cls_path).select(
        pl.col("cnpj_classe"),
        pl.col("benchmark").alias("benchmark_canonico"),
        pl.col("denom_social_classe").alias("nome_classe"),
    )
    sub_attrs = pl.read_parquet(sub_path).select(
        pl.col("id_subclasse_cvm"),
        pl.col("benchmark").alias("benchmark_canonico_sub"),
        pl.col("denom_social_subclasse").alias("nome_subclasse"),
    )
    df = (
        df.join(cls_attrs, on="cnpj_classe", how="left")
        .join(sub_attrs, on="id_subclasse_cvm", how="left")
        .with_columns(
            benchmark_canonico=pl.coalesce(
                "benchmark_canonico_sub", "benchmark_canonico"
            ),
            nome=pl.coalesce("nome_subclasse", "nome_classe"),
        )
        .drop("benchmark_canonico_sub", "nome_subclasse", "nome_classe")
    )

    total = df.height
    eligible = df.filter(pl.col("situacao") == "Em Funcionamento Normal")
    excluded = total - eligible.height

    lines: list[str] = []
    lines.append(f"# Fund Ranking — Renda Fixa (as_of = {as_of.isoformat()})\n")
    lines.append("## Filtro de elegibilidade\n")
    lines.append(
        f'Apenas fundos com `situacao = "Em Funcionamento Normal"`. '
        f"**{eligible.height:,}** elegíveis de **{total:,}** totais "
        f"({excluded:,} excluídos por situação).\n"
    )
    lines.append("## Como o score é calculado\n")
    lines.append(
        "- **Numerador (retorno):** `hit_rate vs benchmark` + `1 − σ(Sharpe rolling 12m)` + "
        "`liquid_return_12m`. Cada coluna passa por clip 3σ → minmax 0-1 → soma → minmax."
    )
    lines.append(
        "- **Denominador (risco):** média geométrica de três subgrupos, "
        "**re-normalizada 0-1** antes da divisão:"
    )
    lines.append(
        "  - **Qualidade do veículo (fragilidade):** `equity`, `existing_time`, `net_captation` "
        "invertidos (`1 − x_norm`) — alto PL/idade/captação reduzem risco."
    )
    lines.append(
        "  - **Liquidez (impedimento):** `anbima_risk_weight` + `redemption_days`."
    )
    lines.append(
        "  - **Volatilidade:** `standard_deviation_annualized` + `|max_drawdown|`."
    )
    lines.append(
        "- **Score = `retorno / (risco_norm + 0.01)` → minmax → × 100**, faixa 0–100."
    )
    lines.append("")
    lines.append(
        "Tratamento de nulls: zero no numerador (penaliza ausência), um no denominador "
        "(fragilidade máxima — evita premiar quem não tem dado)."
    )
    lines.append("")

    lines.extend(_summary_section(eligible))

    lines.append("---\n")
    lines.append("## Top-5 por perfil de investidor\n")
    lines.append(
        "Cada perfil enxerga o pool dos fundos cujo `publico_alvo` ele pode acessar:\n\n"
        "- **Geral** → só fundos com `publico_alvo = \"Público Geral\"`.\n"
        "- **Profissional** → fundos `\"Público Geral\"` + `\"Profissional\"`.\n"
        "- **Qualificado** → todos os tipos (`\"Público Geral\"` + `\"Qualificado\"` + `\"Profissional\"`).\n\n"
        "_(Hierarquia conforme pedida no enunciado; difere da regra padrão CVM, "
        "em que Profissional é o topo.)_\n\n"
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
        excluded=excluded,
    )
    return out
