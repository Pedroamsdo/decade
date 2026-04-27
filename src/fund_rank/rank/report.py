"""rank/report — render ranking.md from gold/ranking parquet.

Writes:
  reports/as_of=YYYY-MM-DD/ranking.md
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings

log = get_logger(__name__)


def _fmt_pct(v: float | None, decimals: int = 2) -> str:
    """Format a *ratio* (e.g., 0.123) as a percentage string."""
    if v is None or v != v:
        return "—"
    return f"{v * 100:.{decimals}f}%"


def _fmt_pct_cdi(v: float | None) -> str:
    """Format a CDI ratio (e.g., 1.07 means 107% CDI) without the extra *100."""
    if v is None or v != v:
        return "—"
    return f"{v * 100:.1f}% CDI"


def _fmt_brl(v: float | None) -> str:
    if v is None or v != v:
        return "—"
    if v >= 1e9:
        return f"R$ {v/1e9:.2f} bi"
    if v >= 1e6:
        return f"R$ {v/1e6:.0f} mi"
    return f"R$ {v:.0f}"


def _fmt_ratio(v: float | None, decimals: int = 2) -> str:
    if v is None or v != v:
        return "—"
    return f"{v:.{decimals}f}"


def _segment_block(seg_id: str, label: str, df: pl.DataFrame, top_n: int) -> str:
    lines: list[str] = []
    lines.append(f"## Segmento: {label}\n")
    lines.append(f"Universo: {len(df)} classes elegíveis · Top {top_n} a seguir.\n")

    top = df.head(top_n)
    for row in top.iter_rows(named=True):
        rank = int(row["rank"])
        name = (row.get("denom_social") or "?").strip()
        cnpj_classe = row.get("cnpj_classe") or "?"
        classe_anbima = row.get("classe_anbima") or "?"
        ret_12m = row.get("retorno_acum_12m")
        ret_24m = row.get("retorno_acum_24m")
        ret_36m = row.get("retorno_acum_36m")
        pct_cdi_12m = row.get("pct_cdi_12m")
        pct_cdi_24m = row.get("pct_cdi_24m")
        te_12m = row.get("tracking_error_cdi_12m")
        vol_12m = row.get("vol_anualizada_12m")
        max_dd = row.get("max_drawdown_36m")
        dd_dur = row.get("drawdown_duration_days_36m")
        sharpe = row.get("sharpe_12m")
        sortino = row.get("sortino_24m")
        ir = row.get("info_ratio_12m")
        custo = row.get("taxa_adm_pct")
        pl_med = row.get("pl_mediano_12m")
        cotistas = row.get("cotistas")
        trib = row.get("trib_lprazo")
        score = row.get("score") or 0.0
        history_dias = row.get("history_dias_uteis")
        history_conf = row.get("history_confidence") or "—"

        lines.append(f"### {rank}. {name}")
        lines.append(f"- **CNPJ Classe**: {cnpj_classe}")
        lines.append(f"- **ANBIMA**: {classe_anbima}")
        ret_str = " · ".join(filter(None, [
            f"12M: {_fmt_pct(ret_12m)} ({_fmt_pct_cdi(pct_cdi_12m)})" if ret_12m else None,
            f"24M: {_fmt_pct(ret_24m)} ({_fmt_pct_cdi(pct_cdi_24m)})" if ret_24m else None,
            f"36M: {_fmt_pct(ret_36m)}" if ret_36m else None,
        ]))
        if ret_str:
            lines.append(f"- **Retorno**: {ret_str}")
        risk_parts = []
        if vol_12m is not None:
            risk_parts.append(f"Vol 12M: {_fmt_pct(vol_12m)}")
        if te_12m is not None:
            risk_parts.append(f"TE vs CDI: {_fmt_pct(te_12m)}")
        if max_dd is not None:
            dur = f" ({dd_dur}d)" if dd_dur else ""
            risk_parts.append(f"Max DD 36M: {_fmt_pct(max_dd)}{dur}")
        if risk_parts:
            lines.append(f"- **Risco**: {' · '.join(risk_parts)}")
        ra_parts = []
        if sharpe is not None:
            ra_parts.append(f"Sharpe: {_fmt_ratio(sharpe)}")
        if sortino is not None:
            ra_parts.append(f"Sortino: {_fmt_ratio(sortino)}")
        if ir is not None:
            ra_parts.append(f"IR: {_fmt_ratio(ir)}")
        if ra_parts:
            lines.append(f"- **Risco-ajustado**: {' · '.join(ra_parts)}")
        liq_parts = []
        if pl_med is not None:
            liq_parts.append(f"PL mediano: {_fmt_brl(pl_med)}")
        if cotistas is not None:
            liq_parts.append(f"Cotistas: {cotistas}")
        if liq_parts:
            lines.append(f"- **Liquidez**: {' · '.join(liq_parts)}")
        if custo is not None:
            lines.append(f"- **Custo**: TAXA_ADM {_fmt_pct(custo / 100 if custo > 1 else custo, decimals=2)}")
        trib_str = "longo prazo" if trib in ("S", "Sim", True) else "curto prazo" if trib in ("N", "Não", False) else "—"
        lines.append(f"- **Tributação**: {trib_str}")
        lines.append(
            f"- **Score**: {_fmt_ratio(score)} · "
            f"Histórico: {history_conf} ({history_dias or 0} dias úteis)"
        )
        lines.append("")

    return "\n".join(lines)


def run(settings: Settings, as_of: date) -> Path:
    rank_root = settings.gold_root / "ranking" / f"as_of={as_of.isoformat()}"
    if not rank_root.exists():
        raise FileNotFoundError(f"No ranking at {rank_root}; run `rank` first.")

    universe_cfg = settings.universe.get("segments", {})
    top_n = settings.scoring.get("selection", {}).get("top_n", 5)

    body = []
    body.append(f"# Top {top_n} Renda Fixa — Referência {as_of.isoformat()}")
    body.append("")
    body.append(
        f"> Gerado por fund_rank em {datetime.utcnow().isoformat(timespec='seconds')}Z. "
        f"Metodologia em [docs/methodology.md](docs/methodology.md). "
        f"Ranqueamento em **classe** (CNPJ_Classe), pós CVM 175."
    )
    body.append("")

    for seg_id in ["caixa", "rfgeral", "qualificado"]:
        seg_dir = rank_root / f"segment={seg_id}"
        if not (seg_dir / "data.parquet").exists():
            continue
        df = pl.read_parquet(seg_dir / "data.parquet")
        label = universe_cfg.get(seg_id, {}).get("label", seg_id.title())
        body.append(_segment_block(seg_id, label, df, top_n))

    body.append("---")
    body.append("")
    body.append("## Limitações conhecidas")
    body.append("")
    body.append("- Carry de taxa de performance é estimado, não medido (campo `taxa_perfm_text` no silver).")
    body.append("- Tributação (longo/curto prazo) é reportada, não entra no score.")
    body.append("- Filtros de cotização/liquidação por dias estão em config mas inativos: dado não está em CVM Dados Abertos públicos (extrato_fi seria a fonte).")
    body.append("- v1 usa CDI como benchmark único; IMA-B / IRF-M no backlog.")
    body.append("- Master/feeder: ranking é em feeder; colapso quando ≥95% overlap em mesmo master.")
    body.append("- Stitch CVM 175: aplicado para fundos com 1 classe; multi-classe sob mesmo guarda-chuva ficam em série orfã.")
    body.append("")

    out = settings.pipeline.reports_root / f"as_of={as_of.isoformat()}" / "ranking.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(body))
    log.info("rank.report.written", path=str(out))
    return out
