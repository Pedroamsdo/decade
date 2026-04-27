"""gold/compute_metrics — per-fund metrics for ranking.

Reads silver/quota_series + silver/universe + gold benchmarks, computes
return / risk / risk-adjusted / cost / liquidity metrics on rolling
windows ending at as_of, and writes to gold/fund_metrics/.
"""
from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from fund_rank.gold.benchmarks import cdi_cum_factor, load_cdi_daily
from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import silver_path

log = get_logger(__name__)


# Number of business days per CVM/ANBIMA convention
DU_PER_YEAR = 252
WINDOW_DU = {"12m": 252, "24m": 504, "36m": 756}


def _annualize_return(cum_return: float, du: int) -> float:
    if du <= 0:
        return float("nan")
    return (1.0 + cum_return) ** (DU_PER_YEAR / du) - 1.0


def _annualize_vol(daily_log_returns: pl.Series) -> float:
    if daily_log_returns.len() < 2:
        return float("nan")
    return float(daily_log_returns.std()) * math.sqrt(DU_PER_YEAR)


def _max_drawdown_and_duration(cum_factors: pl.Series) -> tuple[float, int]:
    """Return (max_drawdown_negative_pct, duration_days_in_dd)."""
    if cum_factors.len() == 0:
        return (float("nan"), 0)
    arr = cum_factors.to_list()
    peak = arr[0]
    peak_idx = 0
    max_dd = 0.0
    max_dur = 0
    cur_dur = 0
    for i, v in enumerate(arr):
        if v > peak:
            peak = v
            peak_idx = i
            cur_dur = 0
        else:
            cur_dur = i - peak_idx
            dd = v / peak - 1.0
            if dd < max_dd:
                max_dd = dd
                max_dur = cur_dur
    return (max_dd, max_dur)


def _per_fund_metrics(
    series_df: pl.DataFrame,
    cdi_daily: pl.DataFrame,
    as_of: date,
    stress_events: list[dict],
) -> dict:
    """Compute one row of FundMetrics-shaped dict from a single series.

    series_df has columns: dt_comptc, vl_quota, vl_patrim_liq, captc_dia, resg_dia, log_return, jump_flag.
    Sorted by dt_comptc ascending.
    """
    series = series_df.filter(
        (pl.col("dt_comptc") <= as_of) & (~pl.col("jump_flag").fill_null(False))
    ).sort("dt_comptc")
    if series.is_empty():
        return {}

    metrics: dict = {}

    # Returns over rolling windows
    end_quota = float(series["vl_quota"].tail(1).item())
    for label, du in WINDOW_DU.items():
        win = series.tail(du + 1)  # need start point
        if win.height < du:
            continue
        start_quota = float(win["vl_quota"].head(1).item())
        cum_ret = end_quota / start_quota - 1.0
        metrics[f"retorno_acum_{label}"] = cum_ret
        metrics[f"retorno_anualizado_{label}" if label != "24m" else "retorno_anualizado_24m"] = (
            _annualize_return(cum_ret, du)
        )

        # %CDI
        win_start = win["dt_comptc"].head(1).item()
        cdi_cum = cdi_cum_factor(cdi_daily, win_start, as_of) if win_start else None
        if cdi_cum and cdi_cum > 1e-9:
            metrics[f"pct_cdi_{label}"] = (cum_ret) / (cdi_cum - 1.0) if (cdi_cum - 1.0) != 0 else None

        # Vol
        log_rets = win["log_return"].drop_nulls()
        metrics[f"vol_anualizada_{label}"] = _annualize_vol(log_rets) if label in ("12m", "36m") else None

        # Sharpe (over CDI proxy via cdi_pct_dia / 100 daily)
        if label == "12m":
            cdi_window = cdi_daily.filter(
                (pl.col("dt") >= win_start) & (pl.col("dt") <= as_of)
            )
            if cdi_window.height > 0:
                cdi_daily_arith = (cdi_window["cdi_pct_dia"] / 100.0).to_list()
                # Pad to align
                k = min(len(cdi_daily_arith), log_rets.len())
                if k > 1:
                    fund_arith = log_rets.tail(k).to_list()  # log≈arith for small returns
                    cdi_arr = cdi_daily_arith[-k:]
                    diffs = [f - c for f, c in zip(fund_arith, cdi_arr)]
                    if len(diffs) > 1:
                        mean_d = sum(diffs) / len(diffs)
                        std_d = (sum((d - mean_d) ** 2 for d in diffs) / (len(diffs) - 1)) ** 0.5
                        if std_d > 1e-12:
                            metrics["sharpe_12m"] = (mean_d * DU_PER_YEAR) / (
                                std_d * math.sqrt(DU_PER_YEAR)
                            )
                            metrics["tracking_error_cdi_12m"] = std_d * math.sqrt(DU_PER_YEAR)
                            mean_excess_ann = mean_d * DU_PER_YEAR
                            metrics["info_ratio_12m"] = mean_excess_ann / (
                                std_d * math.sqrt(DU_PER_YEAR)
                            )

    # Max drawdown 36m (price level)
    win36 = series.tail(WINDOW_DU["36m"] + 1)
    if win36.height >= WINDOW_DU["36m"]:
        cum = win36["vl_quota"] / win36["vl_quota"].head(1).item()
        dd, dur = _max_drawdown_and_duration(cum)
        metrics["max_drawdown_36m"] = dd
        metrics["drawdown_duration_days_36m"] = dur

    # Sortino 24m
    win24 = series.tail(WINDOW_DU["24m"] + 1)
    if win24.height >= WINDOW_DU["24m"]:
        log_rets = win24["log_return"].drop_nulls()
        cdi_window = cdi_daily.filter(
            (pl.col("dt") >= win24["dt_comptc"].head(1).item()) & (pl.col("dt") <= as_of)
        )["cdi_pct_dia"].to_list()
        k = min(len(cdi_window), log_rets.len())
        if k > 1:
            fund = log_rets.tail(k).to_list()
            cdi = [c / 100.0 for c in cdi_window[-k:]]
            below = [f - c for f, c in zip(fund, cdi) if f < c]
            if below:
                downside = (sum(b * b for b in below) / k) ** 0.5
                excess = sum(f - c for f, c in zip(fund, cdi)) / k
                if downside > 1e-12:
                    metrics["sortino_24m"] = (excess * DU_PER_YEAR) / (
                        downside * math.sqrt(DU_PER_YEAR)
                    )
                metrics["downside_dev_24m"] = downside * math.sqrt(DU_PER_YEAR)
        # Excess return 24m
        if "retorno_acum_24m" in metrics:
            cdi_cum = cdi_cum_factor(cdi_daily, win24["dt_comptc"].head(1).item(), as_of)
            if cdi_cum:
                metrics["excesso_retorno_24m"] = metrics["retorno_acum_24m"] - (cdi_cum - 1.0)

    # Consistency rolling 12m above CDI (pct of daily log_return >= cdi)
    if not log_rets.is_empty():  # noqa: F823 — uses last window's log_rets
        pass

    # Liquidity (PL median)
    pl_median = float(series["vl_patrim_liq"].median()) if series["vl_patrim_liq"].drop_nulls().len() > 0 else None
    metrics["pl_mediano_12m"] = pl_median
    if pl_median and pl_median > 0:
        metrics["log_pl_mediano_12m"] = math.log(pl_median)
    metrics["cotistas"] = (
        int(series["nr_cotst"].drop_nulls().tail(1).item())
        if series["nr_cotst"].drop_nulls().len() > 0
        else None
    )

    # Stress events
    stress_returns: list[float] = []
    for ev in stress_events:
        ev_start = date.fromisoformat(str(ev["start"]))
        ev_end = date.fromisoformat(str(ev["end"]))
        ev_slice = series.filter(
            (pl.col("dt_comptc") >= ev_start) & (pl.col("dt_comptc") <= ev_end)
        )
        if ev_slice.height >= 2:
            ret = (
                float(ev_slice["vl_quota"].tail(1).item())
                / float(ev_slice["vl_quota"].head(1).item())
                - 1.0
            )
            metrics[f"retorno_{ev['name']}"] = ret
            stress_returns.append(ret)
    if stress_returns:
        metrics["retorno_stress_event"] = sum(stress_returns) / len(stress_returns)

    # History tier
    metrics["history_dias_uteis"] = int(series.height)
    if series.height >= 756:
        metrics["history_confidence"] = "HIGH"
    elif series.height >= 504:
        metrics["history_confidence"] = "MED"
    elif series.height >= 252:
        metrics["history_confidence"] = "LOW"

    return metrics


def run(settings: Settings, as_of: date) -> dict[str, Path]:
    qs_path = silver_path(settings, "quota_series", as_of.isoformat()).parent / "data.parquet"
    funds_path = silver_path(settings, "funds", as_of.isoformat()).parent / "data.parquet"
    if not qs_path.exists() or not funds_path.exists():
        raise FileNotFoundError("Run silver layer (build_funds + build_quota_series) first.")

    qs = pl.read_parquet(qs_path)
    funds = pl.read_parquet(funds_path)
    cdi_daily = load_cdi_daily(settings)

    stress_events = settings.scoring.get("stress_events", [])

    out_root = settings.gold_root / "fund_metrics" / f"as_of={as_of.isoformat()}"
    out_root.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Path] = {}
    for seg_id in ["caixa", "rfgeral", "qualificado"]:
        u_path = (
            silver_path(settings, "universe", as_of.isoformat(), f"segment={seg_id}").parent
            / "data.parquet"
        )
        if not u_path.exists():
            log.warning("gold.metrics.no_universe_segment", segment=seg_id)
            continue
        univ = pl.read_parquet(u_path)
        log.info("gold.metrics.segment.start", segment=seg_id, rows=len(univ))

        rows: list[dict] = []
        for cnpj_classe in univ["cnpj_classe"].to_list():
            sub = qs.filter(pl.col("series_id") == cnpj_classe).sort("dt_comptc")
            if sub.is_empty():
                continue
            base = (
                univ.filter(pl.col("cnpj_classe") == cnpj_classe)
                .select(
                    "cnpj_classe",
                    "cnpj_fundo",
                    pl.col("denom_social"),
                    "classe_anbima_raw",
                    "tipo_classe",
                )
                .row(0, named=True)
            )
            row: dict = {
                "schema_version": "1.0.0",
                "cnpj_classe": base["cnpj_classe"],
                "cnpj_fundo": base["cnpj_fundo"] or base["cnpj_classe"],
                "denom_social": base["denom_social"],
                "classe_anbima": base["classe_anbima_raw"],
                "tipo": base["tipo_classe"],
                "dt_ref": as_of,
                "history_source": "own",
            }
            row.update(_per_fund_metrics(sub, cdi_daily, as_of, stress_events))
            rows.append(row)

        if not rows:
            log.warning("gold.metrics.no_rows", segment=seg_id)
            continue

        df = pl.DataFrame(rows)
        out = out_root / f"segment={seg_id}" / "data.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out, compression="zstd")
        paths[seg_id] = out
        log.info("gold.metrics.segment.written", segment=seg_id, rows=len(df), path=str(out))

    return paths
