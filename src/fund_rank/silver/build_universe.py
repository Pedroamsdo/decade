"""silver/universe — apply structural filters and segment the universe.

Reads silver/funds + silver/quota_series, applies global filters from
universe.yaml, then partitions into segment={caixa, rfgeral, qualificado}.
Master/feeder dedupe: when multiple feeders point at the same master AND
qualify for the same segment, keep the one with lowest taxa_adm.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from fund_rank.obs.logging import get_logger
from fund_rank.settings import Settings
from fund_rank.silver._io import (
    normalize_text,
    silver_path,
    write_parquet,
)

log = get_logger(__name__)

SEGMENT_ORDER = ["caixa", "rfgeral", "qualificado"]


def _normalize_strings_in_lists(values: list[str]) -> list[str]:
    out = []
    for v in values:
        n = normalize_text(v)
        if n:
            out.append(n)
    return out


def _publico_aceito(seg_cfg: dict, publico_norm: pl.Expr) -> pl.Expr:
    accepted = _normalize_strings_in_lists(seg_cfg.get("publico_alvo_aceitos", []))
    if not accepted:
        return pl.lit(True)
    return publico_norm.is_in(accepted)


def _classe_anbima_match(seg_cfg: dict, classe_norm: pl.Expr) -> pl.Expr:
    starts = _normalize_strings_in_lists(seg_cfg.get("classes_anbima_starts", []))
    excl = _normalize_strings_in_lists(seg_cfg.get("classes_anbima_excluir_indexador", []))
    if not starts:
        return pl.lit(False)
    starts_expr = pl.lit(False)
    for s in starts:
        starts_expr = starts_expr | classe_norm.str.starts_with(s)
    if excl:
        for e in excl:
            starts_expr = starts_expr & ~classe_norm.str.contains(e, literal=False)
    return starts_expr.fill_null(False)


def _liquidity_pass(seg_cfg: dict, cot_dias: pl.Expr, liq_dias: pl.Expr) -> pl.Expr:
    expr = pl.lit(True)
    if "cotizacao_max_dias" in seg_cfg:
        expr = expr & (cot_dias.fill_null(99999) <= seg_cfg["cotizacao_max_dias"])
    if "liquidacao_max_dias" in seg_cfg:
        expr = expr & (liq_dias.fill_null(99999) <= seg_cfg["liquidacao_max_dias"])
    return expr


def run(settings: Settings, as_of: date) -> dict[str, Path]:
    universe_cfg = settings.universe
    filters = universe_cfg["filters_global"]
    segments = universe_cfg["segments"]

    # Read silver/funds
    funds_path = silver_path(settings, "funds", as_of.isoformat()).parent / "data.parquet"
    if not funds_path.exists():
        raise FileNotFoundError(f"silver/funds not found at {funds_path}; run build_funds first.")
    funds = pl.read_parquet(funds_path)

    # Quota stats from silver/quota_series for PL median + cotistas + history check
    qs_path = silver_path(settings, "quota_series", as_of.isoformat()).parent / "data.parquet"
    if not qs_path.exists():
        raise FileNotFoundError(f"silver/quota_series not found at {qs_path}.")
    qs = pl.read_parquet(qs_path)

    qs_stats = (
        qs.filter(pl.col("dt_comptc") <= as_of)
        .group_by("series_id")
        .agg(
            pl.col("vl_patrim_liq").median().alias("pl_mediano"),
            pl.col("nr_cotst").last().alias("cotistas"),
            pl.col("dt_comptc").min().alias("dt_min"),
            pl.col("dt_comptc").max().alias("dt_max"),
            pl.len().alias("dias_uteis"),
            (pl.col("captc_dia") > 0).sum().alias("dias_captc_positivos"),
        )
    )

    # Join: funds.cnpj_classe is series_id for post-CVM 175. We also include cnpj_fundo
    # for legacy series, but funds only knows cnpj_classe.
    funds_x = funds.join(
        qs_stats, left_on="cnpj_classe", right_on="series_id", how="left"
    )

    # Normalize
    funds_x = funds_x.with_columns(
        pl.col("publico_alvo")
        .fill_null("")
        .map_elements(lambda x: normalize_text(x) or "", return_dtype=pl.Utf8)
        .alias("publico_norm"),
        pl.col("classe_anbima_norm").fill_null(""),
    )

    # Apply global filters
    rejected_situacoes = filters.get("situacao_rejeitar_passada", [])
    sit_norm = (
        pl.col("situacao")
        .fill_null("")
        .map_elements(lambda x: normalize_text(x) or "", return_dtype=pl.Utf8)
    )
    rejected_norms = _normalize_strings_in_lists(rejected_situacoes)
    sit_aceita = normalize_text(filters.get("situacao_aceita", "EM FUNCIONAMENTO NORMAL")) or ""

    base_filter = (
        (sit_norm == sit_aceita)
        & (~sit_norm.is_in(rejected_norms) if rejected_norms else pl.lit(True))
        & (
            pl.col("exclusivo").fill_null("").str.to_uppercase().str.starts_with("N")
            | pl.lit(filters.get("fundo_exclusivo", False))
        )
        & (pl.col("dias_uteis").fill_null(0) >= filters.get("historico_min_dias_uteis", 252))
    )

    condom_aceitos = filters.get("condominio_aceito", [])
    if condom_aceitos:
        condom_norms = _normalize_strings_in_lists(condom_aceitos)
        base_filter = base_filter & (
            pl.col("condominio")
            .fill_null("")
            .map_elements(lambda x: normalize_text(x) or "", return_dtype=pl.Utf8)
            .is_in(condom_norms)
        )

    pre_filter = funds_x.filter(base_filter)
    log.info(
        "silver.universe.global_filter",
        before=len(funds_x),
        after=len(pre_filter),
    )

    # Per-segment classification + filters
    out_paths: dict[str, Path] = {}
    for seg_id in SEGMENT_ORDER:
        seg_cfg = segments.get(seg_id)
        if seg_cfg is None:
            continue

        classe_match = _classe_anbima_match(seg_cfg, pl.col("classe_anbima_norm"))
        publico_match = _publico_aceito(seg_cfg, pl.col("publico_norm"))
        seg_filter = classe_match & publico_match

        if "pl_mediano_minimo_brl" in seg_cfg:
            seg_filter = seg_filter & (
                pl.col("pl_mediano").fill_null(0) >= seg_cfg["pl_mediano_minimo_brl"]
            )
        if "cotistas_minimo" in seg_cfg:
            seg_filter = seg_filter & (
                pl.col("cotistas").fill_null(0) >= seg_cfg["cotistas_minimo"]
            )

        seg_df = pre_filter.filter(seg_filter)
        seg_df = seg_df.with_columns(pl.lit(seg_id).alias("segment_id"))

        # Master/feeder dedupe within segment
        if "cnpj_master" in seg_df.columns and seg_df["cnpj_master"].is_not_null().any():
            seg_df = (
                seg_df.sort(
                    ["cnpj_master", "taxa_adm_pct"],
                    descending=[False, False],
                    nulls_last=True,
                )
                .with_columns(
                    pl.when(pl.col("cnpj_master").is_not_null())
                    .then(pl.cum_count("cnpj_master").over("cnpj_master"))
                    .otherwise(1)
                    .alias("_dedupe_rank")
                )
                .filter(pl.col("_dedupe_rank") == 1)
                .drop("_dedupe_rank")
            )

        out = (
            silver_path(settings, "universe", as_of.isoformat(), f"segment={seg_id}").parent
            / "data.parquet"
        )
        write_parquet(seg_df, out)
        out_paths[seg_id] = out
        log.info("silver.universe.segment_written", segment=seg_id, rows=len(seg_df), path=str(out))

    return out_paths
