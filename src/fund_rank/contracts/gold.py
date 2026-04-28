"""Gold-layer external contracts.

These types are the **stable contract** consumed by downstream services.
Schema changes require a major version bump. The JSON Schema is exported
to `docs/contracts/` on each build for CI verification.
"""
from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field, ConfigDict


CONTRACT_VERSION = "1.0.0"


class FundMetrics(BaseModel):
    """Per-fund computed metrics for a given reference date.

    External contract — change only with version bump.
    """

    model_config = ConfigDict(extra="forbid", frozen=False)

    schema_version: str = CONTRACT_VERSION

    # Identity
    cnpj_classe: str = Field(..., description="Class CNPJ (post-CVM 175). Ranking unit.")
    cnpj_fundo: str = Field(..., description="Umbrella fund CNPJ.")
    denom_social: str
    classe_anbima: str | None = None
    tipo: str | None = None  # FI, FIC, FIDC, etc.

    # Reference
    dt_ref: date

    # Returns
    retorno_acum_12m: float | None = None
    retorno_acum_24m: float | None = None
    retorno_acum_36m: float | None = None
    retorno_anualizado_12m: float | None = None
    retorno_anualizado_36m: float | None = None
    pct_cdi_12m: float | None = None
    pct_cdi_24m: float | None = None
    pct_cdi_36m: float | None = None
    excesso_retorno_24m: float | None = None

    # Risk
    vol_anualizada_12m: float | None = None
    vol_anualizada_36m: float | None = None
    max_drawdown_36m: float | None = None
    drawdown_duration_days_36m: int | None = None
    downside_dev_24m: float | None = None

    # Risk-adjusted
    sharpe_12m: float | None = None
    sortino_24m: float | None = None
    info_ratio_12m: float | None = None
    ir_cdi_spread_24m: float | None = None
    tracking_error_cdi_12m: float | None = None
    tracking_error_class_bench_12m: float | None = None

    # Consistency
    consistency_rolling_12m_above_bench: float | None = None
    pct_dias_positivos_12m: float | None = None

    # Cost (forward-looking)
    taxa_adm_pct_aa: float | None = None
    taxa_perfm_estimada_pct_aa: float | None = None
    pf_estimate_quality: str | None = None  # "clean" | "hwm_flagged"
    custo_total_estimado: float | None = None  # taxa_adm + perf carry estimada

    # Liquidity
    cotizacao_dias: int | None = None
    liquidacao_dias: int | None = None
    liquidez_settlement_window_days: int | None = None  # cot+liq combinada
    pl_mediano_12m: float | None = None
    log_pl_mediano_12m: float | None = None
    cotistas: int | None = None

    # Stress
    retorno_stress_event: float | None = None  # média ponderada das janelas
    retorno_covid_2020: float | None = None
    retorno_marola_credito_2024: float | None = None

    # Tributação
    trib_lprazo: bool | None = None

    # Provenance
    history_source: str = Field(
        default="own",
        description="One of: own, stitched_cvm175",
    )
    history_confidence: str | None = None  # HIGH | MED | LOW
    history_dias_uteis: int | None = None


class RankingEntry(BaseModel):
    """One row of the final ranking output.

    External contract — change only with version bump.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: str = CONTRACT_VERSION

    dt_ref: date
    segment_id: str  # caixa | rfgeral | qualificado
    rank: int

    cnpj_classe: str
    cnpj_fundo: str
    denom_social: str

    score: float
    rationale: str

    metrics: FundMetrics

    model_version: str  # hash of code + scoring config that produced this
    generated_at: datetime
