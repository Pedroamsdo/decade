"""silver/class_funds_fixed_income — RF subset of class_funds (filter-only).

Filters class_funds to rows whose `classificacao_anbima` starts with
"Renda Fixa". Excludes Previdência RF (different ANBIMA category).

This stage is **filter-only**: nulls and outliers in `taxa_adm`,
`taxa_perform` and the raw CVM `benchmark` strings are preserved for
auditability. Benchmark mapping and taxa imputation live in
`build_class_funds_fixed_income_treated`.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from fund_rank.settings import Settings
from fund_rank.silver._fixed_income_filter import filter_rf_subset

OUTPUT_COLUMNS: list[str] = [
    "cnpj_fundo",
    "cnpj_classe",
    "denom_social_fundo",
    "denom_social_classe",
    "situacao",
    "data_de_inicio",
    "exclusivo",
    "publico_alvo",
    "condominio",
    "classificacao_anbima",
    "composicao_fundos",
    "tributacao_alvo",
    "aplicacao_minima",
    "prazo_de_resgate",
    "taxa_adm",
    "taxa_perform",
    "benchmark",
]


def run(settings: Settings, as_of: date) -> Path:
    return filter_rf_subset(
        settings, as_of,
        in_table="class_funds",
        out_table="class_funds_fixed_income",
        distinct_key="cnpj_classe",
        output_columns=OUTPUT_COLUMNS,
    )
