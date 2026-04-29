# Architectural Decision Records

ADRs for non-obvious choices in `fund_rank`. Each decision lists the alternatives considered and why the current choice wins for the v1 take-home.

---

## ADR-001 · Rank classes (CNPJ_Classe), not umbrellas (CNPJ_Fundo)

**Status:** accepted · 2026-04-25

**Context.** Post-CVM Resolution 175, regulated Brazilian funds are organized as `Fundo (umbrella) → Classe (CNPJ_Classe) → Subclasse`. The investable unit a downstream system sells is the *classe*, with its own fee, public, and quota series. Pre-CVM 175, the same unit was the *fundo*. CVM's `INF_DIARIO` reports at *classe* granularity post-2023.

**Decision.** The ranking unit is `CNPJ_Classe`. The umbrella's `CNPJ_Fundo` is preserved as an attribute for joining to legacy data and for displaying gestor/administrador.

**Alternative.** Rank umbrellas. Rejected because (a) `INF_DIARIO` post-CVM 175 is not at umbrella level; (b) two classes under the same umbrella may have very different fees and target publics, so a single umbrella-level ranking would be misleading.

---

## ADR-003 · CDI as the sole benchmark in v1

**Status:** accepted · 2026-04-25

**Context.** Long-duration RF Indexados / IMA-B funds and short-duration Caixa funds shouldn't be measured against the same benchmark. ANBIMA publishes IMA-B / IMA-B 5 / IMA-B 5+ / IRF-M as proper benchmarks per duration bucket — but those feeds are paid-only.

**Decision.** Use BCB SGS series 12 (CDI) as the universal benchmark in v1. Mitigate the misfit by *segmenting the universe by duration bucket* (Caixa = D+1, RF Geral = up to D+30, Qualificado = no liquidity cap) so the benchmark mismatch is bounded inside each segment.

**Alternative.** Approximate IMA-B from Tesouro Direto prices. Rejected for v1: implementation is non-trivial, and there's no precedent of using TD-derived IMA-B in production. Backlog.

**Open question.** Should `pct_cdi` continue to drive Caixa scoring once IMA-B is wired? Probably not for the long-duration Indexados in RF Geral, but yes for Caixa where CDI tracking is the actual goal.

---

## ADR-004 · CVM 175 history stitch via single-class predecessor

**Status:** accepted · 2026-04-25

**Context.** CVM 175 (Oct/2023) created a new identifier (`CNPJ_Classe`) for what used to be reported at `CNPJ_Fundo` level. A naive cut by post-2023 data alone would discard ~ 30 % of the universe at the 12-month-min-history filter, including most pre-existing flagship funds.

**Decision.** Stitch the legacy `CNPJ_FUNDO` quota series to the *single classe* under the same umbrella, where exactly one class exists. Mark the row in silver as `history_source = "stitched_cvm175"`.

**Alternative.** Stitch all classes (multi-class case). Rejected because there is no clean attribution rule — the legacy fundo may have split into classes with very different policies. We accept losing pre-2023 history for those (`history_source = "orphan_pre_cvm175"`).

---

## ADR-005 · Z-scores intra-segment (not global)

**Status:** accepted · 2026-04-25

**Context.** Metrics like `tracking_error_cdi_12m` differ by orders of magnitude between segments: a Caixa fund has TE ~ 5 bps, a Qualificado credit fund has TE ~ 5 %. A global z-score would put every Caixa fund near the "best" pole and every Qualificado fund near the "worst", drowning the actual relative performance.

**Decision.** Z-scores are computed *within each segment*. Top-N selection is per segment.

---

## ADR-006 · Don't deduct fees from past returns (`vl_quota` is already net)

**Status:** accepted · 2026-04-25

**Context.** Beginners often build performance views by deducting `taxa_adm` from gross returns. CVM's `vl_quota` is already net of *realized* admin and performance fees, so deducting again double-counts costs.

**Decision.** Past returns are read directly from `vl_quota`. Forward-looking cost (`taxa_adm_pct` + estimated `taxa_perfm` carry on excess) enters as a *signal* in the score, not as a return adjustment. The estimated perf carry is flagged with `pf_estimate_quality ∈ {clean, hwm_flagged}`.

---

## ADR-007 · Polars + DuckDB on local Parquet (not Spark / BigQuery in v1)

**Status:** accepted · 2026-04-25

**Context.** Brazilian fund universe ≈ 30 k classes × 252 days/year × 5 years ≈ 30–40 M rows for the daily quotas table. Each daily INF_DIARIO file is ~10 MB compressed.

**Decision.** All transforms run in Polars (Python), with DuckDB for ad-hoc queries on the same Parquet files. `fsspec` abstracts the filesystem so swapping `data/` for `s3://` is configuration-only.

**Alternative.** Spark/BigQuery from day 1. Rejected because the volume fits comfortably in a laptop's memory (silver tables together are well under 1 GB Parquet). Spark adds operational overhead with no win for v1.

**Migration path.** When daily ingestion or the universe grows 10×, swap local FS → S3 (no code change), run the same Prefect flows on a worker pool. At 100×, treat the Parquet partitions as external tables in BigQuery / Snowflake — the SQL doesn't change. Documented in [scaling.md](scaling.md).

---

## ADR-008 · Idempotency by sha256, not just etag

**Status:** accepted · 2026-04-25

**Context.** CVM Dados Abertos serves an etag for most files, but during republishes the etag may change while the content is byte-identical. We don't want to create a new partition for unchanged content.

**Decision.** A fetched payload is compared by `sha256` against the latest existing partition's manifest. If hash matches, no new partition is written. The etag is kept as a fast pre-check (`If-None-Match: <etag>`) — when the server replies 304, we skip the body entirely.

---

## ADR-009 · ANBIMA classification matched by *prefix* after accent-strip

**Status:** accepted · 2026-04-25

**Context.** CVM publishes ANBIMA classifications in `Classificacao_Anbima` with abbreviations and accents — `"Renda Fixa Duração Baixa Grau de Invest."` (period) versus `"Renda Fixa Duração Baixa Grau de Invest"` (no period) versus `"Renda Fixa Duração Baixa Grau de Investimento"` versus `"Previdência RF Duração Livre Crédito Liv"` (truncated). A literal-string match is brittle.

**Decision.** Both YAML patterns and CVM strings are normalized (`strip accents → lowercase → collapse whitespace`) and matched by *prefix*. So `"Renda Fixa Duração Baixa Grau de Invest"` in the YAML matches all of "Grau de Invest", "Grau de Invest.", and "Grau de Investimento" rows.

**Trade-off.** Prefix match can over-include if a future ANBIMA category starts with the same words but means something different. Mitigated by reviewing the row counts in `silver/class_funds_fixed_income` and `silver/subclass_funds_fixed_income` after each ANBIMA refresh.

---

## ADR-010 · Decimal `;` as separator and `quote_char=None` for CVM CSV reads

**Status:** accepted · 2026-04-25

**Context.** CVM CSVs use `;` as separator and Latin-1 encoding. Some text fields (`INF_TAXA_PERFM` in particular) embed multi-paragraph methodology text with unescaped quotes that break standard CSV parsing.

**Decision.** Read with `polars.read_csv(separator=";", encoding="latin-1", quote_char=None, truncate_ragged_lines=True, infer_schema_length=0)`. Disabling quote handling means `;` is the only delimiter — robust against CVM's malformed quoting. Numeric and date types are cast in the silver layer rather than inferred.

---

## ADR-011 · Bronze layout: `source/ingested_at=DATE/competence=YYYY-MM/raw.ext`

**Status:** accepted · 2026-04-25

**Context.** Some sources are snapshots (`cad_fi.csv`), some are monthly (`inf_diario_fi_YYYYMM.zip`), some are dynamic ranges (`bcb_cdi` per query interval).

**Decision.** Universal partition scheme: `{source}/ingested_at={today}/[competence={key}/]raw.{ext}`. The optional `competence` partition is set when the source has a natural data period (month / year / range). A `_manifest.json` sidecar records `(url, etag, last_modified, sha256, byte_size, ingested_at, status)`.

This makes it trivial to (a) replay a build at an old `as_of` by reading the latest partition with `ingested_at <= replay_date`, and (b) audit when a value changed by listing the partitions for a `competence`.
