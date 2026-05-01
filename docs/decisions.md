# Architectural Decision Records

ADRs for non-obvious choices in `fund_rank`. Each decision lists the alternatives considered and why the current choice wins for the v1 take-home.

---

## ADR-001 · Rank classes (CNPJ_Classe), not umbrellas (CNPJ_Fundo)

**Status:** accepted · 2026-04-25

**Context.** Post-CVM Resolution 175, regulated Brazilian funds are organized as `Fundo (umbrella) → Classe (CNPJ_Classe) → Subclasse`. The investable unit a downstream system sells is the *classe*, with its own fee, public, and quota series. Pre-CVM 175, the same unit was the *fundo*. CVM's `INF_DIARIO` reports at *classe* granularity post-2023.

**Decision.** The ranking unit is `CNPJ_Classe`. The umbrella's `CNPJ_Fundo` is preserved as an attribute for joining to legacy data and for displaying gestor/administrador.

**Alternative.** Rank umbrellas. Rejected because (a) `INF_DIARIO` post-CVM 175 is not at umbrella level; (b) two classes under the same umbrella may have very different fees and target publics, so a single umbrella-level ranking would be misleading.

---

## ADR-003 · Canonical benchmark per ANBIMA classification (not CDI universal)

**Status:** accepted · 2026-05-01 (supersedes earlier "CDI universal in v1")

**Context.** Long-duration RF Indexados / IMA-B funds and short-duration Caixa funds shouldn't be measured against the same benchmark. Using CDI universally and segmenting by duration was the v0 approach — it bounded the mismatch inside each segment but still mis-rated long-duration funds within their segment.

**Decision.** Each fund is mapped to a **canonical benchmark code** (CDI, IPCA, IMA-B, IMA-B 5, IMA-B 5+, IRF-M, IMA Geral, IMA-S, …) by its ANBIMA classification (`silver/_benchmark_mapping.py`). Excess returns and IR / Sortino are computed against that canonical benchmark, not against CDI universally. ANBIMA index histories are sourced from manual XLS drops in `data/bronze/anbima_indices/dropped/` (no paid-feed dependency); CDI / SELIC / IPCA come from BCB SGS.

**Alternative.** Keep CDI universal + segment by duration. Rejected because (a) the new benchmark mapping is config-driven and lighter than maintaining segments, and (b) ANBIMA index histories turned out to be obtainable as a manual XLS drop, removing the paid-feed blocker.

**Consequence.** The ranking pipeline no longer needs duration-bucket segmentation — see ADR-005.

---

## ADR-004 · CVM 175 history stitch via single-class predecessor

**Status:** accepted · 2026-04-25

**Context.** CVM 175 (Oct/2023) created a new identifier (`CNPJ_Classe`) for what used to be reported at `CNPJ_Fundo` level. A naive cut by post-2023 data alone would discard ~ 30 % of the universe at the 12-month-min-history filter, including most pre-existing flagship funds.

**Decision.** Stitch the legacy `CNPJ_FUNDO` quota series to the *single classe* under the same umbrella, where exactly one class exists. Mark the row in silver as `history_source = "stitched_cvm175"`.

**Alternative.** Stitch all classes (multi-class case). Rejected because there is no clean attribution rule — the legacy fundo may have split into classes with very different policies. We accept losing pre-2023 history for those (`history_source = "orphan_pre_cvm175"`).

---

## ADR-005 · Single eligible universe, z-scores global, profile filtering as post-step

**Status:** accepted · 2026-05-01 (supersedes earlier "Z-scores intra-segment")

**Context.** The earlier design segmented funds into 3 buckets (Caixa / RF Geral / Qualificado) by duration + ANBIMA prefix and ranked each bucket independently with its own weights. Justified at the time because everything was measured against CDI (ADR-003 v0) — segments contained the benchmark mismatch.

Once each fund got its own canonical benchmark (ADR-003 current), excess returns become directly comparable across the whole RF universe — IR of a CDI-tracker and IR of an IMA-B fund both measure "alpha vs the right yardstick". Segmentation became redundant scaffolding.

**Decision.** A single eligible universe. The composite (`0.7 × z(IR) + 0.3 × z(Sortino)`) is z-scored over that single universe, and `score = percentile_rank(composite) × 100`. The 3 profile views in `ranking.md` (Geral / Qualificado / Profissional) are pure post-filters on `publico_alvo` — they show different slices of the same global score, never re-rank.

**Why this is better.**
- No tuning of per-segment weights.
- Investor profiles in `ranking.md` reflect a real CVM access rule, not an analyst-chosen segmentation.
- Adding new metrics is config-only (one entry in `scoring.yaml`); no segment-specific weight matrix to balance.

**Trade-off.** A Caixa-style fund and a long-duration IMA-B fund end up on the same leaderboard. This is OK because each is rated against its own benchmark — the score answers "did you beat your benchmark?" — and the profile filter handles "is this fund accessible to me?".

---

## ADR-006 · Don't deduct fees from past returns (`vl_quota` is already net)

**Status:** accepted · 2026-04-25

**Context.** Beginners often build performance views by deducting `taxa_adm` from gross returns. CVM's `vl_quota` is already net of *realized* admin and performance fees, so deducting again double-counts costs.

**Decision.** Past returns are read directly from `vl_quota`. Forward-looking fee adjustment (estimating future perf-fee carry on excess) is **not** in the score — the v0 plan to ingest it as a signal was retired when the score was simplified to a metric composite (IR + Sortino). Fee data still flows through silver for reporting / display.

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

## ADR-011 · Bronze layout: `source/[competence=YYYY-MM/]raw.ext` (canonical, no `ingested_at`)

**Status:** accepted · 2026-05-01 (supersedes earlier `ingested_at=DATE/...` layout)

**Context.** Some sources are snapshots (`cad_fi_hist.zip`, `registro_fundo_classe.zip`), some are monthly (`inf_diario_fi_YYYYMM.zip`), some are dynamic ranges (`bcb_cdi` per query interval). The earlier design partitioned every source by `ingested_at=<today>` to keep an audit trail of every fetch — but this ballooned bronze on idempotent re-runs and made silver builds harder (had to pick "the latest `ingested_at` ≤ as_of").

**Decision.** A single canonical location per source: `{source}/[competence={key}/]raw.{ext}`. No `ingested_at` partitions. The `_manifest.json` sidecar records the run's `(url, etag, last_modified, sha256, byte_size, ingested_at, status)` — so the audit trail lives in the manifest, not the path.

Idempotency is enforced by sha256 (ADR-008): if the new payload matches the manifest's hash, the file isn't rewritten. Etag is the fast pre-check (`If-None-Match` returns 304 → skip body entirely).

**Trade-off.** Replaying a historical fetch byte-for-byte is no longer possible — once a CVM file is republished and the hash differs, the old version is overwritten. The manifest's `last_modified` and `sha256` history (kept in git on the manifest file when changes are committed) is the audit substitute. For this project the trade-off is correct: the case study runs against current snapshots, not historical replays.
