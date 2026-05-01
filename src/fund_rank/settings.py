from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class _SourceUrl(BaseModel):
    url: str | None = None
    url_template: str | None = None
    extension: str = "bin"

    @model_validator(mode="after")
    def _check_url_or_template(self) -> _SourceUrl:
        if not self.url and not self.url_template:
            raise ValueError("Source must define `url` or `url_template`")
        return self


class _SourcesConfig(BaseModel):
    cvm_cad_fi_hist: _SourceUrl
    cvm_registro_classe: _SourceUrl
    cvm_inf_diario: _SourceUrl
    cvm_inf_diario_hist: _SourceUrl | None = None
    bcb_cdi: _SourceUrl
    bcb_selic: _SourceUrl
    bcb_ipca: _SourceUrl
    bcb_inpc: _SourceUrl
    bcb_igpm: _SourceUrl


class _HttpConfig(BaseModel):
    timeout_seconds: int = 180
    max_retries: int = 5
    retry_backoff_min_seconds: float = 2.0
    retry_backoff_max_seconds: float = 60.0
    user_agent: str = "fund-rank/0.1"


class _IngestConfig(BaseModel):
    inf_diario_lookback_months: int = 60
    index_series_lookback_years: int = 26


class PipelineConfig(BaseModel):
    data_root: Path
    reports_root: Path
    sources: _SourcesConfig
    http: _HttpConfig
    ingest: _IngestConfig


class _MetricSpec(BaseModel):
    direction: str  # "positive" | "negative"
    weight: float

    @model_validator(mode="after")
    def _check_direction(self) -> _MetricSpec:
        if self.direction not in ("positive", "negative"):
            raise ValueError(
                f"metric direction must be 'positive' or 'negative', got {self.direction!r}"
            )
        if self.weight <= 0:
            raise ValueError(f"metric weight must be > 0, got {self.weight}")
        return self


class _EligibilityConfig(BaseModel):
    situacao: str
    nr_cotst_min: int
    existing_time_min_days: int
    equity_min_brl: float


class _SelectionConfig(BaseModel):
    top_n: int = 5


class ScoringConfig(BaseModel):
    metrics: dict[str, _MetricSpec]
    eligibility: _EligibilityConfig
    selection: _SelectionConfig

    @model_validator(mode="after")
    def _check_weights_sum_to_one(self) -> ScoringConfig:
        if not self.metrics:
            raise ValueError("scoring.metrics must declare at least one metric")
        total = sum(spec.weight for spec in self.metrics.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(
                f"scoring.metrics weights must sum to 1.0, got {total:.6f}"
            )
        return self


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FUND_RANK_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    config_dir: Path = Field(default_factory=lambda: Path("configs"))
    data_root: Path | None = None
    reports_root: Path | None = None
    log_level: str = "INFO"

    _pipeline: PipelineConfig | None = None
    _scoring: ScoringConfig | None = None
    _benchmarks: dict[str, Any] | None = None

    @property
    def pipeline(self) -> PipelineConfig:
        if self._pipeline is None:
            with open(self.config_dir / "pipeline.yaml") as f:
                raw = yaml.safe_load(f)
            self._pipeline = PipelineConfig.model_validate(raw)
            # Apply env overrides AFTER yaml load, so envs win
            if self.data_root is not None:
                self._pipeline.data_root = self.data_root
            if self.reports_root is not None:
                self._pipeline.reports_root = self.reports_root
        return self._pipeline

    @property
    def scoring(self) -> ScoringConfig:
        if self._scoring is None:
            with open(self.config_dir / "scoring.yaml") as f:
                raw = yaml.safe_load(f)
            self._scoring = ScoringConfig.model_validate(raw)
        return self._scoring

    @property
    def benchmarks(self) -> dict[str, Any]:
        if self._benchmarks is None:
            with open(self.config_dir / "benchmarks.yaml") as f:
                self._benchmarks = yaml.safe_load(f)
        return self._benchmarks

    @property
    def bronze_root(self) -> Path:
        return self.pipeline.data_root / "bronze"

    @property
    def silver_root(self) -> Path:
        return self.pipeline.data_root / "silver"

    @property
    def gold_root(self) -> Path:
        return self.pipeline.data_root / "gold"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached settings singleton. Use this in production code paths."""
    return Settings()
