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
    cvm_cad_fi: _SourceUrl
    cvm_cad_fi_hist: _SourceUrl
    cvm_registro_classe: _SourceUrl
    cvm_inf_diario: _SourceUrl
    cvm_inf_diario_hist: _SourceUrl | None = None
    cvm_cda: _SourceUrl
    bcb_cdi: _SourceUrl


class _HttpConfig(BaseModel):
    timeout_seconds: int = 180
    max_retries: int = 5
    retry_backoff_min_seconds: float = 2.0
    retry_backoff_max_seconds: float = 60.0
    user_agent: str = "fund-rank/0.1"


class _IngestConfig(BaseModel):
    inf_diario_lookback_months: int = 60
    cda_lookback_months: int = 13
    cdi_lookback_years: int = 5


class PipelineConfig(BaseModel):
    data_root: Path
    reports_root: Path
    sources: _SourcesConfig
    http: _HttpConfig
    ingest: _IngestConfig


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
    _universe: dict[str, Any] | None = None
    _scoring: dict[str, Any] | None = None
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
    def universe(self) -> dict[str, Any]:
        if self._universe is None:
            with open(self.config_dir / "universe.yaml") as f:
                self._universe = yaml.safe_load(f)
        return self._universe

    @property
    def scoring(self) -> dict[str, Any]:
        if self._scoring is None:
            with open(self.config_dir / "scoring.yaml") as f:
                self._scoring = yaml.safe_load(f)
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
