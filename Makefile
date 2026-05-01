.PHONY: help setup ingest build all test lint clean clean-all

AS_OF ?= 2025-12-31
PY := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PIP := $(if $(wildcard .venv/bin/pip),.venv/bin/pip,pip3)

help:
	@echo "Targets:"
	@echo "  setup           Create .venv and install package + dev deps"
	@echo "  ingest          Run bronze ingestion (idempotent, etag-aware)"
	@echo "  build           silver layer (class_funds + subclass_funds + RF subsets + quota_series)"
	@echo "  rank            gold layer (fund_metrics + validacao + ranking.md)"
	@echo "  all             ingest + build + rank"
	@echo "  test            pytest"
	@echo "  lint            ruff check"
	@echo "  clean           Remove silver/gold/reports (preserves bronze cache)"
	@echo "  clean-all       Remove everything under data/ and reports/"
	@echo ""
	@echo "AS_OF defaults to 2025-12-31. Override: make all AS_OF=2024-12-31"

setup:
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

ingest:
	$(PY) -m fund_rank.cli ingest --as-of $(AS_OF)

build:
	$(PY) -m fund_rank.cli build --as-of $(AS_OF)

rank:
	$(PY) -m fund_rank.cli rank --as-of $(AS_OF)

all: ingest build rank

test:
	$(PY) -m pytest

lint:
	.venv/bin/ruff check src tests

clean:
	rm -rf data/silver data/gold reports

clean-all: clean
	rm -rf data/bronze
