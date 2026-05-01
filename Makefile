.PHONY: help setup ingest build rank all reproduce test lint clean clean-all

AS_OF ?= 2025-12-31
INGEST_UNTIL ?= $(shell date +%Y-%m-%d)
INGEST_FLAGS ?=
PY := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PIP := $(if $(wildcard .venv/bin/pip),.venv/bin/pip,pip3)

help:
	@echo "Targets:"
	@echo "  setup           Create .venv and install package + dev deps"
	@echo "  ingest          Run bronze ingestion (idempotent, etag-aware)"
	@echo "  build           silver layer (class_funds + subclass_funds + RF subsets + quota_series)"
	@echo "  rank            gold layer (fund_metrics + validacao + ranking.md)"
	@echo "  all             ingest + build + rank"
	@echo "  reproduce       setup + all (one-shot for fresh clones)"
	@echo "  test            pytest"
	@echo "  lint            ruff check"
	@echo "  clean           Remove silver/gold/reports (preserves bronze cache)"
	@echo "  clean-all       Remove everything under data/ and reports/"
	@echo ""
	@echo "AS_OF defaults to 2025-12-31 (case-study cutoff for calculations)."
	@echo "INGEST_UNTIL defaults to today (upper bound of bronze download window)."
	@echo "Override: make all AS_OF=2024-12-31"

setup:
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

ingest:
	$(PY) -m fund_rank.cli ingest --as-of $(AS_OF) --ingest-until $(INGEST_UNTIL) $(INGEST_FLAGS)

build:
	$(PY) -m fund_rank.cli build --as-of $(AS_OF)

rank:
	$(PY) -m fund_rank.cli rank --as-of $(AS_OF)

all: ingest build rank

reproduce: setup all
	@echo ""
	@echo "Pipeline complete. See ranking.md for results."

test:
	$(PY) -m pytest

lint:
	.venv/bin/ruff check src tests

clean:
	rm -rf data/silver data/gold reports

clean-all: clean
	rm -rf data/bronze
