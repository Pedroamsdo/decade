.PHONY: help setup ingest build rank report all test lint clean clean-all

AS_OF ?= 2025-12-31
PY := .venv/bin/python
PIP := .venv/bin/pip

help:
	@echo "Targets:"
	@echo "  setup           Create .venv and install package + dev deps"
	@echo "  ingest          Run bronze ingestion (idempotent, etag-aware)"
	@echo "  build           silver + gold (metrics)"
	@echo "  rank            Compute rankings into gold/ranking/"
	@echo "  report          Render reports/as_of=.../ranking.md"
	@echo "  all             ingest + build + rank + report"
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

report:
	$(PY) -m fund_rank.cli report --as-of $(AS_OF)

all: ingest build rank report

test:
	$(PY) -m pytest

lint:
	.venv/bin/ruff check src tests

clean:
	rm -rf data/silver data/gold reports

clean-all: clean
	rm -rf data/bronze
