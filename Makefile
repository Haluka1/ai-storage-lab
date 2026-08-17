SHELL := /bin/bash

PYTHON ?= python3
GO_CACHE ?= /tmp/ai-inference-showcase-gocache

.PHONY: test test-router test-kv test-io test-demo demos demo demo-kv audit clean

test: test-router test-kv test-io test-demo

test-router:
	@test -z "$$(gofmt -l router/cmd router/internal)" || { gofmt -l router/cmd router/internal; echo "Go files need gofmt" >&2; exit 1; }
	cd router && GOCACHE=$(GO_CACHE) go build -buildvcs=false ./...
	cd router && GOCACHE=$(GO_CACHE) go vet ./...
	cd router && GOCACHE=$(GO_CACHE) go test -buildvcs=false ./...
	cd router && GOCACHE=$(GO_CACHE) go test -buildvcs=false -race ./...

test-kv:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=kvstore $(PYTHON) -m unittest discover -s kvstore/tests -v
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=.:kvstore $(PYTHON) -m unittest discover -s shared/tests -v

test-io:
	cmake -S io-profile -B io-profile/build -DCMAKE_BUILD_TYPE=Release
	cmake --build io-profile/build --parallel 2
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=io-profile/python $(PYTHON) -m unittest discover -s io-profile/tests -v

test-demo:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=examples/local-demo $(PYTHON) -m unittest discover -s examples/local-demo -p 'test_*.py' -v

demos: demo demo-kv

demo:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) examples/local-demo/run_demo.py

demo-kv:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) examples/local-demo/kv_demo.py

audit:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/check_links.py .
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/privacy_audit.py .
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/validate_tier_profiles.py
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/license_status.py

clean:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/clean.py
