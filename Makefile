# SPDX-FileCopyrightText: 2026 Nawer Kezal
# SPDX-License-Identifier: AGPL-3.0-only

# docker or podman. compose is "docker compose" or "podman compose".
CONTAINER ?= $(shell command -v docker >/dev/null 2>&1 && echo docker || echo podman)
COMPOSE   ?= $(CONTAINER) compose

# Tools are run with `go run` at a pinned version: nothing to install, and
# CI uses exactly the same ones.
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
GOVULNCHECK   := go run golang.org/x/vuln/cmd/govulncheck@v1.9.0
SQLC          := go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1

TEST_DB_PORT ?= 55432
TEST_DB_URL  := postgres://postgres:test@localhost:$(TEST_DB_PORT)/postgres

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

##@ Development

.PHONY: build
build: ## Build the postier binary into bin/
	CGO_ENABLED=0 go build -trimpath -o bin/postier ./cmd/postier

.PHONY: generate
generate: ## Regenerate code from SQL and OpenAPI, and sync the chart's policy
	$(SQLC) generate
	go generate ./...
	cp kumo/policy/init.lua deploy/helm/postier/files/init.lua

.PHONY: test
test: ## Unit tests, with the race detector
	go test -race ./...

.PHONY: test-integration
test-integration: ## All tests, including those that need Postgres (starts a throwaway one)
	-$(CONTAINER) rm -f postier-test-pg >/dev/null 2>&1
	$(CONTAINER) run -d --rm --name postier-test-pg -e POSTGRES_PASSWORD=test \
		-p 127.0.0.1:$(TEST_DB_PORT):5432 docker.io/library/postgres:16-alpine >/dev/null
	@until $(CONTAINER) exec postier-test-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
	POSTIER_TEST_DATABASE_URL=$(TEST_DB_URL) go test -race -count=1 ./...; \
		status=$$?; $(CONTAINER) rm -f postier-test-pg >/dev/null; exit $$status

.PHONY: fuzz
fuzz: ## Fuzz the log record decoder for a minute
	go test ./internal/kumo/ -run '^$$' -fuzz FuzzDecodeBatch -fuzztime 60s

.PHONY: lint
lint: ## golangci-lint
	$(GOLANGCI_LINT) run

.PHONY: vuln
vuln: ## govulncheck
	$(GOVULNCHECK) ./...

.PHONY: reuse
reuse: ## Check that every file carries its licence (needs the reuse tool)
	reuse lint

.PHONY: check
check: lint test vuln chart-lint ## Everything CI runs, except the integration tests

##@ Test bench

.PHONY: bench-up
bench-up: ## Start the local test bench (compose)
	$(COMPOSE) up --build -d

.PHONY: bench-down
bench-down: ## Stop the test bench and delete its data
	$(COMPOSE) down -v

.PHONY: milestone
milestone: ## Phase 0 milestone: 10 000 messages while ingest and dispatch are killed
	CONTAINER=$(CONTAINER) hack/milestone.sh 10000

.PHONY: capture
capture: ## Refresh internal/kumo/testdata from a capture directory: make capture DIR=…
	go run ./test/capture -in $(DIR)

##@ Kubernetes

.PHONY: image
image: ## Build the container image postier:dev
	$(CONTAINER) build --target postier -t postier:dev .

.PHONY: chart-lint
chart-lint: ## Lint and render the Helm chart with both profiles
	helm lint deploy/helm/postier
	helm lint deploy/helm/postier -f deploy/helm/postier/values-quickstart.yaml
	helm template postier deploy/helm/postier >/dev/null
	helm template postier deploy/helm/postier -f deploy/helm/postier/values-quickstart.yaml >/dev/null

.PHONY: quickstart
quickstart: ## Postier on a local kind cluster, delivering to Mailpit
	CONTAINER=$(CONTAINER) hack/quickstart.sh

.PHONY: quickstart-delete
quickstart-delete: ## Remove the quickstart cluster
	CONTAINER=$(CONTAINER) hack/quickstart.sh delete
