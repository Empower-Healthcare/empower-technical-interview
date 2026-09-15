# ParcelLab developer commands. `make` (or `make help`) lists them.
#
# Every command a person needs to run against this project lives here, so the
# README and the interview can say "make <target>" instead of pasting docker,
# grpcurl, psql, or kafka CLI invocations.
SHELL := /bin/bash
.DEFAULT_GOAL := help

# Host ports. `.env` (make env-init copies .env.example) overrides these for
# both Compose and the targets below, so a laptop with 5432 or 9090 in use still
# works. Keep .env to plain KEY=number lines: Make reads it too and does not
# understand quotes, ${VAR:-default}, or empty values.
-include .env
GRPC_PORT          ?= 50051
SHIPMENT_MGMT_PORT ?= 8080
RELAY_MGMT_PORT    ?= 8081
CONSUMER_MGMT_PORT ?= 8082
PRICING_PORT       ?= 8000
POSTGRES_PORT      ?= 5432
KAFKA_HOST_PORT    ?= 19092
KAFKA_UI_PORT      ?= 8085
PROMETHEUS_PORT    ?= 9090

# Container engine. Docker by default; Podman works when `podman compose` uses
# the docker-compose provider (see README "Podman"). Override with
# CONTAINER_CLI=podman or COMPOSE="podman-compose" if you must.
CONTAINER_CLI ?= $(shell command -v docker >/dev/null 2>&1 && echo docker || echo podman)
COMPOSE       ?= $(CONTAINER_CLI) compose
export COMPOSE

# Repository-local toolchain. `make tools` puts protoc, protoc-gen-go,
# protoc-gen-go-grpc and grpcurl into ./bin (gitignored) and nothing anywhere
# else, so it does not matter whether Go came from go.dev, Homebrew, asdf or
# mise. These pins match the headers of the committed gen/ files; other
# versions produce a header-only diff that fails `make generate-check`.
BIN := $(CURDIR)/bin
export PATH := $(BIN):$(PATH)
# File rules below use the relative bin/ so a checkout path containing spaces
# does not split into several Make targets; shell uses of $(BIN) are quoted.
PROTOC_VERSION             := 36.1
PROTOC_GEN_GO_VERSION      := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2
GRPCURL_VERSION            := v1.9.4
PROTOC_INCLUDE := $(BIN)/protoc-$(PROTOC_VERSION)/include
install_protoc_gen_go      = GOBIN="$(BIN)" go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
install_protoc_gen_go_grpc = GOBIN="$(BIN)" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
install_grpcurl            = GOBIN="$(BIN)" go install github.com/fullstorydev/grpcurl/cmd/grpcurl@$(GRPCURL_VERSION)
install_protoc             = go run ./tools/protoc -version $(PROTOC_VERSION) -dest "$(BIN)"
GRPCURL := "$(BIN)/grpcurl"

PROTO       := proto/parcellab/v1/shipment.proto
GEN_OUT     ?= gen
GRPC_ADDR   := 127.0.0.1:$(GRPC_PORT)
GRPC_SVC    := parcellab.v1.ShipmentService
TOPIC       := shipment.created.v1
GROUP       := dispatch-consumer
KAFKA_BIN   := /opt/kafka/bin
KAFKA_EXEC  := $(COMPOSE) exec -T kafka
PSQL        := $(COMPOSE) exec -T postgres psql -U parcellab -d parcellab
DEV_DB_URL  := postgres://parcellab:parcellab_dev_password@127.0.0.1:$(POSTGRES_PORT)

# Parameters for targets that take input.
WEIGHT_GRAMS   ?= 1500
REQUEST_JSON   ?= {"weight_grams": $(WEIGHT_GRAMS)}
CORRELATION_ID ?= req_make_$(shell date +%s)
DELAY_MS       ?= 5000
TEST_DATABASE_URL ?= $(DEV_DB_URL)/parcellab_test?sslmode=disable

# Colours for the help output.
CYAN := \033[36m
BOLD := \033[1m
NC   := \033[0m

.PHONY: help doctor tools env-init ports urls \
        up down reset restart build pull ps logs logs-service logs-pricing logs-relay logs-consumer \
        test test-go test-python test-integration smoke lint fmt vet \
        generate generate-check \
        grpc-list quote create get replay psql sql sql-shipment sql-dispatch-counts sql-pending sql-processed sql-event \
        kafka-topics kafka-messages kafka-lag metrics prometheus-targets \
        kafka-stop kafka-start pricing-slow pricing-normal pricing-stop pricing-start poison-event

##@ Getting started

help: ## Display details on all commands
	@awk 'BEGIN {FS = ":.*?##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

doctor: ## Check the host has what the interview needs and print install hints; installs nothing
	@ok=1; \
	check() { if command -v "$$1" >/dev/null 2>&1; then printf '  ok      %-18s %s\n' "$$1" "$$(command -v "$$1")"; else printf '  MISSING %-18s %s\n' "$$1" "$$2"; ok=0; fi; }; \
	echo "Base tools (install these yourself, however you manage tools):"; \
	check git   "https://git-scm.com/downloads"; \
	check go    "Go 1.27+: https://go.dev/dl (or asdf/mise plugin golang)"; \
	check uv    "https://docs.astral.sh/uv/getting-started/installation (or asdf/mise plugin uv)"; \
	check $(CONTAINER_CLI) "Docker Engine/Desktop https://docs.docker.com/get-docker or Podman https://podman.io/docs/installation"; \
	if $(COMPOSE) version >/dev/null 2>&1; then printf '  ok      %-18s %s\n' compose "$$($(COMPOSE) version 2>/dev/null | head -1)"; \
	else printf '  MISSING %-18s %s\n' compose "'$(COMPOSE)' failed; Docker: install the compose plugin; Podman: install docker-compose so 'podman compose' can use it"; ok=0; fi; \
	if $(CONTAINER_CLI) info >/dev/null 2>&1; then printf '  ok      %-18s running\n' "$(CONTAINER_CLI) engine"; \
	else printf '  DOWN    %-18s %s\n' "$(CONTAINER_CLI) engine" "start Docker Desktop / dockerd, or 'podman machine start'"; ok=0; fi; \
	echo; echo "Repository-local tools in ./bin (make tools fetches them; nothing else on the machine is touched):"; \
	gotool() { if [ -x "$(BIN)/$$1" ]; then have="$$(go version -m "$(BIN)/$$1" 2>/dev/null | awk '$$1 == "mod" { print $$3 }')"; \
	    if [ "$$have" = "$$2" ]; then printf '  ok      %-18s %s\n' "$$1" "$$have"; else printf '  STALE   %-18s have %s, want %s (make tools)\n' "$$1" "$${have:-?}" "$$2"; ok=0; fi; \
	  else printf '  MISSING %-18s make tools\n' "$$1"; ok=0; fi; }; \
	if [ -x "$(BIN)/protoc" ]; then have="$$("$(BIN)/protoc" --version 2>/dev/null | awk '{ print $$2 }')"; \
	    if [ "$$have" = "$(PROTOC_VERSION)" ]; then printf '  ok      %-18s %s\n' protoc "$$have"; else printf '  STALE   %-18s have %s, want %s (make tools)\n' protoc "$${have:-?}" "$(PROTOC_VERSION)"; ok=0; fi; \
	  else printf '  MISSING %-18s make tools\n' protoc; ok=0; fi; \
	gotool protoc-gen-go      $(PROTOC_GEN_GO_VERSION); \
	gotool protoc-gen-go-grpc $(PROTOC_GEN_GO_GRPC_VERSION); \
	gotool grpcurl            $(GRPCURL_VERSION); \
	echo; \
	case "$$(uname -s)" in \
	  Darwin) echo "macOS: Docker Desktop or Podman Desktop; Go from go.dev/dl, Homebrew, asdf or mise; uv from the installer, Homebrew, asdf or mise." ;; \
	  Linux)  echo "Linux: Docker Engine + compose plugin from your distro or docs.docker.com, or Podman + docker-compose; Go from go.dev/dl or asdf/mise; uv from the installer or asdf/mise." ;; \
	  *)      echo "Windows: use WSL2 (Ubuntu) with Docker Desktop's WSL integration, then follow the Linux notes inside WSL." ;; \
	esac; \
	[ $$ok = 1 ] && echo "all good" || { echo "fix the items above (make tools handles the ./bin section)"; exit 1; }

tools: ## Fetch the pinned protoc, protoc-gen-go, protoc-gen-go-grpc and grpcurl into ./bin
	@mkdir -p "$(BIN)"
	$(install_protoc_gen_go)
	$(install_protoc_gen_go_grpc)
	$(install_grpcurl)
	$(install_protoc)

# On-demand rules so `make generate` or `make quote` work without an explicit `make tools`.
bin/protoc-gen-go: ; @mkdir -p "$(BIN)" && $(install_protoc_gen_go)
bin/protoc-gen-go-grpc: ; @mkdir -p "$(BIN)" && $(install_protoc_gen_go_grpc)
bin/grpcurl: ; @mkdir -p "$(BIN)" && $(install_grpcurl)
bin/protoc: ; @mkdir -p "$(BIN)" && $(install_protoc)

env-init: ## Create .env from .env.example so host ports can be overridden (no-op if .env exists)
	@if [ -e .env ]; then echo ".env already exists; left unchanged"; else cp .env.example .env && echo "created .env; edit the ports you need, then make up"; fi

ports: ## Show which host ports the stack will use and whether each is free (changes nothing)
	@go run ./tools/ports -check

urls: ## Print the local addresses of every service and tool
	@printf '  %-34s %s\n' 'gRPC API (reflection on)'          '$(GRPC_ADDR)'
	@printf '  %-34s %s\n' 'shipment-service management HTTP' 'http://127.0.0.1:$(SHIPMENT_MGMT_PORT)  (/healthz /readyz /metrics)'
	@printf '  %-34s %s\n' 'outbox-relay management HTTP'     'http://127.0.0.1:$(RELAY_MGMT_PORT)'
	@printf '  %-34s %s\n' 'dispatch-consumer management HTTP' 'http://127.0.0.1:$(CONSUMER_MGMT_PORT)'
	@printf '  %-34s %s\n' 'pricing (FastAPI)'                 'http://127.0.0.1:$(PRICING_PORT)  (POST /quote, /healthz, /metrics)'
	@printf '  %-34s %s\n' 'PostgreSQL'                        '127.0.0.1:$(POSTGRES_PORT)  user/db parcellab'
	@printf '  %-34s %s\n' 'Kafka (host listener)'             '127.0.0.1:$(KAFKA_HOST_PORT)'
	@printf '  %-34s %s\n' 'Kafka UI'                          'http://127.0.0.1:$(KAFKA_UI_PORT)'
	@printf '  %-34s %s\n' 'Prometheus'                        'http://127.0.0.1:$(PROMETHEUS_PORT)'

##@ Stack lifecycle (Docker Compose)

up: ## Build and start the whole stack on free host ports (busy ones are swapped and recorded in .env)
	@go run ./tools/ports
	$(COMPOSE) up -d --build --wait --wait-timeout 180
	@echo
	@$(MAKE) --no-print-directory urls
	@echo
	@echo "Run 'make smoke' to verify the whole path."

down: ## Stop the stack; data volumes are kept
	$(COMPOSE) --profile tools down --remove-orphans

reset: ## Stop the stack AND delete its data volumes (destructive)
	$(COMPOSE) --profile tools down --remove-orphans --volumes

restart: ## Restart the application services without touching Postgres or Kafka
	$(COMPOSE) restart shipment-service pricing outbox-relay dispatch-consumer

build: ## Build the Go and Python images without starting anything
	$(COMPOSE) --profile tools build

pull: ## Pull the third-party images (Postgres, Kafka, Kafka UI, Prometheus) ahead of time
	$(COMPOSE) pull --ignore-buildable

ps: ## Show container status
	$(COMPOSE) ps -a

logs: ## Follow logs of the four application services
	$(COMPOSE) logs -f shipment-service pricing outbox-relay dispatch-consumer

logs-service: ## Follow shipment-service logs
	$(COMPOSE) logs -f shipment-service

logs-pricing: ## Follow pricing logs
	$(COMPOSE) logs -f pricing

logs-relay: ## Follow outbox-relay logs
	$(COMPOSE) logs -f outbox-relay

logs-consumer: ## Follow dispatch-consumer logs
	$(COMPOSE) logs -f dispatch-consumer

##@ Tests and checks

test: test-go test-python ## Run all unit tests (no stack needed)

test-go: vet ## Go unit tests with the race detector
	go test -race -count=1 ./...

test-python: ## Python unit tests (pricing rules and the FastAPI app)
	cd pricing && uv sync --frozen --quiet && uv run pytest -q

test-integration: ## Storage tests against the running Compose PostgreSQL (needs `make up`)
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -count=1 ./internal/storage

smoke: ## End-to-end check: gRPC -> pricing -> Postgres -> Kafka -> consumer -> replay -> metrics
	$(COMPOSE) --profile tools run --rm --build smoke

lint: vet generate-check ## go vet, gofmt, and committed bindings match the proto
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	@echo "lint ok"

vet: ## go vet
	go vet ./...

fmt: ## gofmt the Go sources in place
	gofmt -w .

##@ Contract (Protocol Buffers)

generate: bin/protoc bin/protoc-gen-go bin/protoc-gen-go-grpc ## Regenerate Go bindings in gen/ from proto/parcellab/v1/shipment.proto
	"$(BIN)/protoc" -I proto -I "$(PROTOC_INCLUDE)" \
	  --go_out=$(GEN_OUT) --go_opt=paths=source_relative \
	  --go-grpc_out=$(GEN_OUT) --go-grpc_opt=paths=source_relative \
	  $(PROTO)

generate-check: ## Fail if gen/ differs from what the proto generates (does not touch gen/)
	@tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	$(MAKE) --no-print-directory generate GEN_OUT="$$tmp" >/dev/null; \
	if diff -r "$$tmp" gen; then echo "generated bindings up to date"; \
	else echo "gen/ is out of date (or a different generator version is installed): run 'make generate'"; exit 1; fi

##@ Talk to the running stack

grpc-list: bin/grpcurl ## List the gRPC methods via reflection
	$(GRPCURL) -plaintext $(GRPC_ADDR) list $(GRPC_SVC)

quote: bin/grpcurl ## GetQuote for WEIGHT_GRAMS (default 1500); REQUEST_JSON='{...}' sends any request body
	$(GRPCURL) -plaintext -d '$(REQUEST_JSON)' $(GRPC_ADDR) $(GRPC_SVC)/GetQuote

create: bin/grpcurl ## CreateShipment for WEIGHT_GRAMS with CORRELATION_ID (default req_make_<epoch>); REQUEST_JSON='{...}' overrides the body
	$(GRPCURL) -plaintext -H 'x-correlation-id: $(CORRELATION_ID)' \
	  -d '$(REQUEST_JSON)' $(GRPC_ADDR) $(GRPC_SVC)/CreateShipment

get: bin/grpcurl ## GetShipment: make get SHIPMENT_ID=shp_...
	@test -n "$(SHIPMENT_ID)" || { echo "usage: make get SHIPMENT_ID=shp_..."; exit 2; }
	$(GRPCURL) -plaintext -d '{"shipment_id": "$(SHIPMENT_ID)"}' $(GRPC_ADDR) $(GRPC_SVC)/GetShipment

replay: ## Republish one outbox event with its original event ID: make replay EVENT_ID=evt_...
	@test -n "$(EVENT_ID)" || { echo "usage: make replay EVENT_ID=evt_..."; exit 2; }
	$(COMPOSE) --profile tools run --rm replay "$(EVENT_ID)"

psql: ## Interactive psql session on the parcellab database
	$(COMPOSE) exec postgres psql -U parcellab -d parcellab

sql: ## Run an ad-hoc statement: make sql Q="select count(*) from shipments"
	@test -n "$(Q)" || { echo 'usage: make sql Q="select ..."'; exit 2; }
	$(PSQL) -c "$(Q)"

sql-shipment: ## One shipment end to end (shipment, outbox, dispatch): make sql-shipment SHIPMENT_ID=shp_...
	@test -n "$(SHIPMENT_ID)" || { echo "usage: make sql-shipment SHIPMENT_ID=shp_..."; exit 2; }
	$(PSQL) -c "SELECT s.shipment_id, s.weight_grams, s.amount_cents, s.currency, o.event_id, \
	  o.published_at IS NOT NULL AS published, o.attempts, d.dispatch_id, d.dispatched_at \
	  FROM shipments s LEFT JOIN outbox_events o USING (shipment_id) LEFT JOIN dispatches d USING (shipment_id) \
	  WHERE s.shipment_id = '$(SHIPMENT_ID)'"

sql-dispatch-counts: ## Dispatches per shipment; must never exceed 1
	$(PSQL) -c "SELECT shipment_id, count(*) AS dispatches FROM dispatches GROUP BY shipment_id ORDER BY dispatches DESC, shipment_id LIMIT 20"

sql-pending: ## Outbox events not yet published (grows while Kafka is down)
	$(PSQL) -c "SELECT event_id, shipment_id, created_at, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY created_at"

sql-processed: ## Event IDs the consumer has already handled (the deduplication table)
	$(PSQL) -c "SELECT event_id, processed_at FROM processed_events ORDER BY processed_at DESC LIMIT 20"

sql-event: ## The stored payload of one outbox event: make sql-event EVENT_ID=evt_...
	@test -n "$(EVENT_ID)" || { echo "usage: make sql-event EVENT_ID=evt_..."; exit 2; }
	$(PSQL) -c "SELECT jsonb_pretty(payload) FROM outbox_events WHERE event_id = '$(EVENT_ID)'"

kafka-topics: ## Describe the shipment.created.v1 topic (partitions, leader)
	$(KAFKA_EXEC) $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $(TOPIC)

kafka-messages: ## Dump every record on shipment.created.v1 with key and headers (stops after 5 s idle)
	$(KAFKA_EXEC) $(KAFKA_BIN)/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
	  --topic $(TOPIC) --from-beginning --timeout-ms 5000 \
	  --formatter-property print.key=true --formatter-property print.headers=true --formatter-property print.partition=true --formatter-property print.offset=true

kafka-lag: ## Offsets and lag of the dispatch-consumer group
	$(KAFKA_EXEC) $(KAFKA_BIN)/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $(GROUP)

metrics: ## Print the parcellab_* and pricing_* metrics from every service (histogram buckets omitted)
	@for ep in shipment-service:$(SHIPMENT_MGMT_PORT) outbox-relay:$(RELAY_MGMT_PORT) dispatch-consumer:$(CONSUMER_MGMT_PORT) pricing:$(PRICING_PORT); do \
	  echo "== $${ep%%:*}"; \
	  if body="$$(curl -fsS "http://127.0.0.1:$${ep##*:}/metrics")"; then \
	    grep -E '^(parcellab|pricing)_' <<<"$$body" | grep -v '_bucket{' || echo "  (no parcellab_* series yet)"; \
	  else echo "  unreachable"; fi; \
	done

prometheus-targets: ## Health of every Prometheus scrape target
	@set -o pipefail; curl -fsS "http://127.0.0.1:$(PROMETHEUS_PORT)/api/v1/targets" \
	  | grep -oE '"(scrapeUrl|health)":"[^"]*"' | paste - - \
	  | sed -E 's/"scrapeUrl":"([^"]*)"[[:space:]]+"health":"([^"]*)"/  \2\t\1/'

##@ Failure scenarios (interview discussion)

kafka-stop: ## Take the broker down; CreateShipment keeps working, outbox backlog grows
	$(COMPOSE) stop kafka

kafka-start: ## Bring the broker back; the relay drains pending events
	$(COMPOSE) start kafka

pricing-slow: ## Make every quote take DELAY_MS (default 5000) so CreateShipment hits DEADLINE_EXCEEDED
	PRICING_ARTIFICIAL_DELAY_MS=$(DELAY_MS) $(COMPOSE) up -d --no-deps --wait --wait-timeout 60 pricing

pricing-normal: ## Remove the artificial pricing delay
	PRICING_ARTIFICIAL_DELAY_MS=0 $(COMPOSE) up -d --no-deps --wait --wait-timeout 60 pricing

pricing-stop: ## Take the pricing service down; quotes fail with UNAVAILABLE, nothing is stored
	$(COMPOSE) stop pricing

pricing-start: ## Bring the pricing service back
	$(COMPOSE) start pricing

poison-event: ## Publish a malformed record; the consumer rejects it, exits, and restarts. Recovery: make reset && make up
	@echo "This leaves an uncommitted poison record on one partition; the consumer crash-loops until 'make reset && make up'. Ctrl-C within 5 s to abort."; sleep 5
	echo 'not json at all' | $(KAFKA_EXEC) $(KAFKA_BIN)/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $(TOPIC)
	@echo "Now: make logs-consumer   /   make kafka-lag   /   make ps"
