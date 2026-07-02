GO ?= go
BUILD_DIR ?= bin

# - go help build
GOFLAGS := -trimpath
# - go tool compile -h
CFLAGS :=
# - go tool link -h
LDFLAGS := -s -w

.DEFAULT_GOAL := help

.PHONY: build
build: ## Build all 'cmd' binaries
	@mkdir -p $(BUILD_DIR)
	@$(GO) build \
		$(GOFLAGS) \
		-ldflags "$(LDFLAGS)" \
		-gcflags "$(CFLAGS)" \
		-o $(BUILD_DIR) \
		./cmd/...

.PHONY: fmt
fmt: ## Format source files
	@$(GO) fmt ./...

.PHONY: lint
lint: ## Run static analysis checks (golangci-lint)
	@golangci-lint run ./...

.PHONY: vuln
vuln: ## Scan for known vulnerabilities (govulncheck)
	@govulncheck ./...

.PHONY: test
test: ## Run tests with race detection and coverage
	@$(GO) test -v -race -cover ./...

.PHONY: check
check: fmt lint vuln test ## Run CI checks

.PHONY: tidy
tidy: ## Update Go module dependencies
	@$(GO) mod tidy

.PHONY: verify
verify: ## Verify Go module checksums
	@$(GO) mod verify

.PHONY: clean
clean: ## Remove build artifacts and test caches
	@rm -rf $(BUILD_DIR) && $(GO) clean -testcache

.PHONY: help
help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "\033[33m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
