-include .env
export

DIST_DIR ?= dist
GOCACHE ?= $(CURDIR)/.gocache
GOMODCACHE ?= $(CURDIR)/.gomodcache
GO ?= go

.PHONY: build-dirs
build-dirs:
	mkdir -p $(DIST_DIR)

.PHONY: install-hooks
install-hooks:
	./scripts/install-hooks.sh

.PHONY: hooks-status
hooks-status:
	git config --get core.hooksPath

.PHONY: lint
lint:
	$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.3.0 run ./... --timeout=7m --color always

.PHONY: vuln
vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@v1.1.4 -show verbose ./...

.PHONY: test
test:
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test ./...

.PHONY: build-node-macos-arm64
build-node-macos-arm64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=darwin GOARCH=arm64 $(GO) build -o $(DIST_DIR)/corsa-node-darwin-arm64 ./cmd/corsa-node

.PHONY: build-node-macos-amd64
build-node-macos-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=darwin GOARCH=amd64 $(GO) build -o $(DIST_DIR)/corsa-node-darwin-amd64 ./cmd/corsa-node

.PHONY: build-node-linux-amd64
build-node-linux-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=linux GOARCH=amd64 $(GO) build -o $(DIST_DIR)/corsa-node-linux-amd64 ./cmd/corsa-node

.PHONY: build-node-windows-amd64
build-node-windows-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=windows GOARCH=amd64 $(GO) build -o $(DIST_DIR)/corsa-node-windows-amd64.exe ./cmd/corsa-node

.PHONY: build-desktop-macos-arm64
build-desktop-macos-arm64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=darwin GOARCH=arm64 $(GO) build -o $(DIST_DIR)/corsa-desktop-darwin-arm64 ./cmd/corsa-desktop

.PHONY: build-desktop-macos-amd64
build-desktop-macos-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=darwin GOARCH=amd64 $(GO) build -o $(DIST_DIR)/corsa-desktop-darwin-amd64 ./cmd/corsa-desktop

.PHONY: build-desktop-linux-amd64
build-desktop-linux-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=linux GOARCH=amd64 $(GO) build -o $(DIST_DIR)/corsa-desktop-linux-amd64 ./cmd/corsa-desktop

.PHONY: build-desktop-windows-amd64
build-desktop-windows-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=windows GOARCH=amd64 $(GO) build -ldflags="-H windowsgui" -o $(DIST_DIR)/corsa-desktop-windows-amd64.exe ./cmd/corsa-desktop

.PHONY: build-node-all
build-node-all: build-node-macos-arm64 build-node-macos-amd64 build-node-linux-amd64 build-node-windows-amd64

.PHONY: build-desktop-all
build-desktop-all: build-desktop-macos-arm64 build-desktop-macos-amd64 build-desktop-linux-amd64 build-desktop-windows-amd64

.PHONY: build-all
build-all: build-node-all build-desktop-all
