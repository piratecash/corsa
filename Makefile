-include .env
export

DIST_DIR ?= dist
PREFIX ?= /usr/local
GOCACHE ?= $(CURDIR)/.gocache
GOMODCACHE ?= $(CURDIR)/.gomodcache
GOFLAGS ?= -trimpath
GO_LDFLAGS ?= -s -w
GO ?= go
APP_ID ?= chat.corsa
APP_NAME ?= Corsa
DESKTOP_ICON_ICNS ?= assets/icons/app-icon.icns
DESKTOP_ICON_ICO ?= assets/icons/app-icon.ico
GO_DESKTOP_LDFLAGS ?= $(GO_LDFLAGS) -X gioui.org/app.ID=$(APP_ID)
GO_WINDOWS_DESKTOP_LDFLAGS ?= $(GO_DESKTOP_LDFLAGS) -H windowsgui
RSRC ?= $(GO) tool rsrc

.PHONY: build-dirs
build-dirs:
	mkdir -p $(DIST_DIR)

.PHONY: install-hooks
install-hooks:
	./scripts/install-hooks.sh

.PHONY: hooks-status
hooks-status:
	git config --get core.hooksPath

.PHONY: mocks
mocks:
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) run github.com/vektra/mockery/v3@v3.7.0

.PHONY: lint
lint:
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.3.0 run ./... --timeout=7m --color always

.PHONY: fmt gofmt-changed
fmt:
	@files=$$(git status --porcelain | awk '{print $$2}' | grep '\.go$$' || true); \
	if [ -n "$$files" ]; then \
		gofmt -w $$files; \
		gofumpt -w $$files; \
	else \
		echo "No changed .go files to format"; \
	fi

gofmt-changed: fmt

.PHONY: vuln
vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@v1.1.4 -show verbose ./...

.PHONY: test
test:
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -timeout=15m ./...

.PHONY: test-v
test-v:
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -v -count=1 -timeout=15m ./...

.PHONY: test-all
test-all: lint enforce-netcore-boundary test-v

# enforce-netcore-boundary runs the §2.9 grep-gate from
# docs/netcore-migration.md: asserts that forbidden patterns (direct
# socket writes outside owner, parallel net.Conn registries, stale
# net.Conn-first wrappers, carve-out membership growth beyond the frozen
# 14, etc.) have not regressed. Fails non-zero on any drift.
# See scripts/enforce-netcore-boundary.sh for the concrete baselines.
.PHONY: enforce-netcore-boundary
enforce-netcore-boundary:
	@./scripts/enforce-netcore-boundary.sh

.PHONY: build-node-macos-arm64
build-node-macos-arm64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=darwin GOARCH=arm64 $(GO) build -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/corsa-node-darwin-arm64 ./cmd/corsa-node

.PHONY: build-node-macos-amd64
build-node-macos-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=darwin GOARCH=amd64 $(GO) build -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/corsa-node-darwin-amd64 ./cmd/corsa-node

.PHONY: build-node-linux-amd64
build-node-linux-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=linux GOARCH=amd64 $(GO) build -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/corsa-node-linux-amd64 ./cmd/corsa-node

.PHONY: build-node-windows-amd64
build-node-windows-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=windows GOARCH=amd64 $(GO) build -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/corsa-node-windows-amd64.exe ./cmd/corsa-node

.PHONY: build-node-windows-arm64
build-node-windows-arm64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=windows GOARCH=arm64 $(GO) build -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/corsa-node-windows-arm64.exe ./cmd/corsa-node

.PHONY: build-desktop-macos-arm64
build-desktop-macos-arm64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) CLANG_MODULE_CACHE_PATH=$(GOCACHE)/clang-module-cache CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 $(GO) build -ldflags="$(GO_DESKTOP_LDFLAGS)" -o $(DIST_DIR)/corsa-desktop-darwin-arm64 ./cmd/corsa-desktop
	mkdir -p $(DIST_DIR)/$(APP_NAME)-darwin-arm64.app/Contents/MacOS $(DIST_DIR)/$(APP_NAME)-darwin-arm64.app/Contents/Resources
	cp $(DIST_DIR)/corsa-desktop-darwin-arm64 $(DIST_DIR)/$(APP_NAME)-darwin-arm64.app/Contents/MacOS/$(APP_NAME)
	cp packaging/macos/Info.plist $(DIST_DIR)/$(APP_NAME)-darwin-arm64.app/Contents/Info.plist
	cp $(DESKTOP_ICON_ICNS) $(DIST_DIR)/$(APP_NAME)-darwin-arm64.app/Contents/Resources/app-icon.icns

.PHONY: build-desktop-macos-amd64
build-desktop-macos-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) CLANG_MODULE_CACHE_PATH=$(GOCACHE)/clang-module-cache CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 $(GO) build -ldflags="$(GO_DESKTOP_LDFLAGS)" -o $(DIST_DIR)/corsa-desktop-darwin-amd64 ./cmd/corsa-desktop
	mkdir -p $(DIST_DIR)/$(APP_NAME)-darwin-amd64.app/Contents/MacOS $(DIST_DIR)/$(APP_NAME)-darwin-amd64.app/Contents/Resources
	cp $(DIST_DIR)/corsa-desktop-darwin-amd64 $(DIST_DIR)/$(APP_NAME)-darwin-amd64.app/Contents/MacOS/$(APP_NAME)
	cp packaging/macos/Info.plist $(DIST_DIR)/$(APP_NAME)-darwin-amd64.app/Contents/Info.plist
	cp $(DESKTOP_ICON_ICNS) $(DIST_DIR)/$(APP_NAME)-darwin-amd64.app/Contents/Resources/app-icon.icns

.PHONY: build-desktop-linux-amd64
build-desktop-linux-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=linux GOARCH=amd64 $(GO) build -ldflags="$(GO_DESKTOP_LDFLAGS)" -o $(DIST_DIR)/corsa-desktop-linux-amd64 ./cmd/corsa-desktop

.PHONY: build-desktop-windows-amd64
build-desktop-windows-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(RSRC) -arch amd64 -ico $(DESKTOP_ICON_ICO) -o cmd/corsa-desktop/corsa-icon_windows_amd64.syso
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=windows GOARCH=amd64 $(GO) build -ldflags="$(GO_WINDOWS_DESKTOP_LDFLAGS)" -o $(DIST_DIR)/corsa-desktop-windows-amd64.exe ./cmd/corsa-desktop

.PHONY: build-desktop-windows-arm64
build-desktop-windows-arm64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(RSRC) -arch arm64 -ico $(DESKTOP_ICON_ICO) -o cmd/corsa-desktop/corsa-icon_windows_arm64.syso
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=windows GOARCH=arm64 $(GO) build -ldflags="$(GO_WINDOWS_DESKTOP_LDFLAGS)" -o $(DIST_DIR)/corsa-desktop-windows-arm64.exe ./cmd/corsa-desktop

.PHONY: install-desktop-linux
install-desktop-linux: build-desktop-linux-amd64
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 0755 $(DIST_DIR)/corsa-desktop-linux-amd64 $(DESTDIR)$(PREFIX)/bin/corsa-desktop
	install -d $(DESTDIR)$(PREFIX)/share/applications
	install -m 0644 packaging/linux/corsa.desktop $(DESTDIR)$(PREFIX)/share/applications/corsa.desktop
	install -d $(DESTDIR)$(PREFIX)/share/icons/hicolor/256x256/apps
	install -m 0644 assets/icons/png/app-icon-256.png $(DESTDIR)$(PREFIX)/share/icons/hicolor/256x256/apps/corsa.png

.PHONY: build-node-all
build-node-all: build-node-macos-arm64 build-node-macos-amd64 build-node-linux-amd64 build-node-windows-amd64 build-node-windows-arm64

.PHONY: build-desktop-all
build-desktop-all: build-desktop-macos-arm64 build-desktop-macos-amd64 build-desktop-linux-amd64 build-desktop-windows-amd64 build-desktop-windows-arm64

.PHONY: build-cli-macos-arm64
build-cli-macos-arm64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=darwin GOARCH=arm64 $(GO) build -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/corsa-cli-darwin-arm64 ./cmd/corsa-cli

.PHONY: build-cli-macos-amd64
build-cli-macos-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=darwin GOARCH=amd64 $(GO) build -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/corsa-cli-darwin-amd64 ./cmd/corsa-cli

.PHONY: build-cli-linux-amd64
build-cli-linux-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=linux GOARCH=amd64 $(GO) build -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/corsa-cli-linux-amd64 ./cmd/corsa-cli

.PHONY: build-cli-windows-amd64
build-cli-windows-amd64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=windows GOARCH=amd64 $(GO) build -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/corsa-cli-windows-amd64.exe ./cmd/corsa-cli

.PHONY: build-cli-windows-arm64
build-cli-windows-arm64: build-dirs
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOOS=windows GOARCH=arm64 $(GO) build -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/corsa-cli-windows-arm64.exe ./cmd/corsa-cli

.PHONY: build-cli-all
build-cli-all: build-cli-macos-arm64 build-cli-macos-amd64 build-cli-linux-amd64 build-cli-windows-amd64 build-cli-windows-arm64

.PHONY: build-all
build-all: build-node-all build-desktop-all build-cli-all
