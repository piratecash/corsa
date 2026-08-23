-include .env
export

DIST_DIR ?= dist
PREFIX ?= /usr/local
GOCACHE ?= $(CURDIR)/.gocache
GOMODCACHE ?= $(CURDIR)/.gomodcache
GOFLAGS ?= -trimpath
GO_LDFLAGS ?= -s -w
GO ?= go
# Desktop application identity: gioui.org/app.ID (Wayland app_id, DBus,
# Windows AppUserModelID) and the macOS bundle identifier in
# packaging/macos/Info.plist. NOT the Android package — see
# ANDROID_APPID.
APP_ID ?= chat.corsa
APP_NAME ?= Corsa
DESKTOP_ICON_ICNS ?= assets/icons/app-icon.icns
DESKTOP_ICON_ICO ?= assets/icons/app-icon.ico
GO_DESKTOP_LDFLAGS ?= $(GO_LDFLAGS) -X gioui.org/app.ID=$(APP_ID)
GO_WINDOWS_DESKTOP_LDFLAGS ?= $(GO_DESKTOP_LDFLAGS) -H windowsgui
RSRC ?= $(GO) tool rsrc
# Android SDK autodetect (Android Studio default location on macOS);
# override via environment or .env if the SDK lives elsewhere.
ANDROID_HOME ?= $(wildcard $(HOME)/Library/Android/sdk)
# Android application ID (package name). Deliberately NOT derived from
# APP_ID: the Play package is chat.corsa.app, while the desktop identity
# stays chat.corsa (and must keep matching Info.plist). It also becomes
# the AAR namespace gogio emits and, with a ".wrapper" suffix, the Gradle
# wrapper module's namespace. Changing it after a release breaks updates:
# Android treats a different package as a different app.
ANDROID_APPID ?= chat.corsa.app
# Android 8.0+. Guarantees Context.getNoBackupFilesDir (API 21 — the
# private-key/chatlog backup exclusion in cmd/corsa-android relies on it)
# and makes adaptive icons universal, so the single inset icon asset
# below is always rendered through the adaptive pipeline — the legacy
# ic_launcher.png (where the inset mark looks undersized) never shows.
ANDROID_MINSDK ?= 26
# Google Play requires new apps and updates to target API 36 from
# 2026-08-31; gogio v0.10 would otherwise default to its hardcoded 35.
ANDROID_TARGETSDK ?= 36
# Android adaptive icons crop the outer third of the foreground layer. Keep a
# dedicated inset foreground so the full mark stays inside the launcher safe
# zone; the opaque image remains the fallback launcher icon.
# The standard Android wrapper packages it as a transparent foreground over
# this opaque color resource, giving launchers two genuinely independent layers.
ANDROID_ICON ?= assets/icons/png/app-icon-android-512.png
ANDROID_ADAPTIVE_ICON_FOREGROUND ?= assets/icons/png/app-icon-android-foreground-512.png
ANDROID_ADAPTIVE_ICON_BACKGROUND ?= \#000000
# Android version derives from the canonical Corsa release version —
# config.ClientVersionMajor/Minor/Build in internal/core/config/config.go
# (the single place bumped on a release). versionCode is computed as
# MAJOR*1000000 + MINOR*1000 + BUILD (1.0.63 → 1000063), so it strictly
# increases with every release and Android accepts each update.
CORSA_VERSION := $(shell awk '/ClientVersionMajor =/{ma=$$3} /ClientVersionMinor =/{mi=$$3} /ClientVersionBuild =/{bu=$$3} END{printf "%d.%d.%d", ma, mi, bu}' internal/core/config/config.go)
ANDROID_VERSION_CODE := $(shell awk '/ClientVersionMajor =/{ma=$$3} /ClientVersionMinor =/{mi=$$3} /ClientVersionBuild =/{bu=$$3} END{printf "%d", ma*1000000+mi*1000+bu}' internal/core/config/config.go)
# Extra gogio flags appended to every android build.
GOGIO_EXTRA_FLAGS ?=
# Release signing (build-android-release / build-android-aab): path to
# the release keystore and its password. Deliberately no defaults — the
# release targets refuse to run without them, so a debug-signed APK can
# never masquerade as a release artifact. NB: a debug-signed install
# cannot be UPDATED by a release-signed one (signature mismatch) — the
# app must be uninstalled first, which also wipes the identity in
# no_backup storage.
#
# Signing is done after Gradle produces unsigned release artifacts.
# apksigner (APK) and jarsigner (AAB) both accept the password from an
# environment variable, so it never enters any argv. gogio and Gradle run
# with the secret scrubbed from
# the environment (env -u). Residual exposure when using the plain
# _PASS variable: make's global `export` still hands it to the OTHER
# recipe shells and targets of this file (only the gogio lines are
# scrubbed) — which is why ANDROID_KEYSTORE_PASS_FILE is the
# recommended form: the broadly-exported value is then only a file
# path, and the password itself is read at signing time. Both variants
# reach the signer via environment values, never via make text
# interpolation — quotes and shell-special characters in the password
# are safe. Recipes are @-silenced, so nothing password-adjacent lands
# in the build log. Store and key password are assumed identical (the
# apksigner/jarsigner default). AAB signing additionally needs the key
# alias in ANDROID_KEY_ALIAS.
ANDROID_KEYSTORE ?=
ANDROID_KEYSTORE_PASS ?=
ANDROID_KEYSTORE_PASS_FILE ?=
ANDROID_KEY_ALIAS ?=
ANDROID_BUILD_TOOLS_DIR = $(shell ls -d "$(ANDROID_HOME)"/build-tools/*/ 2>/dev/null | sort -V | tail -1)
# Official gogio package pinned as a Go tool in go.mod; update it with
# `go get -tool gioui.org/cmd/gogio@latest`.
GOGIO ?= $(GO) tool gogio
ANDROID_GRADLE_DIR ?= packaging/android
ANDROID_GRADLE ?= ./gradlew
ANDROID_GIO_ARCHIVE = $(abspath $(DIST_DIR)/corsa-android.aar)
ANDROID_GRADLE_ARGS = --no-daemon \
	"-PcorsaAppId=$(ANDROID_APPID)" \
	"-PcorsaAppName=$(APP_NAME)" \
	"-PcorsaCompileSdk=$(ANDROID_TARGETSDK)" \
	"-PcorsaMinSdk=$(ANDROID_MINSDK)" \
	"-PcorsaTargetSdk=$(ANDROID_TARGETSDK)" \
	"-PcorsaVersionCode=$(ANDROID_VERSION_CODE)" \
	"-PcorsaVersionName=$(CORSA_VERSION)" \
	"-PcorsaAdaptiveIconBackground=$(ANDROID_ADAPTIVE_ICON_BACKGROUND)" \
	"-PcorsaLegacyIcon=$(abspath $(ANDROID_ICON))" \
	"-PcorsaAdaptiveIconForeground=$(abspath $(ANDROID_ADAPTIVE_ICON_FOREGROUND))" \
	"-PcorsaGioArchive=$(ANDROID_GIO_ARCHIVE)"

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
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1 run ./... --timeout=7m --color always

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

# The storage tests against the cgo SQLite driver. That driver ships on Android
# only, and no CI runner runs Android, so without this target its DSN handling,
# WAL behaviour and transaction semantics were never exercised — a divergence
# would have surfaced in an Android build.
.PHONY: test-cgo
test-cgo:
	CGO_ENABLED=1 GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -tags sqlite_cgo -timeout=15m ./internal/core/storage/... ./internal/core/chatlog/...

.PHONY: test-v
test-v:
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -v -count=1 -timeout=15m ./...

.PHONY: test-all
test-all: lint enforce-netcore-boundary test-v test-cgo

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

# check-android-minsdk gates every android build: the private-key backup
# exclusion (Context.getNoBackupFilesDir in cmd/corsa-android) hard-requires
# API 21; below it the JNI lookup panics at startup.
.PHONY: check-android-minsdk
check-android-minsdk:
	@[ "$(ANDROID_MINSDK)" -ge 21 ] || { echo "ANDROID_MINSDK=$(ANDROID_MINSDK) is below 21: getNoBackupFilesDir (identity backup exclusion) requires API 21+"; exit 1; }

# check-android-keystore gates the release targets: release artifacts must not
# leave the build unsigned or use Gradle's development certificate.
.PHONY: check-android-keystore
check-android-keystore:
	@[ -n "$(ANDROID_KEYSTORE)" ] || { echo "ANDROID_KEYSTORE is not set (path to the release keystore)"; exit 1; }
	@[ -f "$(ANDROID_KEYSTORE)" ] || { echo "ANDROID_KEYSTORE=$(ANDROID_KEYSTORE): file not found"; exit 1; }
	@[ -n "$$ANDROID_KEYSTORE_PASS$$ANDROID_KEYSTORE_PASS_FILE" ] || { echo "set ANDROID_KEYSTORE_PASS_FILE (preferred) or ANDROID_KEYSTORE_PASS"; exit 1; }
	@[ -z "$$ANDROID_KEYSTORE_PASS_FILE" ] || [ -f "$$ANDROID_KEYSTORE_PASS_FILE" ] || { echo "ANDROID_KEYSTORE_PASS_FILE: file not found"; exit 1; }
	@[ -n "$(ANDROID_BUILD_TOOLS_DIR)" ] || { echo "no Android build-tools found under $(ANDROID_HOME)/build-tools (apksigner/jarsigner re-signing needs them)"; exit 1; }
	@[ "$(ANDROID_TARGETSDK)" -ge 36 ] || { echo "ANDROID_TARGETSDK=$(ANDROID_TARGETSDK) is below 36: Google Play rejects new apps/updates targeting less (from 2026-08-31)"; exit 1; }

# Android builds use gogio's public archive mode to produce an AAR, then the
# standard Android Gradle plugin owns the manifest, resources, APK and AAB.
# This keeps custom resources (including the two adaptive-icon layers) outside
# gogio and allows the pinned tool to be upgraded without carrying a source fork.
# Prerequisites: a JDK and an Android SDK with an NDK (>= r19c) installed;
# set ANDROID_HOME to the SDK root — gogio picks the newest NDK from
# $$ANDROID_HOME/ndk/.
.PHONY: build-android
build-android: build-dirs check-android-minsdk
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) env -u ANDROID_KEYSTORE_PASS -u ANDROID_KEYSTORE_PASS_FILE $(GOGIO) -target android -buildmode archive -minsdk $(ANDROID_MINSDK) -targetsdk $(ANDROID_TARGETSDK) -appid $(ANDROID_APPID) $(GOGIO_EXTRA_FLAGS) -o $(ANDROID_GIO_ARCHIVE) ./cmd/corsa-android
	cd $(ANDROID_GRADLE_DIR) && env -u ANDROID_KEYSTORE_PASS -u ANDROID_KEYSTORE_PASS_FILE $(ANDROID_GRADLE) :app:assembleDebug $(ANDROID_GRADLE_ARGS)
	cp $(ANDROID_GRADLE_DIR)/app/build/outputs/apk/debug/app-debug.apk $(DIST_DIR)/corsa-android.apk

# build-android-debug is build-android with info-level logcat logging
# (build tag corsadebug — see internal/core/crashlog/logcat_level_*.go).
# Release builds default to warn to avoid leaking identities/message IDs
# into logcat and burning battery on steady-state chatter.
.PHONY: build-android-debug
build-android-debug: build-dirs check-android-minsdk
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) env -u ANDROID_KEYSTORE_PASS -u ANDROID_KEYSTORE_PASS_FILE $(GOGIO) -target android -buildmode archive -minsdk $(ANDROID_MINSDK) -targetsdk $(ANDROID_TARGETSDK) -appid $(ANDROID_APPID) -tags corsadebug $(GOGIO_EXTRA_FLAGS) -o $(ANDROID_GIO_ARCHIVE) ./cmd/corsa-android
	cd $(ANDROID_GRADLE_DIR) && env -u ANDROID_KEYSTORE_PASS -u ANDROID_KEYSTORE_PASS_FILE $(ANDROID_GRADLE) :app:assembleDebug $(ANDROID_GRADLE_ARGS)
	cp $(ANDROID_GRADLE_DIR)/app/build/outputs/apk/debug/app-debug.apk $(DIST_DIR)/corsa-android-debug.apk

# build-android-release produces a release-signed APK: Gradle builds an
# unsigned release APK, then apksigner signs it. On a keystore with more than one
# key entry set ANDROID_KEY_ALIAS (passed via env; optional for
# single-key keystores) — apksigner strips the previous
# signature and preserves zipalign. Requires ANDROID_KEYSTORE plus
# ANDROID_KEYSTORE_PASS_FILE or ANDROID_KEYSTORE_PASS (e.g. via .env).
# Keep ONE release keystore for the app's lifetime — Android updates are
# only accepted from the same signing key.
.PHONY: build-android-release
build-android-release: build-dirs check-android-minsdk check-android-keystore
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) env -u ANDROID_KEYSTORE_PASS -u ANDROID_KEYSTORE_PASS_FILE $(GOGIO) -target android -buildmode archive -minsdk $(ANDROID_MINSDK) -targetsdk $(ANDROID_TARGETSDK) -appid $(ANDROID_APPID) $(GOGIO_EXTRA_FLAGS) -o $(ANDROID_GIO_ARCHIVE) ./cmd/corsa-android
	cd $(ANDROID_GRADLE_DIR) && env -u ANDROID_KEYSTORE_PASS -u ANDROID_KEYSTORE_PASS_FILE $(ANDROID_GRADLE) :app:assembleRelease $(ANDROID_GRADLE_ARGS)
	cp $(ANDROID_GRADLE_DIR)/app/build/outputs/apk/release/app-release-unsigned.apk $(DIST_DIR)/corsa-android-release.apk
	@echo "re-signing APK with $(ANDROID_KEYSTORE) (apksigner, password via env)"
	@pass="$$ANDROID_KEYSTORE_PASS"; \
	if [ -n "$$ANDROID_KEYSTORE_PASS_FILE" ]; then pass="$$(cat "$$ANDROID_KEYSTORE_PASS_FILE")"; fi; \
	set -- ; \
	if [ -n "$$ANDROID_KEY_ALIAS" ]; then set -- --ks-key-alias "$$ANDROID_KEY_ALIAS"; fi; \
	KS_PASS="$$pass" "$(ANDROID_BUILD_TOOLS_DIR)apksigner" sign --ks "$(ANDROID_KEYSTORE)" --ks-pass env:KS_PASS "$$@" "$(DIST_DIR)/corsa-android-release.apk"
	@"$(ANDROID_BUILD_TOOLS_DIR)apksigner" verify "$(DIST_DIR)/corsa-android-release.apk" && echo "release apk signature verified"

# build-android-aab produces a release-signed Android App Bundle for
# store upload. Gradle builds the unsigned bundle and jarsigner signs it
# with the release key, password via environment
# (-storepass:env). jarsigner needs the key alias — ANDROID_KEY_ALIAS.
.PHONY: build-android-aab
build-android-aab: build-dirs check-android-minsdk check-android-keystore
	@[ -n "$$ANDROID_KEY_ALIAS" ] || { echo "ANDROID_KEY_ALIAS is not set (jarsigner needs the key alias for AAB signing)"; exit 1; }
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) env -u ANDROID_KEYSTORE_PASS -u ANDROID_KEYSTORE_PASS_FILE $(GOGIO) -target android -buildmode archive -minsdk $(ANDROID_MINSDK) -targetsdk $(ANDROID_TARGETSDK) -appid $(ANDROID_APPID) $(GOGIO_EXTRA_FLAGS) -o $(ANDROID_GIO_ARCHIVE) ./cmd/corsa-android
	cd $(ANDROID_GRADLE_DIR) && env -u ANDROID_KEYSTORE_PASS -u ANDROID_KEYSTORE_PASS_FILE $(ANDROID_GRADLE) :app:bundleRelease $(ANDROID_GRADLE_ARGS)
	cp $(ANDROID_GRADLE_DIR)/app/build/outputs/bundle/release/app-release.aab $(DIST_DIR)/corsa-android.aab
	@echo "signing AAB with $(ANDROID_KEYSTORE) (jarsigner, password via env)"
	@pass="$$ANDROID_KEYSTORE_PASS"; \
	if [ -n "$$ANDROID_KEYSTORE_PASS_FILE" ]; then pass="$$(cat "$$ANDROID_KEYSTORE_PASS_FILE")"; fi; \
	KS_PASS="$$pass" jarsigner -keystore "$(ANDROID_KEYSTORE)" -storepass:env KS_PASS "$(DIST_DIR)/corsa-android.aab" "$$ANDROID_KEY_ALIAS"

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
