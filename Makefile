-include .env
export

.PHONY: install-hooks
install-hooks:
	./scripts/install-hooks.sh

.PHONY: hooks-status
hooks-status:
	git config --get core.hooksPath

.PHONY: lint
lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.3.0 run ./... --timeout=7m --color always

.PHONY: vuln
vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 -show verbose ./...
