# gocdc 验证命令封装。
# 注意：Go 工具链固定 1.24.x；本开发机默认 PATH 里的 go 是 1.22.2，
# 需将 D:/gopath/go1.24.10/bin 置于 PATH 前列，或 make GO=D:/gopath/go1.24.10/bin/go。
GO ?= go

.PHONY: build web-install web-check web-test web-build vet lint test test-race test-integration clean

# Frontend deps and checks (requires Node.js 22+).
web-install:
	npm --prefix web ci

web-check:
	npm --prefix web run typecheck
	npm --prefix web run lint

web-test:
	npm --prefix web test

web-build: web-install
	npm --prefix web run build

build: web-build
	$(GO) build ./...

vet: web-build
	$(GO) vet ./...

lint: web-build
	golangci-lint run

test: web-build
	$(GO) test ./... -count=1

test-race: web-build
	$(GO) test ./... -race -count=1

# 集成测试：需 Docker Desktop（//go:build integration 标签隔离）
test-integration: web-build
	$(GO) test ./... -tags=integration -count=1

clean:
	$(GO) clean ./...
