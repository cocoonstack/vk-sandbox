GO ?= go
GOTOOLCHAIN ?= go1.26.3
export GOTOOLCHAIN

.PHONY: all build build-linux test race fmt-check vet lint

all: fmt-check vet test build

build:
	$(GO) build -o bin/vk-sandbox .

build-linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build -o bin/vk-sandbox-linux-amd64 .

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

vet:
	$(GO) vet ./...

lint:
	golangci-lint run ./...
