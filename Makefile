VERSION ?= dev
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
PKG     := github.com/genadigeno/data-feed/internal/buildinfo
LDFLAGS := -s -w -X $(PKG).Version=$(VERSION) -X $(PKG).Commit=$(COMMIT)
FUZZTIME ?= 30s

.PHONY: build run fmt lint test test-short fuzz bench docker compose-up compose-down clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/datafeed ./cmd/datafeed

run:
	go run ./cmd/datafeed serve

fmt:
	gofmt -w cmd internal

lint:
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...

test:
	go test -race ./...

test-short:
	go test -short ./...

# Runs each fuzz target for FUZZTIME; there are no fuzz targets until Phase 3.
fuzz:
	@for pkg in $$(go list ./...); do \
		for target in $$(go test -list '^Fuzz' $$pkg | grep '^Fuzz'); do \
			go test -run '^$$' -fuzz "^$$target$$" -fuzztime $(FUZZTIME) $$pkg || exit 1; \
		done; \
	done

bench:
	go test -run '^$$' -bench . -benchmem -benchtime 1s ./...

docker:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t datafeed:$(VERSION) .

compose-up:
	docker compose --profile workers up --build

compose-down:
	docker compose --profile workers down

clean:
	rm -rf bin
