# Contributing to datafeed

Thanks for helping. Bug reports, questions, documentation fixes and code are all welcome.

- **Found a bug?** [Open an issue](https://github.com/genadigeno/data-feed/issues/new/choose).
- **Have a question or an idea?** Start a [discussion](https://github.com/genadigeno/data-feed/discussions).
- **Found a security problem?** See [SECURITY.md](SECURITY.md), and please don't open a public issue.
- **Planning a large change?** Open an issue first, so that no work is wasted.

## Set up

You need [Go 1.26 or newer](https://go.dev/dl/) and nothing else: no C compiler, database or other services.

```bash
git clone https://github.com/genadigeno/data-feed.git
```

```bash
cd data-feed
```

```bash
go run ./cmd/datafeed generate --schema examples/customer.yaml --count 3
```

## Before you open a pull request

```bash
make fmt lint test
```

Without `make`, on Windows for example, run these instead:

```bash
gofmt -w cmd internal examples
```

```bash
go vet ./...
```

```bash
go test -race ./...
```

CI runs the tests on Linux, Windows and macOS.

## What we look for

**Generated output must not change.** Reproducible bytes are the product's main promise: the same schema, seed and reference time must give the same
output. If a change has to alter output, it must raise `gen.Version` and say so clearly in the pull request.

**Tests stay fast and self-contained.** Write unit tests, property tests and short fuzz targets. Please don't add tests that need a database, an HTTP
server or other running services, or that take minutes. Check that behaviour by hand, and describe what you did in the pull request.

**Memory stays bounded.** Nothing may grow with the number of records. Use bounded channels, and let a slow destination slow generation down. Nothing may
run per record if it can run per batch.

**New dependencies need a reason.** datafeed is one binary with no external services, and it should stay that way.

**Interfaces share one set of rules.** The CLI, API and web interface call the same services, and handlers only parse, call and map the result. Package
boundaries are checked by `internal/archtest`. A new API route must also be added to `internal/api/openapi.yaml`, which a test checks.

**Code reads like the code around it.** Small commits with a clear message explaining why are easiest to review.

## Releasing

Maintainers add the version's section to [CHANGELOG.md](CHANGELOG.md), then push a tag such as `v1.2.0`. The release workflow does the rest.

## License

By contributing, you agree that your contribution is licensed under the [Apache License 2.0](LICENSE).
