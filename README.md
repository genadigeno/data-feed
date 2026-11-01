# data-feed

`datafeed` is a self-contained synthetic data generation platform in Go. One binary runs as a Coordinator (API,
embedded SQLite, artifact storage), as a Worker on any host, or as a CLI. No external database, broker, cache or
object store is required.

**Status: under construction.** `datafeed generate` produces data directly. The Coordinator and Workers run sessions
across hosts, surviving Worker and Coordinator restarts, but sessions cannot be created by users until the public API
arrives.

## Quick start

Requires Go 1.26 or newer (`go` downloads the pinned toolchain itself).

```bash
go run ./cmd/datafeed serve
```

In a second terminal:

```bash
go run ./cmd/datafeed health --ready
```

```bash
go run ./cmd/datafeed worker run --coordinator http://127.0.0.1:8081
```

Configuration options are described in [datafeed.example.yaml](datafeed.example.yaml); `datafeed config show` lists
every setting with its current value and source.
