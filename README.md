# data-feed

`datagen` is a self-contained synthetic data generation platform in Go. One binary runs as a Coordinator (API,
embedded SQLite, artifact storage), as a Worker on any host, or as a CLI. No external database, broker, cache or
object store is required.

**Status: under construction.** The foundation works: configuration, logging, SQLite with migrations, the
Coordinator and Worker processes, health checks and graceful shutdown. Data generation is not implemented yet.

## Quick start

Requires Go 1.26 or newer (`go` downloads the pinned toolchain itself).

```bash
go run ./cmd/datagen serve
```

In a second terminal:

```bash
go run ./cmd/datagen health --ready
```

```bash
go run ./cmd/datagen worker run --coordinator http://127.0.0.1:8081
```

Configuration options are described in [datagen.example.yaml](datagen.example.yaml); `datagen config show` lists
every setting with its current value and source.
