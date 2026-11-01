# datafeed

**Generate realistic synthetic data at scale, and get exactly the same bytes every time you ask.**

datafeed is a self-contained synthetic data platform. One binary generates data on your laptop, or runs as a Coordinator
with Workers across many hosts. It needs no external database, message broker, cache or object store.

Use it to fill test databases, load-test pipelines, seed demo environments, or feed a stream of events into a system
under test, without touching production data.

## Features

- **Deterministic:** the same schema, seed and reference time reproduce a dataset byte for byte, however many Workers ran it.
- **Three strategies:** schemaless random data, schema-driven records, and relational tables with working foreign keys.
- **Four formats:** JSON, NDJSON, CSV and XML, with optional gzip and automatic part rotation.
- **Batch or stream:** a set quantity, or a steady rate until you stop it.
- **Resilient:** sessions survive Worker crashes and Coordinator restarts, and no record is lost or duplicated.
- **Three interfaces:** a CLI, a web interface and a versioned HTTP API.
- **Feeds for your applications:** read data over HTTP by page or as a live stream, from any position, with nothing stored.
- **Vocabulary catalogs:** built-in values, your own, or AI-generated drafts that you review before use (optional).
- **Bounded memory:** a slow disk or network slows generation down rather than filling memory.

## Installation

datafeed is built from source and has no runtime dependencies. You need [Go 1.26 or newer](https://go.dev/dl/); no C compiler is
required.

### Windows

In PowerShell:

```powershell
winget install GoLang.Go
```

Open a new terminal so `go` is on your `PATH`, then:

```powershell
go install github.com/genadigeno/data-feed/cmd/datafeed@latest
```

```powershell
datafeed version
```

### macOS

```bash
brew install go
```

```bash
go install github.com/genadigeno/data-feed/cmd/datafeed@latest
```

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

```bash
datafeed version
```

Add the `export` line to `~/.zshrc` to keep it.

### Linux

Distribution packages of Go are often too old, so install it from [go.dev/dl](https://go.dev/dl/). Then:

```bash
go install github.com/genadigeno/data-feed/cmd/datafeed@latest
```

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

```bash
datafeed version
```

Add the `export` line to `~/.bashrc` to keep it.

### From a clone

```bash
git clone https://github.com/genadigeno/data-feed.git
```

```bash
cd data-feed
```

```bash
go build -o datafeed ./cmd/datafeed
```

On Windows, use `-o datafeed.exe`. The result is a single file that you can copy to any machine with the same OS and CPU type. To
build for another platform, set `GOOS` and `GOARCH`: `linux`, `darwin` or `windows`, with `amd64` or `arm64`.

### Docker

```bash
docker compose up --build
```

This starts a Coordinator at <http://127.0.0.1:8080>. Add Workers with:

```bash
docker compose --profile workers up --build --scale worker=3
```

This setup has no authentication and is for local use only.

## Quick start

Generate ten records of random data:

```bash
datafeed generate --count 10 --format ndjson
```

To shape the data, write a schema. Save this as `customer.yaml`:

```yaml
kind: hybrid
fields:
  id: {type: uuid, unique: true}
  name: {type: full_name}
  email: {type: email}
  age: {type: integer, min: 18, max: 90}
  balance: {type: decimal, scale: 2, min: 0, max: 100000}
  active: {type: boolean}
  created_at: {type: timestamp}
```

Then generate a million rows as CSV:

```bash
datafeed generate --schema customer.yaml --count 1000000 --format csv -O customers.csv --workers 4
```

## Common scenarios

### Reproduce a dataset exactly

Fix the seed and the reference time, and the output is identical on every run:

```bash
datafeed generate --schema customer.yaml --count 100000 --seed 42 --reference-time 2026-01-01T00:00:00Z -O customers.ndjson
```

### Generate related tables

Save this as `shop.yaml`. Every order points at a real customer:

```yaml
kind: relational
tables:
  customers:
    count: 10000
    primary_key: id
    fields:
      id: {type: uuid}
      name: {type: full_name}
      email: {type: email}
  orders:
    primary_key: id
    owner: {ref: customers, per_parent: {min: 1, max: 3}}
    fields:
      id: {type: uuid}
      customer: {type: reference, ref: customers.id, owner: true}
      total: {type: decimal, scale: 2, min: 1, max: 900}
      placed_at: {type: timestamp}
```

Each table gets its own files, so the output must be a directory:

```bash
datafeed generate --schema shop.yaml --format csv -O shop/
```

### Stream events at a steady rate

```bash
datafeed generate --schema customer.yaml --mode stream --rate 1000 --duration 30s --format ndjson -O events/
```

Pipe to another program by leaving out `-O`. Ctrl+C stops cleanly and leaves a complete, valid document.

### Split and compress large output

```bash
datafeed generate --schema customer.yaml --count 10000000 --part-max-bytes 64MiB --compression gzip -O big/
```

### Track a run as a session

Sessions keep their history and can be paused, resumed, retried and downloaded later. `--local` runs a temporary Coordinator just for
that command, and the output stays in `./data`:

```bash
datafeed --local session run --schema shop.yaml --format csv
```

### Run the server and web interface

```bash
datafeed serve
```

Open <http://127.0.0.1:8080> for the web interface. In a second terminal, drive the same server from the CLI:

```bash
datafeed session create --schema customer.yaml --count 1000000 --start --name customers
```

```bash
datafeed session list
```

```bash
datafeed session watch <session-id>
```

```bash
datafeed session download <session-id> -O customers.ndjson
```

`serve` runs an embedded Worker, so this works with nothing else started. Set `DATAFEED_WORKER__LOCAL_SLOTS=0` to leave all the work to
separate Workers.

### Spread work across hosts

Workers on other hosts connect over TLS. On the Coordinator host:

```bash
datafeed serve --worker-listen 0.0.0.0:8081
```

The Coordinator logs its certificate fingerprint when it starts. Create a join token and save it to a file:

```bash
datafeed worker token create --ttl 24h > join-token.txt
```

In Windows PowerShell, `>` changes the file's encoding and the token won't be accepted, so pipe to `Out-File join-token.txt -Encoding ascii`
instead.

Copy that file to each Worker host, then start the Worker with the fingerprint:

```bash
datafeed worker run --coordinator https://coordinator-host:8081 --coordinator-fingerprint sha256:<fingerprint> --join-token file:join-token.txt
```

The Worker keeps its own credential after that, so the token is only needed the first time. A Worker on the Coordinator's own machine can
skip all of this while developing:

```bash
datafeed worker run --coordinator http://127.0.0.1:8081
```

### Use the HTTP API

```bash
curl -X POST "http://127.0.0.1:8080/api/v1/sessions?start=true" -H "Content-Type: application/json" -d '{"name": "demo", "mode": "batch", "seed": 1, "schema": {"inline": "kind: hybrid\nfields:\n  id: {type: uuid}\n"}, "limits": {"count": 1000}, "output": {"format": "ndjson"}}'
```

```bash
curl http://127.0.0.1:8080/api/v1/sessions
```

On Windows PowerShell, use `curl.exe` and pass the JSON from a file with `-d "@session.json"`.

### Feed data to your applications

A feed freezes a schema, seed and format, so your applications can read the same data over HTTP whenever they need it. Nothing is
stored: each read generates what it asks for. With `datafeed serve` running, create one:

```bash
datafeed feed create --schema customer.yaml --seed 42 --name customers
```

Read it a page at a time. Each page is a complete document, and the `X-Next-Offset` response header says where the next one starts:

```bash
curl -i "http://127.0.0.1:8080/api/v1/feeds/<feed-id>/records?offset=0&limit=1000"
```

Or stream it, at whatever rate you choose:

```bash
curl -N "http://127.0.0.1:8080/api/v1/feeds/<feed-id>/stream?from=0&rate=1000"
```

The stream goes on until the client disconnects, or until the feed ends if you gave it a `--count`. A slow client slows generation
down. If a connection drops, reconnect with `from=` set to the number of records received so far: nothing is lost or repeated, and every
client sees the same bytes at the same position.

Streaming needs a feed in `ndjson` or `csv`. A table in a relational feed that has gaps between positions, such as child rows, can be
read by page but not streamed. Feeds run on the Coordinator rather than Workers, and `api.feed_max_streams` limits how many streams can
be open at once. `datafeed feed --help` lists the other commands.

## Going to production

Copy [datafeed.example.yaml](datafeed.example.yaml) to `datafeed.yaml`, and at least set:

```yaml
application:
  environment: production
auth:
  mode: token
  bootstrap_admin_token: ${BOOTSTRAP_ADMIN_TOKEN}
```

Don't name your own variables with a `DATAFEED_` prefix: datafeed reads those as settings, and refuses ones it doesn't know.

Then check it, and create named tokens to replace the bootstrap one:

```bash
datafeed config validate
```

```bash
datafeed token create --role admin --name ops --ttl 720h
```

Off loopback, production also requires `server.tls`, or `server.behind_tls_proxy: true` if a proxy terminates TLS. Keep the data directory on
a local disk, as network filesystems are refused. Back up with `datafeed admin backup <file>`, then copy `<data_dir>/outputs`.

Settings can also come from environment variables: `worker.local_slots` becomes `DATAFEED_WORKER__LOCAL_SLOTS`. `datafeed config show`
lists every setting with its value and where it came from.

## Performance

Measured on one machine: a 6-core i7-8700 with one SSD, on Windows, using 10-field records of 279 bytes.

| Measure | Result |
|---|---|
| Generation, nothing written | about 1.5M records/s |
| `datafeed generate` to disk | about 600k records/s, 160 MiB/s |
| Session, embedded Worker | about 680k records/s, limited by the disk |
| Sustained 60,000 records/s for 8 minutes | Flat memory: Coordinator 28 MiB, Worker under 165 MiB |
| Restart after a kill, with 30,000 parts | Ready in about 1 s |

Your numbers will depend mostly on your disk. Reproduce the in-process benchmarks with `make bench`.

## Getting help

- `datafeed <command> --help` describes every command and flag.
- `datafeed config validate` reports every configuration problem at once.
- `datafeed health --ready` checks a running Coordinator.
- [Open an issue](https://github.com/genadigeno/data-feed/issues) for bugs and questions.

## Contributing

```bash
make fmt lint test
```

Tests must stay fast and need no external services. Changes must keep output byte-identical, or say clearly why they can't.

## Project status

datafeed is pre-1.0. It has been measured and failure-tested on Windows, and the usage commands in this file have been run on Windows and on
Ubuntu, all on one machine. It builds for macOS, but has not been run there. Running it across real hosts is not yet proven, so try it on
your own hosts before relying on it. There are no prebuilt releases yet.

## License

Licensed under the [Apache License 2.0](LICENSE).
