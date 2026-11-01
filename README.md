# datafeed

**Generate realistic synthetic data at scale, and get exactly the same bytes every time you ask.**

datafeed is a self-contained synthetic data platform. One program generates data on your laptop, serves it to your applications
over HTTP, or runs across many hosts. It needs no external database, message broker, cache or object store.

Use it to fill test databases, load-test pipelines, seed demo environments, or feed events into a system under test, without touching
production data.

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

## Get started

This takes about five minutes. There are no prebuilt downloads yet, so you build datafeed from source.

### 1. Install Go and Git

You need [Go 1.26 or newer](https://go.dev/dl/) and [Git](https://git-scm.com/downloads). No C compiler is required.

| System | Install Go with |
|---|---|
| Windows | `winget install GoLang.Go`, then open a new terminal |
| macOS | `brew install go` |
| Linux | The archive from [go.dev/dl](https://go.dev/dl/), as distribution packages are often too old |

Check it worked:

```bash
go version
```

### 2. Install datafeed

```bash
git clone https://github.com/genadigeno/data-feed.git
```

```bash
cd data-feed
```

```bash
go install ./cmd/datafeed
```

**Open a new terminal**, go back to the `data-feed` folder, and check:

```bash
datafeed version
```

If you see "not recognized" or "command not found", Go's install folder isn't on your `PATH`. See [Troubleshooting](#troubleshooting).

### 3. Generate your first data

This needs no server. It prints ten customers using a schema from the [examples](examples) folder:

```bash
datafeed generate --schema examples/customer.yaml --count 10
```

### 4. Start the server

Most other features need the server running. **Open a separate terminal for it**, go to the `data-feed` folder, and leave it running:

```bash
datafeed serve
```

It's ready when it prints `coordinator ready`. Open <http://127.0.0.1:8080> for the web interface. Ctrl+C stops the server. Your data is kept in a
`data` folder inside the folder you started it from, so always start it from the same place.

### 5. Serve data to an application

Back in your first terminal, create a feed:

```bash
datafeed feed create --schema examples/customer.yaml --seed 42 --name customers
```

The output shows an `ID` starting with `fed_`. Copy it, and use it in place of `<feed-id>` to fetch five records:

```bash
curl -i "http://127.0.0.1:8080/api/v1/feeds/<feed-id>/records?limit=5"
```

Anything written in `<angle brackets>` in this guide is a value for you to replace, including the brackets. You're now serving data
over HTTP. In PowerShell, write `curl.exe` instead of `curl`.

## How it fits together

| Word | What it is |
|---|---|
| **Schema** | Describes your data's fields. Either a file such as `examples/customer.yaml`, or saved on the server with an ID starting `sch_`. Anywhere that takes `--schema` accepts both |
| **Session** | One tracked generation run on the server, with progress and downloadable output. ID starts `ses_` |
| **Feed** | A frozen schema and seed that applications read from over HTTP. Nothing is stored. ID starts `fed_` |
| **Coordinator** | The server, started with `datafeed serve`. It includes a Worker, so one machine needs nothing else |
| **Worker** | A process that runs sessions. Only needed to spread the work over more machines |

Which features need the server?

| You want to | Use | Server needed? |
|---|---|---|
| Write data to a file or the terminal | `datafeed generate` | No |
| Run and track one job without a server | `datafeed --local session run` | No, and it can't run alongside a server using the same `data` folder |
| Use sessions, the web interface or the API | `datafeed session`, the browser, `curl` | Yes |
| Let applications fetch data | `datafeed feed` | Yes |

Feeds are managed from the CLI or API: the web interface has no feeds page yet, though a schema saved there can be used by its ID.

## Common scenarios

Run these from the `data-feed` folder, so that `examples/` can be found.

### Write data to a file

```bash
datafeed generate --schema examples/customer.yaml --count 1000000 --format csv -O customers.csv --workers 4
```

To write your own schema, copy `examples/customer.yaml` and change the fields.

### Reproduce a dataset exactly

Fix the seed and the reference time, and the output is identical on every run:

```bash
datafeed generate --schema examples/customer.yaml --count 100000 --seed 42 --reference-time 2026-01-01T00:00:00Z -O customers.ndjson
```

### Generate related tables

In `examples/shop.yaml`, every order points at a real customer. Each table gets its own files, so the output must be a directory:

```bash
datafeed generate --schema examples/shop.yaml --format csv -O shop/
```

### Write events at a steady rate

```bash
datafeed generate --schema examples/customer.yaml --mode stream --rate 1000 --duration 30s --format ndjson -O events/
```

Leave out `-O` to send them to another program through a pipe. Ctrl+C stops cleanly and leaves a complete, valid document.

### Split and compress large output

```bash
datafeed generate --schema examples/customer.yaml --count 10000000 --part-max-bytes 64MiB --compression gzip -O big/
```

### Feed data to your applications

*Needs the server running.* A feed freezes a schema, seed and format, so applications get the same data whenever they ask. Each read generates what it
asks for, so nothing is stored.

```bash
datafeed feed create --schema examples/customer.yaml --seed 42 --name customers
```

**Fetch a page.** Each page is a complete document. The `X-Next-Offset` response header says where the next one starts, and is absent at the end:

```bash
curl -i "http://127.0.0.1:8080/api/v1/feeds/<feed-id>/records?offset=0&limit=1000"
```

**Or stream continuously**, at the rate you choose:

```bash
curl -N "http://127.0.0.1:8080/api/v1/feeds/<feed-id>/stream?from=0&rate=1000"
```

The stream runs until the client disconnects, or until the feed ends if you created it with `--count`. A slow client slows generation down. If a
connection drops, reconnect with `from=` set to the number of records received so far: nothing is lost or repeated.

The same from the CLI:

```bash
datafeed feed read <feed-id> --limit 5
```

```bash
datafeed feed stream <feed-id> --rate 10
```

Streaming needs a feed in `ndjson` or `csv`. In a relational feed, choose a table with `&table=<name>`. Tables with gaps between positions, such as child
rows, can be read by page but not streamed. Feeds run on the Coordinator rather than Workers, and at most 8 streams can be open at once by default
(`api.feed_max_streams`). `datafeed feed --help` lists the other commands.

### Track a run as a session

*Needs the server running.* Sessions keep their history, and can be paused, resumed, retried and downloaded later:

```bash
datafeed session create --schema examples/customer.yaml --count 1000000 --start --name customers
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

To run one session without a server, stop any running `datafeed serve` in this folder first. The output stays in `./data`:

```bash
datafeed --local session run --schema examples/shop.yaml --format csv
```

### Spread work across hosts

Skip this on a single machine: `datafeed serve` already includes a Worker. Workers on other hosts connect over TLS. On the Coordinator host:

```bash
datafeed serve --worker-listen 0.0.0.0:8081
```

It logs its certificate fingerprint when it starts. In another terminal there, create a join token and save it to a file:

```bash
datafeed worker token create --ttl 24h > join-token.txt
```

In Windows PowerShell, `>` changes the file's encoding and the token is then refused, so use `| Out-File join-token.txt -Encoding ascii` instead.

Copy that file to each Worker host, then start the Worker with the fingerprint:

```bash
datafeed worker run --coordinator https://<coordinator-host>:8081 --coordinator-fingerprint sha256:<fingerprint> --join-token file:join-token.txt
```

The Worker keeps its own credential after that, so the token is only needed the first time.

### Create a session through the HTTP API

*Needs the server running.* This uses Linux or macOS quoting. On Windows, put the JSON in a file and pass it with `-d "@session.json"`:

```bash
curl -X POST "http://127.0.0.1:8080/api/v1/sessions?start=true" -H "Content-Type: application/json" -d '{"name": "demo", "mode": "batch", "seed": 1, "schema": {"inline": "kind: hybrid\nfields:\n  id: {type: uuid}\n"}, "limits": {"count": 1000}, "output": {"format": "ndjson"}}'
```

```bash
curl http://127.0.0.1:8080/api/v1/sessions
```

The full contract is served at `/api/v1/openapi.yaml`.

### Run with Docker

```bash
docker compose up --build
```

This starts a Coordinator at <http://127.0.0.1:8080>, with no authentication, for local use only. Add Workers with:

```bash
docker compose --profile workers up --build --scale worker=3
```

## Troubleshooting

| What you see | What it means | What to do |
|---|---|---|
| `'datafeed' is not recognized` or `command not found` | It isn't installed, or Go's install folder isn't on your `PATH` | Run `go install ./cmd/datafeed` in the `data-feed` folder and open a new terminal. If it still fails, see below |
| `cannot reach the server at http://127.0.0.1:8080` | The server isn't running | Start `datafeed serve` in another terminal, and leave it running |
| `no file customer.yaml in this folder` | `--schema` found no such file, and it isn't a saved schema's ID | Check the path, such as `examples/customer.yaml`, and which folder you're in |
| `no saved schema sch_…` | The ID doesn't match a schema on this server | Copy it from the web interface or `datafeed schema list` |
| `cannot read the schema file` | `generate` couldn't find the file | The same: check the path and folder |
| `bind: address already in use`, or `Only one usage of each socket address` | Something else is using port 8080 or 8081, usually a server you already started | Stop it, or use it rather than starting another |
| `another Coordinator already owns data directory` | A server is already using this `data` folder | Stop it, or use it without `--local` |
| Your sessions or feeds have disappeared | The server was started from a different folder, so it has a different `data` folder | Start it from the original folder, or set `DATAFEED_DATA_DIR` |
| `a valid API token is required` | The server has authentication on | Set `DATAFEED_TOKEN`, or pass `--token` |

**To add Go's install folder to your `PATH`:** find it with `go env GOPATH`, and add its `bin` folder. For this terminal only:

| Shell | Command |
|---|---|
| Windows Command Prompt | `set PATH=%PATH%;%USERPROFILE%\go\bin` |
| PowerShell | `$env:Path += ";$env:USERPROFILE\go\bin"` |
| Linux or macOS | `export PATH="$PATH:$(go env GOPATH)/bin"` |

To keep it, on Windows search for "Edit environment variables for your account" and add `%USERPROFILE%\go\bin` to `Path`. On Linux or macOS, add the
`export` line to `~/.bashrc` or `~/.zshrc`.

**Other useful commands:**

- `datafeed <command> --help` describes every command and flag.
- `datafeed health --ready` checks that the server is up.
- `datafeed config validate` reports every configuration problem at once.

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

[Open an issue](https://github.com/genadigeno/data-feed/issues) for bugs and questions.

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
