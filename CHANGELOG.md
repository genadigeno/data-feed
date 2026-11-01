# Changelog

Notable changes to datafeed. Versions follow [Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-11-01

The first stable release.

### Highlights

- **Generate data** as JSON, NDJSON, CSV or XML: schemaless random data, schema-driven records, or relational tables with working foreign keys.
- **Reproduce it exactly:** the same schema, seed and reference time give the same bytes, however many Workers ran it.
- **Serve it to your applications:** feeds let them read over HTTP, by page or as a live stream, from any position, with nothing stored.
- **Run it anywhere:** one binary with no external services, from a laptop up to a Coordinator with Workers on other hosts.
- **Track and recover:** sessions can be paused, resumed and retried, and survive Worker crashes and Coordinator restarts.
- **Use your interface of choice:** a CLI, a web interface and a versioned HTTP API.
- **Vocabulary catalogs:** built-in values, your own, or AI-generated drafts that you review before use.

### What stays stable in 1.x

- CLI commands and flags, the `/api/v1` HTTP API, and the session and schema documents. Additions are possible; breaking changes wait for 2.0.
- Output bytes for a given engine version, which `datafeed version` reports. A release that changes them raises the engine version and says so here.

### Known limits

- Performance was measured on one Windows machine, and failure testing ran with all processes on that machine. Running across real hosts is not yet proven, so try
  it on your own hosts before relying on it.
- Linux and macOS are covered by the automated tests. Linux has also had a manual run of the main commands, and macOS has not.
- Feeds have no page in the web interface yet, and run on the Coordinator rather than Workers.
- The real Anthropic API has not been exercised for AI-generated catalogs. Everything else works without it.
- Binaries are not code-signed, so Windows and macOS may warn when you first run one.

[1.0.0]: https://github.com/genadigeno/data-feed/releases/tag/v1.0.0
