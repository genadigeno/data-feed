-- Initial schema. See docs/01-architecture/sqlite-persistence.md.
-- Times are RFC 3339 UTC strings with millisecond precision (sortable as text).

CREATE TABLE snapshots (
  hash           TEXT PRIMARY KEY,
  engine_version INTEGER NOT NULL,
  body           BLOB NOT NULL,          -- gzip(canonical JSON)
  created_at     TEXT NOT NULL
);

CREATE TABLE sessions (
  id                 TEXT PRIMARY KEY,
  name               TEXT NOT NULL,
  description        TEXT NOT NULL DEFAULT '',
  strategy           TEXT NOT NULL,
  mode               TEXT NOT NULL,
  format             TEXT NOT NULL,
  config_json        TEXT NOT NULL,      -- specification as submitted, for clone and edit
  snapshot_hash      TEXT NOT NULL REFERENCES snapshots(hash),
  catalog_id         TEXT,
  catalog_version    INTEGER,
  seed               INTEGER NOT NULL,
  reference_time     TEXT NOT NULL,
  engine_version     INTEGER NOT NULL,
  state              TEXT NOT NULL,
  state_reason       TEXT,
  version            INTEGER NOT NULL DEFAULT 0,
  limits_json        TEXT NOT NULL,
  created_at         TEXT NOT NULL,
  queued_at          TEXT,
  started_at         TEXT,
  ended_at           TEXT,
  paused_at          TEXT,
  paused_total_ms    INTEGER NOT NULL DEFAULT 0,
  records            INTEGER NOT NULL DEFAULT 0,   -- committed
  bytes              INTEGER NOT NULL DEFAULT 0,   -- committed
  peak_rate          REAL NOT NULL DEFAULT 0,
  error_count        INTEGER NOT NULL DEFAULT 0,
  last_error         TEXT,
  last_activity_at   TEXT,
  created_by         TEXT,
  retry_of           TEXT REFERENCES sessions(id) ON DELETE SET NULL,
  pinned             INTEGER NOT NULL DEFAULT 0,
  purging            INTEGER NOT NULL DEFAULT 0,
  artifacts_degraded INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX sessions_state ON sessions(state, created_at);

CREATE TABLE session_events (
  id         INTEGER PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  at         TEXT NOT NULL,
  from_state TEXT,
  to_state   TEXT NOT NULL,
  event      TEXT NOT NULL,
  actor      TEXT,
  reason     TEXT
);
CREATE INDEX session_events_session ON session_events(session_id, id);

CREATE TABLE session_samples (
  session_id   TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  bucket_start TEXT NOT NULL,
  records      INTEGER NOT NULL,
  bytes        INTEGER NOT NULL,
  PRIMARY KEY (session_id, bucket_start)
) WITHOUT ROWID;

CREATE TABLE workers (
  id                TEXT PRIMARY KEY,
  name              TEXT NOT NULL,
  host              TEXT,
  credential_hash   BLOB NOT NULL,
  capabilities_json TEXT NOT NULL,
  slots             INTEGER NOT NULL,
  state             TEXT NOT NULL,        -- ACTIVE | DRAINING | DISCONNECTED | REVOKED
  registered_at     TEXT NOT NULL,
  last_heartbeat_at TEXT,
  last_error        TEXT
);

CREATE TABLE work_units (
  id                TEXT PRIMARY KEY,
  session_id        TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  stream            TEXT NOT NULL,
  seq               INTEGER NOT NULL,     -- order within the stream
  range_start       INTEGER NOT NULL,
  range_end         INTEGER NOT NULL,
  committed_through INTEGER NOT NULL,     -- range_start initially
  next_part_seq     INTEGER NOT NULL DEFAULT 0,
  state             TEXT NOT NULL,        -- PENDING | LEASED | COMMITTED | FAILED | CANCELLED
  fencing_token     INTEGER NOT NULL DEFAULT 0,
  lease_id          TEXT,
  worker_id         TEXT REFERENCES workers(id) ON DELETE SET NULL,
  lease_expires_at  TEXT,
  granted_at        TEXT,
  attempts          INTEGER NOT NULL DEFAULT 0,
  last_error        TEXT,
  records           INTEGER NOT NULL DEFAULT 0,
  bytes             INTEGER NOT NULL DEFAULT 0,
  UNIQUE (session_id, stream, seq)
);
CREATE INDEX work_units_dispatch ON work_units(state, session_id);
CREATE INDEX work_units_expiry ON work_units(state, lease_expires_at);
CREATE UNIQUE INDEX work_units_lease ON work_units(lease_id) WHERE lease_id IS NOT NULL;

CREATE TABLE parts (
  unit_id       TEXT NOT NULL REFERENCES work_units(id) ON DELETE CASCADE,
  seq           INTEGER NOT NULL,
  session_id    TEXT NOT NULL,
  stream        TEXT NOT NULL,
  first_index   INTEGER NOT NULL,
  end_index     INTEGER NOT NULL,
  records       INTEGER NOT NULL,
  bytes         INTEGER NOT NULL,
  sha256        BLOB NOT NULL,
  rel_path      TEXT NOT NULL UNIQUE,
  header_len    INTEGER NOT NULL,
  footer_len    INTEGER NOT NULL,
  fencing_token INTEGER NOT NULL,
  worker_id     TEXT,
  committed_at  TEXT NOT NULL,
  PRIMARY KEY (unit_id, seq)
) WITHOUT ROWID;
CREATE INDEX parts_session ON parts(session_id, stream);

CREATE TABLE schemas (
  id              TEXT NOT NULL,
  version         INTEGER NOT NULL,
  name            TEXT NOT NULL,
  kind            TEXT NOT NULL,
  definition_json TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  PRIMARY KEY (id, version)
);

CREATE TABLE domains (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL UNIQUE,
  description TEXT NOT NULL,
  locale      TEXT NOT NULL,
  created_at  TEXT NOT NULL
);

CREATE TABLE catalogs (
  id             TEXT NOT NULL,
  version        INTEGER NOT NULL,
  domain_id      TEXT NOT NULL REFERENCES domains(id),
  status         TEXT NOT NULL,           -- DRAFT | PUBLISHED
  parent_version INTEGER,
  content_hash   TEXT,
  created_at     TEXT NOT NULL,
  published_at   TEXT,
  PRIMARY KEY (id, version)
);

CREATE TABLE catalog_groups (
  catalog_id    TEXT NOT NULL,
  version       INTEGER NOT NULL,
  key           TEXT NOT NULL,
  name          TEXT NOT NULL,
  description   TEXT,
  data_type     TEXT NOT NULL,
  category      TEXT,
  locale        TEXT,
  metadata_json TEXT,
  PRIMARY KEY (catalog_id, version, key),
  FOREIGN KEY (catalog_id, version) REFERENCES catalogs(id, version) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE TABLE catalog_values (
  catalog_id    TEXT NOT NULL,
  version       INTEGER NOT NULL,
  group_key     TEXT NOT NULL,
  value         TEXT NOT NULL,
  weight        REAL NOT NULL DEFAULT 1,
  metadata_json TEXT,
  PRIMARY KEY (catalog_id, version, group_key, value),
  FOREIGN KEY (catalog_id, version, group_key) REFERENCES catalog_groups(catalog_id, version, key) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE TABLE vocab_jobs (
  id             TEXT PRIMARY KEY,
  catalog_id     TEXT NOT NULL,
  target_version INTEGER NOT NULL,
  request_json   TEXT NOT NULL,
  state          TEXT NOT NULL,
  progress_json  TEXT NOT NULL,
  usage_json     TEXT,
  error          TEXT,
  created_at     TEXT NOT NULL,
  ended_at       TEXT
);

CREATE TABLE ai_cache (
  key           TEXT PRIMARY KEY,
  provider      TEXT NOT NULL,
  model         TEXT NOT NULL,
  response_json TEXT NOT NULL,
  created_at    TEXT NOT NULL
);

CREATE TABLE api_tokens (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  hash       BLOB NOT NULL UNIQUE,
  role       TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT,
  revoked_at TEXT
);

CREATE TABLE idempotency_keys (
  principal     TEXT NOT NULL,
  key           TEXT NOT NULL,
  request_hash  BLOB NOT NULL,
  status        INTEGER NOT NULL,
  response_json TEXT NOT NULL,
  created_at    TEXT NOT NULL,
  PRIMARY KEY (principal, key)
) WITHOUT ROWID;

CREATE TABLE join_tokens (
  id         TEXT PRIMARY KEY,
  hash       BLOB NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  expires_at TEXT,
  max_uses   INTEGER,
  uses       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE settings (
  key        TEXT PRIMARY KEY,
  value_json TEXT NOT NULL
);
