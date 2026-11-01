-- Feeds: frozen specifications that clients read from over the API.

CREATE TABLE feeds (
  id             TEXT PRIMARY KEY,
  name           TEXT NOT NULL,
  description    TEXT NOT NULL DEFAULT '',
  strategy       TEXT NOT NULL,
  format         TEXT NOT NULL,
  snapshot_hash  TEXT NOT NULL REFERENCES snapshots(hash),
  seed           INTEGER NOT NULL,
  reference_time TEXT NOT NULL,
  engine_version INTEGER NOT NULL,
  rate           REAL NOT NULL DEFAULT 0,
  streams_json   TEXT NOT NULL,
  created_at     TEXT NOT NULL,
  created_by     TEXT NOT NULL DEFAULT ''
) WITHOUT ROWID;
