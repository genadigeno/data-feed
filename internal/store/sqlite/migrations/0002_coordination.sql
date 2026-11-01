-- Stop handling, architecture pinning and lazy unit planning.

ALTER TABLE sessions ADD COLUMN stop_immediate INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN finishing INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN worker_arch TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN rate REAL NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN max_workers INTEGER NOT NULL DEFAULT 0;

ALTER TABLE parts ADD COLUMN unit_seq INTEGER NOT NULL DEFAULT 0;

CREATE TABLE session_streams (
  session_id       TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  stream           TEXT NOT NULL,
  position         INTEGER NOT NULL,
  end_index        INTEGER NOT NULL,
  per_parent       INTEGER NOT NULL,
  expected_rows    REAL NOT NULL,
  max_record_bytes INTEGER NOT NULL,
  next_index       INTEGER NOT NULL DEFAULT 0,
  next_seq         INTEGER NOT NULL DEFAULT 0,
  exhausted        INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (session_id, stream)
) WITHOUT ROWID;

CREATE INDEX work_units_session_state ON work_units(session_id, state, stream, seq);
CREATE INDEX work_units_worker ON work_units(worker_id) WHERE worker_id IS NOT NULL;
