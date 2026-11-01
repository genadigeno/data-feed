package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/genadigeno/data-feed/internal/store"
)

// Store returns the persistence port over this database.
func (db *DB) Store() store.DB { return storeAdapter{db} }

type storeAdapter struct{ db *DB }

func (a storeAdapter) Write(ctx context.Context, fn func(context.Context, store.Tx) error) error {
	return a.db.Write(ctx, func(ctx context.Context, q Querier) error { return fn(ctx, &repo{q}) })
}

func (a storeAdapter) Read(ctx context.Context, fn func(context.Context, store.Tx) error) error {
	return a.db.Read(ctx, func(ctx context.Context, q Querier) error { return fn(ctx, &repo{q}) })
}

type repo struct{ q Querier }

// Times are stored as fixed-width UTC text, which sorts correctly as text.
const timeLayout = "2006-01-02T15:04:05.000Z"

func ts(t time.Time) string { return t.UTC().Format(timeLayout) }

func tsZero(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return ts(t)
}

func tsPtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return ts(*t)
}

func parseTime(s string) time.Time {
	t, err := time.Parse(timeLayout, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func nullTime(s sql.NullString) *time.Time {
	if !s.Valid {
		return nil
	}
	t := parseTime(s.String)
	return &t
}

func nullText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func notFound(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return store.ErrNotFound
	}
	return err
}

func placeholders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

func (r *repo) exec(ctx context.Context, query string, args ...any) error {
	_, err := r.q.ExecContext(ctx, query, args...)
	return err
}

func collect[T any](rows *sql.Rows, scan func(*sql.Rows) (T, error)) ([]T, error) {
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// Snapshots

func (r *repo) PutSnapshot(ctx context.Context, hash string, engine int, body []byte, at time.Time) error {
	return r.exec(ctx, `INSERT OR IGNORE INTO snapshots (hash, engine_version, body, created_at) VALUES (?,?,?,?)`, hash, engine, body, ts(at))
}

func (r *repo) GetSnapshot(ctx context.Context, hash string) ([]byte, error) {
	var body []byte
	err := r.q.QueryRowContext(ctx, `SELECT body FROM snapshots WHERE hash = ?`, hash).Scan(&body)
	return body, notFound(err)
}

func (r *repo) DeleteUnreferencedSnapshots(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := r.q.ExecContext(ctx, `DELETE FROM snapshots WHERE created_at < ? AND hash NOT IN (SELECT snapshot_hash FROM sessions)`, ts(olderThan))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Sessions

const sessionColumns = `id, name, description, strategy, mode, format, config_json, snapshot_hash, catalog_id, catalog_version,
 seed, reference_time, engine_version, state, state_reason, version, limits_json, created_at, queued_at, started_at, ended_at,
 paused_at, paused_total_ms, records, bytes, peak_rate, error_count, last_error, last_activity_at, created_by, retry_of,
 pinned, purging, artifacts_degraded, stop_immediate, finishing, worker_arch, rate, max_workers`

type scanner interface{ Scan(...any) error }

func scanSession(row scanner) (store.Session, error) {
	var s store.Session
	var catalogID, stateReason, lastError, createdBy, retryOf sql.NullString
	var catalogVersion sql.NullInt64
	var refTime, createdAt string
	var queuedAt, startedAt, endedAt, pausedAt, lastActivity sql.NullString
	var seed, records int64
	var pausedMS int64
	err := row.Scan(&s.ID, &s.Name, &s.Description, &s.Strategy, &s.Mode, &s.Format, &s.SpecJSON, &s.SnapshotHash, &catalogID, &catalogVersion,
		&seed, &refTime, &s.EngineVersion, &s.State, &stateReason, &s.Version, &s.LimitsJSON, &createdAt, &queuedAt, &startedAt, &endedAt,
		&pausedAt, &pausedMS, &records, &s.Bytes, &s.PeakRate, &s.ErrorCount, &lastError, &lastActivity, &createdBy, &retryOf,
		&s.Pinned, &s.Purging, &s.ArtifactsDegraded, &s.StopImmediate, &s.Finishing, &s.WorkerArch, &s.Rate, &s.MaxWorkers)
	if err != nil {
		return s, err
	}
	s.CatalogID, s.CatalogVersion = catalogID.String, int(catalogVersion.Int64)
	s.Seed = uint64(seed)
	s.ReferenceTime, s.CreatedAt = parseTime(refTime), parseTime(createdAt)
	s.StateReason, s.LastError, s.CreatedBy, s.RetryOf = stateReason.String, lastError.String, createdBy.String, retryOf.String
	s.QueuedAt, s.StartedAt, s.EndedAt, s.PausedAt, s.LastActivityAt = nullTime(queuedAt), nullTime(startedAt), nullTime(endedAt), nullTime(pausedAt), nullTime(lastActivity)
	s.PausedTotal = time.Duration(pausedMS) * time.Millisecond
	s.Records = uint64(records)
	return s, nil
}

func (r *repo) InsertSession(ctx context.Context, s *store.Session) error {
	return r.exec(ctx, `INSERT INTO sessions (`+sessionColumns+`) VALUES (`+placeholders(39)+`)`,
		s.ID, s.Name, s.Description, s.Strategy, s.Mode, s.Format, s.SpecJSON, s.SnapshotHash, nullText(s.CatalogID), s.CatalogVersion,
		int64(s.Seed), ts(s.ReferenceTime), s.EngineVersion, s.State, nullText(s.StateReason), s.Version, s.LimitsJSON, ts(s.CreatedAt),
		tsPtr(s.QueuedAt), tsPtr(s.StartedAt), tsPtr(s.EndedAt), tsPtr(s.PausedAt), s.PausedTotal.Milliseconds(), int64(s.Records), s.Bytes,
		s.PeakRate, s.ErrorCount, nullText(s.LastError), tsPtr(s.LastActivityAt), nullText(s.CreatedBy), nullText(s.RetryOf),
		boolInt(s.Pinned), boolInt(s.Purging), boolInt(s.ArtifactsDegraded), boolInt(s.StopImmediate), boolInt(s.Finishing), s.WorkerArch, s.Rate, s.MaxWorkers)
}

func (r *repo) GetSession(ctx context.Context, id string) (*store.Session, error) {
	s, err := scanSession(r.q.QueryRowContext(ctx, `SELECT `+sessionColumns+` FROM sessions WHERE id = ?`, id))
	if err != nil {
		return nil, notFound(err)
	}
	return &s, nil
}

// UpdateSession writes every mutable column if the row is still at expectVersion, and
// advances the version. Losing the compare-and-set returns store.ErrConflict.
func (r *repo) UpdateSession(ctx context.Context, s *store.Session, expect int64) error {
	res, err := r.q.ExecContext(ctx, `UPDATE sessions SET name=?, description=?, config_json=?, snapshot_hash=?, strategy=?, mode=?, format=?,
  catalog_id=?, catalog_version=?, seed=?, reference_time=?, state=?, state_reason=?, version=version+1, limits_json=?, queued_at=?, started_at=?, ended_at=?,
  paused_at=?, paused_total_ms=?, records=?, bytes=?, peak_rate=?, error_count=?, last_error=?, last_activity_at=?, pinned=?, purging=?,
  artifacts_degraded=?, stop_immediate=?, finishing=?, worker_arch=?, rate=?, max_workers=? WHERE id=? AND version=?`,
		s.Name, s.Description, s.SpecJSON, s.SnapshotHash, s.Strategy, s.Mode, s.Format, nullText(s.CatalogID), s.CatalogVersion, int64(s.Seed), ts(s.ReferenceTime),
		s.State, nullText(s.StateReason), s.LimitsJSON, tsPtr(s.QueuedAt), tsPtr(s.StartedAt), tsPtr(s.EndedAt), tsPtr(s.PausedAt), s.PausedTotal.Milliseconds(),
		int64(s.Records), s.Bytes, s.PeakRate, s.ErrorCount, nullText(s.LastError), tsPtr(s.LastActivityAt), boolInt(s.Pinned), boolInt(s.Purging),
		boolInt(s.ArtifactsDegraded), boolInt(s.StopImmediate), boolInt(s.Finishing), s.WorkerArch, s.Rate, s.MaxWorkers, s.ID, expect)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return store.ErrConflict
	}
	s.Version = expect + 1
	return nil
}

func (r *repo) ListSessions(ctx context.Context, f store.SessionFilter) ([]store.Session, error) {
	var where []string
	var args []any
	if len(f.States) > 0 {
		where = append(where, "state IN ("+placeholders(len(f.States))+")")
		for _, s := range f.States {
			args = append(args, s)
		}
	}
	if f.Mode != "" {
		where = append(where, "mode = ?")
		args = append(args, f.Mode)
	}
	if f.Query != "" {
		where = append(where, "name LIKE ? ESCAPE '\\'")
		esc := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(f.Query)
		args = append(args, "%"+esc+"%")
	}
	// IDs are time-ordered, so they double as the creation order and a stable cursor.
	order := "id DESC"
	cmp := "id < ?"
	if f.Sort == "created_at" {
		order, cmp = "id ASC", "id > ?"
	}
	if f.After != "" {
		where = append(where, cmp)
		args = append(args, f.After)
	}
	query := `SELECT ` + sessionColumns + ` FROM sessions`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY " + order + " LIMIT ?"
	args = append(args, f.Limit)
	rows, err := r.q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.Session, error) { return scanSession(rows) })
}

func (r *repo) SessionsInStates(ctx context.Context, states ...string) ([]store.Session, error) {
	args := make([]any, len(states))
	for i, s := range states {
		args[i] = s
	}
	rows, err := r.q.QueryContext(ctx, `SELECT `+sessionColumns+` FROM sessions WHERE state IN (`+placeholders(len(states))+`) ORDER BY id`, args...)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.Session, error) { return scanSession(rows) })
}

func (r *repo) CountSessionsByState(ctx context.Context) (map[string]int64, error) {
	return r.counts(ctx, `SELECT state, COUNT(*) FROM sessions GROUP BY state`)
}

func (r *repo) counts(ctx context.Context, query string, args ...any) (map[string]int64, error) {
	rows, err := r.q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var k string
		var n int64
		if err := rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, rows.Err()
}

func (r *repo) SumCommitted(ctx context.Context) (uint64, int64, error) {
	var records, bytes int64
	err := r.q.QueryRowContext(ctx, `SELECT COALESCE(SUM(records),0), COALESCE(SUM(bytes),0) FROM sessions`).Scan(&records, &bytes)
	return uint64(records), bytes, err
}

func (r *repo) DeleteSession(ctx context.Context, id string) error {
	return r.exec(ctx, `DELETE FROM sessions WHERE id = ?`, id)
}

func (r *repo) InsertEvent(ctx context.Context, e *store.Event) error {
	return r.exec(ctx, `INSERT INTO session_events (session_id, at, from_state, to_state, event, actor, reason) VALUES (?,?,?,?,?,?,?)`,
		e.SessionID, ts(e.At), nullText(e.From), e.To, e.Event, nullText(e.Actor), nullText(e.Reason))
}

func (r *repo) ListEvents(ctx context.Context, session string, limit int) ([]store.Event, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT id, session_id, at, from_state, to_state, event, actor, reason FROM session_events WHERE session_id = ? ORDER BY id LIMIT ?`, session, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.Event, error) {
		var e store.Event
		var at string
		var from, actor, reason sql.NullString
		err := rows.Scan(&e.ID, &e.SessionID, &at, &from, &e.To, &e.Event, &actor, &reason)
		e.At, e.From, e.Actor, e.Reason = parseTime(at), from.String, actor.String, reason.String
		return e, err
	})
}

func (r *repo) PutSample(ctx context.Context, s store.Sample) error {
	return r.exec(ctx, `INSERT INTO session_samples (session_id, bucket_start, records, bytes) VALUES (?,?,?,?)
  ON CONFLICT (session_id, bucket_start) DO UPDATE SET records = records + excluded.records, bytes = bytes + excluded.bytes`,
		s.SessionID, ts(s.Bucket), int64(s.Records), s.Bytes)
}

func (r *repo) ListSamples(ctx context.Context, session string, since time.Time) ([]store.Sample, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT bucket_start, records, bytes FROM session_samples WHERE session_id = ? AND bucket_start >= ? ORDER BY bucket_start`, session, ts(since))
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.Sample, error) {
		s := store.Sample{SessionID: session}
		var at string
		var records int64
		err := rows.Scan(&at, &records, &s.Bytes)
		s.Bucket, s.Records = parseTime(at), uint64(records)
		return s, err
	})
}

func (r *repo) TrimSamples(ctx context.Context, session string, keep int) error {
	return r.exec(ctx, `DELETE FROM session_samples WHERE session_id = ? AND bucket_start NOT IN
  (SELECT bucket_start FROM session_samples WHERE session_id = ? ORDER BY bucket_start DESC LIMIT ?)`, session, session, keep)
}

// Streams and units

func (r *repo) InsertStreamState(ctx context.Context, s *store.StreamState) error {
	return r.exec(ctx, `INSERT OR IGNORE INTO session_streams (session_id, stream, position, end_index, per_parent, expected_rows, max_record_bytes, next_index, next_seq, exhausted)
  VALUES (?,?,?,?,?,?,?,?,?,?)`, s.SessionID, s.Stream, s.Position, int64(s.End), int64(s.PerParent), s.ExpectedRows, s.MaxRecordBytes, int64(s.NextIndex), int64(s.NextSeq), boolInt(s.Exhausted))
}

func (r *repo) UpdateStreamState(ctx context.Context, s *store.StreamState) error {
	return r.exec(ctx, `UPDATE session_streams SET next_index=?, next_seq=?, exhausted=? WHERE session_id=? AND stream=?`,
		int64(s.NextIndex), int64(s.NextSeq), boolInt(s.Exhausted), s.SessionID, s.Stream)
}

func (r *repo) StreamStates(ctx context.Context, session string) ([]store.StreamState, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT session_id, stream, position, end_index, per_parent, expected_rows, max_record_bytes, next_index, next_seq, exhausted
  FROM session_streams WHERE session_id = ? ORDER BY position`, session)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.StreamState, error) {
		var s store.StreamState
		var end, per, next, seq int64
		err := rows.Scan(&s.SessionID, &s.Stream, &s.Position, &end, &per, &s.ExpectedRows, &s.MaxRecordBytes, &next, &seq, &s.Exhausted)
		s.End, s.PerParent, s.NextIndex, s.NextSeq = uint64(end), uint64(per), uint64(next), uint64(seq)
		return s, err
	})
}

const unitColumns = `id, session_id, stream, seq, range_start, range_end, committed_through, next_part_seq, state, fencing_token,
 lease_id, worker_id, lease_expires_at, granted_at, attempts, last_error, records, bytes`

func scanUnit(row scanner) (store.Unit, error) {
	var u store.Unit
	var seq, start, end, through, token, records int64
	var leaseID, workerID, expires, granted, lastError sql.NullString
	err := row.Scan(&u.ID, &u.SessionID, &u.Stream, &seq, &start, &end, &through, &u.NextPartSeq, &u.State, &token,
		&leaseID, &workerID, &expires, &granted, &u.Attempts, &lastError, &records, &u.Bytes)
	if err != nil {
		return u, err
	}
	u.Seq, u.RangeStart, u.RangeEnd, u.CommittedThrough, u.FencingToken, u.Records = uint64(seq), uint64(start), uint64(end), uint64(through), uint64(token), uint64(records)
	u.LeaseID, u.WorkerID, u.LastError = leaseID.String, workerID.String, lastError.String
	if expires.Valid {
		u.LeaseExpiresAt = parseTime(expires.String)
	}
	if granted.Valid {
		u.GrantedAt = parseTime(granted.String)
	}
	return u, nil
}

func (r *repo) InsertUnit(ctx context.Context, u *store.Unit) error {
	return r.exec(ctx, `INSERT INTO work_units (`+unitColumns+`) VALUES (`+placeholders(18)+`)`,
		u.ID, u.SessionID, u.Stream, int64(u.Seq), int64(u.RangeStart), int64(u.RangeEnd), int64(u.CommittedThrough), u.NextPartSeq, u.State, int64(u.FencingToken),
		nullText(u.LeaseID), nullText(u.WorkerID), tsZero(u.LeaseExpiresAt), tsZero(u.GrantedAt), u.Attempts, nullText(u.LastError), int64(u.Records), u.Bytes)
}

func (r *repo) GetUnit(ctx context.Context, id string) (*store.Unit, error) {
	u, err := scanUnit(r.q.QueryRowContext(ctx, `SELECT `+unitColumns+` FROM work_units WHERE id = ?`, id))
	if err != nil {
		return nil, notFound(err)
	}
	return &u, nil
}

func (r *repo) UpdateUnit(ctx context.Context, u *store.Unit) error {
	return r.exec(ctx, `UPDATE work_units SET range_end=?, committed_through=?, next_part_seq=?, state=?, fencing_token=?, lease_id=?, worker_id=?,
  lease_expires_at=?, granted_at=?, attempts=?, last_error=?, records=?, bytes=? WHERE id=?`,
		int64(u.RangeEnd), int64(u.CommittedThrough), u.NextPartSeq, u.State, int64(u.FencingToken), nullText(u.LeaseID), nullText(u.WorkerID),
		tsZero(u.LeaseExpiresAt), tsZero(u.GrantedAt), u.Attempts, nullText(u.LastError), int64(u.Records), u.Bytes, u.ID)
}

func (r *repo) NextPendingUnit(ctx context.Context, session string) (*store.Unit, error) {
	u, err := scanUnit(r.q.QueryRowContext(ctx, `SELECT `+unitColumns+` FROM work_units WHERE session_id = ? AND state = 'PENDING' ORDER BY stream, seq LIMIT 1`, session))
	if err != nil {
		return nil, notFound(err)
	}
	return &u, nil
}

func (r *repo) unitsWhere(ctx context.Context, where string, args ...any) ([]store.Unit, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT `+unitColumns+` FROM work_units WHERE `+where, args...)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.Unit, error) { return scanUnit(rows) })
}

func (r *repo) UnitsInState(ctx context.Context, session, state string) ([]store.Unit, error) {
	return r.unitsWhere(ctx, `session_id = ? AND state = ? ORDER BY stream, seq`, session, state)
}

func (r *repo) CountUnits(ctx context.Context, session string) (map[string]int64, error) {
	return r.counts(ctx, `SELECT state, COUNT(*) FROM work_units WHERE session_id = ? GROUP BY state`, session)
}

func (r *repo) LeasedUnits(ctx context.Context) ([]store.Unit, error) {
	return r.unitsWhere(ctx, `state = 'LEASED'`)
}

func (r *repo) LeasedByWorker(ctx context.Context, worker string) ([]store.Unit, error) {
	return r.unitsWhere(ctx, `state = 'LEASED' AND worker_id = ?`, worker)
}

// Parts

const partColumns = `unit_id, seq, session_id, stream, unit_seq, first_index, end_index, records, bytes, sha256, rel_path, header_len, footer_len, fencing_token, worker_id, committed_at`

func scanPart(row scanner) (store.Part, error) {
	var p store.Part
	var unitSeq, first, end, records, token int64
	var worker sql.NullString
	var at string
	var sum []byte
	err := row.Scan(&p.UnitID, &p.Seq, &p.SessionID, &p.Stream, &unitSeq, &first, &end, &records, &p.Bytes, &sum, &p.RelPath, &p.HeaderLen, &p.FooterLen, &token, &worker, &at)
	p.UnitSeq, p.FirstIndex, p.EndIndex, p.Records, p.FencingToken = uint64(unitSeq), uint64(first), uint64(end), uint64(records), uint64(token)
	p.SHA256, p.WorkerID, p.CommittedAt = string(sum), worker.String, parseTime(at)
	return p, err
}

func (r *repo) InsertPart(ctx context.Context, p *store.Part) error {
	return r.exec(ctx, `INSERT INTO parts (`+partColumns+`) VALUES (`+placeholders(16)+`)`,
		p.UnitID, p.Seq, p.SessionID, p.Stream, int64(p.UnitSeq), int64(p.FirstIndex), int64(p.EndIndex), int64(p.Records), p.Bytes, []byte(p.SHA256), p.RelPath,
		p.HeaderLen, p.FooterLen, int64(p.FencingToken), nullText(p.WorkerID), ts(p.CommittedAt))
}

func (r *repo) GetPart(ctx context.Context, unit string, seq int) (*store.Part, error) {
	p, err := scanPart(r.q.QueryRowContext(ctx, `SELECT `+partColumns+` FROM parts WHERE unit_id = ? AND seq = ?`, unit, seq))
	if err != nil {
		return nil, notFound(err)
	}
	return &p, nil
}

func (r *repo) partsWhere(ctx context.Context, where string, args ...any) ([]store.Part, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT `+partColumns+` FROM parts `+where, args...)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.Part, error) { return scanPart(rows) })
}

func (r *repo) SessionParts(ctx context.Context, session string) ([]store.Part, error) {
	return r.partsWhere(ctx, `WHERE session_id = ? ORDER BY stream, unit_seq, seq`, session)
}

func (r *repo) AllParts(ctx context.Context) ([]store.Part, error) {
	return r.partsWhere(ctx, ``)
}

func (r *repo) DeletePart(ctx context.Context, unit string, seq int) error {
	return r.exec(ctx, `DELETE FROM parts WHERE unit_id = ? AND seq = ?`, unit, seq)
}

// Workers

func (r *repo) PutWorker(ctx context.Context, w *store.Worker) error {
	credential := w.CredentialHash
	if credential == nil {
		credential = []byte{} // the embedded worker has none, and SQLite reads an empty blob back as NULL
	}
	return r.exec(ctx, `INSERT INTO workers (id, name, host, credential_hash, capabilities_json, slots, state, registered_at, last_heartbeat_at, last_error)
  VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (id) DO UPDATE SET name=excluded.name, host=excluded.host, credential_hash=excluded.credential_hash,
  capabilities_json=excluded.capabilities_json, slots=excluded.slots, state=excluded.state, last_heartbeat_at=excluded.last_heartbeat_at, last_error=excluded.last_error`,
		w.ID, w.Name, nullText(w.Host), credential, w.CapabilitiesJSON, w.Slots, w.State, ts(w.RegisteredAt), tsPtr(w.LastHeartbeatAt), nullText(w.LastError))
}

func scanWorker(row scanner) (store.Worker, error) {
	var w store.Worker
	var host, last, lastError sql.NullString
	var registered string
	err := row.Scan(&w.ID, &w.Name, &host, &w.CredentialHash, &w.CapabilitiesJSON, &w.Slots, &w.State, &registered, &last, &lastError)
	w.Host, w.LastError, w.RegisteredAt, w.LastHeartbeatAt = host.String, lastError.String, parseTime(registered), nullTime(last)
	return w, err
}

const workerColumns = `id, name, host, credential_hash, capabilities_json, slots, state, registered_at, last_heartbeat_at, last_error`

func (r *repo) GetWorker(ctx context.Context, id string) (*store.Worker, error) {
	w, err := scanWorker(r.q.QueryRowContext(ctx, `SELECT `+workerColumns+` FROM workers WHERE id = ?`, id))
	if err != nil {
		return nil, notFound(err)
	}
	return &w, nil
}

func (r *repo) ListWorkers(ctx context.Context) ([]store.Worker, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT `+workerColumns+` FROM workers ORDER BY name, id`)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.Worker, error) { return scanWorker(rows) })
}

func (r *repo) CountWorkers(ctx context.Context) (int64, error) {
	var n int64
	err := r.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM workers WHERE state != 'REVOKED'`).Scan(&n)
	return n, err
}

func (r *repo) DeleteWorker(ctx context.Context, id string) error {
	return r.exec(ctx, `DELETE FROM workers WHERE id = ?`, id)
}

func (r *repo) InsertJoinToken(ctx context.Context, t *store.JoinToken) error {
	return r.exec(ctx, `INSERT INTO join_tokens (id, hash, created_at, expires_at, max_uses, uses) VALUES (?,?,?,?,?,?)`,
		t.ID, t.Hash, ts(t.CreatedAt), tsPtr(t.ExpiresAt), t.MaxUses, t.Uses)
}

func (r *repo) FindJoinToken(ctx context.Context, hash []byte) (*store.JoinToken, error) {
	var t store.JoinToken
	var created string
	var expires sql.NullString
	var maxUses sql.NullInt64
	err := r.q.QueryRowContext(ctx, `SELECT id, hash, created_at, expires_at, max_uses, uses FROM join_tokens WHERE hash = ?`, hash).
		Scan(&t.ID, &t.Hash, &created, &expires, &maxUses, &t.Uses)
	if err != nil {
		return nil, notFound(err)
	}
	t.CreatedAt, t.ExpiresAt, t.MaxUses = parseTime(created), nullTime(expires), int(maxUses.Int64)
	return &t, nil
}

func (r *repo) UpdateJoinTokenUses(ctx context.Context, id string, uses int) error {
	return r.exec(ctx, `UPDATE join_tokens SET uses = ? WHERE id = ?`, uses, id)
}

// Schemas

func (r *repo) PutSchema(ctx context.Context, s *store.SchemaRow) error {
	return r.exec(ctx, `INSERT INTO schemas (id, version, name, kind, definition_json, created_at) VALUES (?,?,?,?,?,?)`,
		s.ID, s.Version, s.Name, s.Kind, s.Definition, ts(s.CreatedAt))
}

func scanSchema(row scanner) (store.SchemaRow, error) {
	var s store.SchemaRow
	var at string
	err := row.Scan(&s.ID, &s.Version, &s.Name, &s.Kind, &s.Definition, &at)
	s.CreatedAt = parseTime(at)
	return s, err
}

func (r *repo) GetSchema(ctx context.Context, id string, version int) (*store.SchemaRow, error) {
	query := `SELECT id, version, name, kind, definition_json, created_at FROM schemas WHERE id = ?`
	args := []any{id}
	if version > 0 {
		query += ` AND version = ?`
		args = append(args, version)
	}
	s, err := scanSchema(r.q.QueryRowContext(ctx, query+` ORDER BY version DESC LIMIT 1`, args...))
	if err != nil {
		return nil, notFound(err)
	}
	return &s, nil
}

func (r *repo) ListSchemas(ctx context.Context, after string, limit int) ([]store.SchemaRow, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT id, MAX(version), name, kind, definition_json, created_at FROM schemas WHERE id > ? GROUP BY id ORDER BY id LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.SchemaRow, error) { return scanSchema(rows) })
}

func (r *repo) DeleteSchema(ctx context.Context, id string) error {
	return r.exec(ctx, `DELETE FROM schemas WHERE id = ?`, id)
}

// Vocabulary

func (r *repo) PutDomain(ctx context.Context, d *store.Domain) error {
	return r.exec(ctx, `INSERT INTO domains (id, name, description, locale, created_at) VALUES (?,?,?,?,?)
  ON CONFLICT (id) DO UPDATE SET name=excluded.name, description=excluded.description, locale=excluded.locale`,
		d.ID, d.Name, d.Description, d.Locale, ts(d.CreatedAt))
}

func scanDomain(row scanner) (store.Domain, error) {
	var d store.Domain
	var at string
	err := row.Scan(&d.ID, &d.Name, &d.Description, &d.Locale, &at)
	d.CreatedAt = parseTime(at)
	return d, err
}

func (r *repo) GetDomain(ctx context.Context, id string) (*store.Domain, error) {
	d, err := scanDomain(r.q.QueryRowContext(ctx, `SELECT id, name, description, locale, created_at FROM domains WHERE id = ?`, id))
	if err != nil {
		return nil, notFound(err)
	}
	return &d, nil
}

func (r *repo) ListDomains(ctx context.Context, after string, limit int) ([]store.Domain, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT id, name, description, locale, created_at FROM domains WHERE id > ? ORDER BY id LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.Domain, error) { return scanDomain(rows) })
}

func (r *repo) DeleteDomain(ctx context.Context, id string) error {
	return r.exec(ctx, `DELETE FROM domains WHERE id = ?`, id)
}

func (r *repo) PutCatalog(ctx context.Context, c *store.Catalog) error {
	return r.exec(ctx, `INSERT INTO catalogs (id, version, domain_id, status, parent_version, content_hash, created_at, published_at) VALUES (?,?,?,?,?,?,?,?)
  ON CONFLICT (id, version) DO UPDATE SET status=excluded.status, content_hash=excluded.content_hash, published_at=excluded.published_at`,
		c.ID, c.Version, c.DomainID, c.Status, c.ParentVersion, nullText(c.ContentHash), ts(c.CreatedAt), tsPtr(c.PublishedAt))
}

const catalogColumns = `id, version, domain_id, status, parent_version, content_hash, created_at, published_at`

func scanCatalog(row scanner) (store.Catalog, error) {
	var c store.Catalog
	var parent sql.NullInt64
	var hash, published sql.NullString
	var created string
	err := row.Scan(&c.ID, &c.Version, &c.DomainID, &c.Status, &parent, &hash, &created, &published)
	c.ParentVersion, c.ContentHash, c.CreatedAt, c.PublishedAt = int(parent.Int64), hash.String, parseTime(created), nullTime(published)
	return c, err
}

func (r *repo) GetCatalog(ctx context.Context, id string, version int) (*store.Catalog, error) {
	query := `SELECT ` + catalogColumns + ` FROM catalogs WHERE id = ?`
	args := []any{id}
	if version > 0 {
		query += ` AND version = ?`
		args = append(args, version)
	}
	c, err := scanCatalog(r.q.QueryRowContext(ctx, query+` ORDER BY version DESC LIMIT 1`, args...))
	if err != nil {
		return nil, notFound(err)
	}
	return &c, nil
}

func (r *repo) LatestPublished(ctx context.Context, id string) (*store.Catalog, error) {
	c, err := scanCatalog(r.q.QueryRowContext(ctx, `SELECT `+catalogColumns+` FROM catalogs WHERE id = ? AND status = 'PUBLISHED' ORDER BY version DESC LIMIT 1`, id))
	if err != nil {
		return nil, notFound(err)
	}
	return &c, nil
}

func (r *repo) CatalogVersions(ctx context.Context, id string) ([]store.Catalog, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT `+catalogColumns+` FROM catalogs WHERE id = ? ORDER BY version`, id)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.Catalog, error) { return scanCatalog(rows) })
}

func (r *repo) ListCatalogs(ctx context.Context, domain, after string, limit int) ([]store.Catalog, error) {
	query := `SELECT id, MAX(version), domain_id, status, parent_version, content_hash, created_at, published_at FROM catalogs WHERE id > ?`
	args := []any{after}
	if domain != "" {
		query += ` AND domain_id = ?`
		args = append(args, domain)
	}
	rows, err := r.q.QueryContext(ctx, query+` GROUP BY id ORDER BY id LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.Catalog, error) { return scanCatalog(rows) })
}

func (r *repo) PutGroup(ctx context.Context, g *store.CatalogGroup) error {
	return r.exec(ctx, `INSERT INTO catalog_groups (catalog_id, version, key, name, description, data_type, category, locale, metadata_json) VALUES (?,?,?,?,?,?,?,?,?)
  ON CONFLICT (catalog_id, version, key) DO UPDATE SET name=excluded.name, description=excluded.description, data_type=excluded.data_type,
  category=excluded.category, locale=excluded.locale, metadata_json=excluded.metadata_json`,
		g.CatalogID, g.Version, g.Key, g.Name, nullText(g.Description), g.DataType, nullText(g.Category), nullText(g.Locale), nullText(g.MetadataJSON))
}

func (r *repo) DeleteGroup(ctx context.Context, catalog string, version int, key string) error {
	return r.exec(ctx, `DELETE FROM catalog_groups WHERE catalog_id = ? AND version = ? AND key = ?`, catalog, version, key)
}

func (r *repo) Groups(ctx context.Context, catalog string, version int) ([]store.CatalogGroup, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT catalog_id, version, key, name, description, data_type, category, locale, metadata_json
  FROM catalog_groups WHERE catalog_id = ? AND version = ? ORDER BY key`, catalog, version)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.CatalogGroup, error) {
		var g store.CatalogGroup
		var desc, category, locale, meta sql.NullString
		err := rows.Scan(&g.CatalogID, &g.Version, &g.Key, &g.Name, &desc, &g.DataType, &category, &locale, &meta)
		g.Description, g.Category, g.Locale, g.MetadataJSON = desc.String, category.String, locale.String, meta.String
		return g, err
	})
}

func (r *repo) PutValue(ctx context.Context, v *store.CatalogValue) error {
	return r.exec(ctx, `INSERT INTO catalog_values (catalog_id, version, group_key, value, weight, metadata_json) VALUES (?,?,?,?,?,?)
  ON CONFLICT (catalog_id, version, group_key, value) DO UPDATE SET weight=excluded.weight, metadata_json=excluded.metadata_json`,
		v.CatalogID, v.Version, v.GroupKey, v.Value, v.Weight, nullText(v.MetadataJSON))
}

func (r *repo) DeleteValue(ctx context.Context, catalog string, version int, group, value string) error {
	return r.exec(ctx, `DELETE FROM catalog_values WHERE catalog_id = ? AND version = ? AND group_key = ? AND value = ?`, catalog, version, group, value)
}

func (r *repo) Values(ctx context.Context, catalog string, version int, group string) ([]store.CatalogValue, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT catalog_id, version, group_key, value, weight, metadata_json FROM catalog_values
  WHERE catalog_id = ? AND version = ? AND group_key = ? ORDER BY value`, catalog, version, group)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.CatalogValue, error) {
		var v store.CatalogValue
		var meta sql.NullString
		err := rows.Scan(&v.CatalogID, &v.Version, &v.GroupKey, &v.Value, &v.Weight, &meta)
		v.MetadataJSON = meta.String
		return v, err
	})
}

func (r *repo) PutVocabJob(ctx context.Context, j *store.VocabJob) error {
	return r.exec(ctx, `INSERT INTO vocab_jobs (id, catalog_id, target_version, request_json, state, progress_json, usage_json, error, created_at, ended_at)
  VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (id) DO UPDATE SET state=excluded.state, progress_json=excluded.progress_json, usage_json=excluded.usage_json,
  error=excluded.error, ended_at=excluded.ended_at`,
		j.ID, j.CatalogID, j.TargetVersion, j.RequestJSON, j.State, j.ProgressJSON, nullText(j.UsageJSON), nullText(j.Error), ts(j.CreatedAt), tsPtr(j.EndedAt))
}

func scanVocabJob(row scanner) (store.VocabJob, error) {
	var j store.VocabJob
	var usage, jobErr, ended sql.NullString
	var created string
	err := row.Scan(&j.ID, &j.CatalogID, &j.TargetVersion, &j.RequestJSON, &j.State, &j.ProgressJSON, &usage, &jobErr, &created, &ended)
	j.UsageJSON, j.Error, j.CreatedAt, j.EndedAt = usage.String, jobErr.String, parseTime(created), nullTime(ended)
	return j, err
}

const vocabJobColumns = `id, catalog_id, target_version, request_json, state, progress_json, usage_json, error, created_at, ended_at`

func (r *repo) GetVocabJob(ctx context.Context, id string) (*store.VocabJob, error) {
	j, err := scanVocabJob(r.q.QueryRowContext(ctx, `SELECT `+vocabJobColumns+` FROM vocab_jobs WHERE id = ?`, id))
	if err != nil {
		return nil, notFound(err)
	}
	return &j, nil
}

func (r *repo) VocabJobsInState(ctx context.Context, state string) ([]store.VocabJob, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT `+vocabJobColumns+` FROM vocab_jobs WHERE state = ? ORDER BY id`, state)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.VocabJob, error) { return scanVocabJob(rows) })
}

func (r *repo) GetAICache(ctx context.Context, key string) (string, error) {
	var v string
	err := r.q.QueryRowContext(ctx, `SELECT response_json FROM ai_cache WHERE key = ?`, key).Scan(&v)
	return v, notFound(err)
}

func (r *repo) PutAICache(ctx context.Context, key, provider, model, response string, at time.Time) error {
	return r.exec(ctx, `INSERT OR REPLACE INTO ai_cache (key, provider, model, response_json, created_at) VALUES (?,?,?,?,?)`, key, provider, model, response, ts(at))
}

func (r *repo) DeleteAICacheBefore(ctx context.Context, before time.Time) error {
	return r.exec(ctx, `DELETE FROM ai_cache WHERE created_at < ?`, ts(before))
}

// API

func (r *repo) InsertAPIToken(ctx context.Context, t *store.APIToken) error {
	return r.exec(ctx, `INSERT INTO api_tokens (id, name, hash, role, created_at, expires_at, revoked_at) VALUES (?,?,?,?,?,?,?)`,
		t.ID, t.Name, t.Hash, t.Role, ts(t.CreatedAt), tsPtr(t.ExpiresAt), tsPtr(t.RevokedAt))
}

func scanToken(row scanner) (store.APIToken, error) {
	var t store.APIToken
	var created string
	var expires, revoked sql.NullString
	err := row.Scan(&t.ID, &t.Name, &t.Hash, &t.Role, &created, &expires, &revoked)
	t.CreatedAt, t.ExpiresAt, t.RevokedAt = parseTime(created), nullTime(expires), nullTime(revoked)
	return t, err
}

func (r *repo) FindAPIToken(ctx context.Context, hash []byte) (*store.APIToken, error) {
	t, err := scanToken(r.q.QueryRowContext(ctx, `SELECT id, name, hash, role, created_at, expires_at, revoked_at FROM api_tokens WHERE hash = ?`, hash))
	if err != nil {
		return nil, notFound(err)
	}
	return &t, nil
}

func (r *repo) ListAPITokens(ctx context.Context) ([]store.APIToken, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT id, name, hash, role, created_at, expires_at, revoked_at FROM api_tokens ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(rows *sql.Rows) (store.APIToken, error) { return scanToken(rows) })
}

func (r *repo) RevokeAPIToken(ctx context.Context, id string, at time.Time) error {
	return r.exec(ctx, `UPDATE api_tokens SET revoked_at = ? WHERE id = ?`, ts(at), id)
}

func (r *repo) CountAPITokens(ctx context.Context) (int64, error) {
	var n int64
	err := r.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_tokens WHERE revoked_at IS NULL`).Scan(&n)
	return n, err
}

func (r *repo) GetIdempotency(ctx context.Context, principal, key string) (*store.IdempotencyRecord, error) {
	rec := store.IdempotencyRecord{Principal: principal, Key: key}
	var at string
	err := r.q.QueryRowContext(ctx, `SELECT request_hash, status, response_json, created_at FROM idempotency_keys WHERE principal = ? AND key = ?`, principal, key).
		Scan(&rec.RequestHash, &rec.Status, &rec.ResponseJSON, &at)
	if err != nil {
		return nil, notFound(err)
	}
	rec.CreatedAt = parseTime(at)
	return &rec, nil
}

func (r *repo) PutIdempotency(ctx context.Context, rec *store.IdempotencyRecord) error {
	return r.exec(ctx, `INSERT OR REPLACE INTO idempotency_keys (principal, key, request_hash, status, response_json, created_at) VALUES (?,?,?,?,?,?)`,
		rec.Principal, rec.Key, rec.RequestHash, rec.Status, rec.ResponseJSON, ts(rec.CreatedAt))
}

func (r *repo) DeleteIdempotencyBefore(ctx context.Context, before time.Time) error {
	return r.exec(ctx, `DELETE FROM idempotency_keys WHERE created_at < ?`, ts(before))
}

var _ store.Tx = (*repo)(nil)
