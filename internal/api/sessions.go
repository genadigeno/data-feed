package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/artifact"
	"github.com/genadigeno/data-feed/internal/coord"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/obs"
	"github.com/genadigeno/data-feed/internal/output"
	"github.com/genadigeno/data-feed/internal/pipeline"
	"github.com/genadigeno/data-feed/internal/serial"
	"github.com/genadigeno/data-feed/internal/session"
	"github.com/genadigeno/data-feed/internal/store"
)

func notFound(what, id string) error {
	return apperr.New(apperr.NotFound, "ERR_NOT_FOUND", "no "+what+" "+id)
}

func badRequest(msg string) error { return apperr.New(apperr.InvalidArgument, "ERR_BAD_REQUEST", msg) }

func readJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return err
		}
		return badRequest("invalid request body: " + err.Error())
	}
	return nil
}

// readOptionalJSON reads a body that may be absent.
func readOptionalJSON(r *http.Request, v any) error {
	err := readJSON(r, v)
	var e *apperr.Error
	if errors.As(err, &e) && strings.HasSuffix(e.Message, "EOF") {
		return nil
	}
	return err
}

func (s *server) dto(row *store.Session) SessionDTO {
	return ToSessionDTO(row, s.o.Core.LiveStats(row.ID), s.o.Clock.Now())
}

func (s *server) detail(ctx context.Context, row *store.Session) (SessionDTO, error) {
	d := s.dto(row)
	p, err := s.o.Sessions.LoadProgress(ctx, row.ID)
	if err != nil {
		return d, err
	}
	WithProgress(&d, p, s.o.Core.LiveStats(row.ID), row.Records)
	return d, nil
}

func (s *server) createSession(w http.ResponseWriter, r *http.Request) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	row, err := s.o.Sessions.Create(r.Context(), body, principalOf(r).Name)
	if err != nil {
		return err
	}
	if r.URL.Query().Get("start") == "true" {
		if row, err = s.o.Sessions.Start(r.Context(), row.ID, principalOf(r).Name); err != nil {
			return err
		}
	}
	w.Header().Set("Location", prefix+"/sessions/"+row.ID)
	s.writeJSON(w, http.StatusCreated, s.dto(row))
	return nil
}

func (s *server) validateSession(w http.ResponseWriter, r *http.Request) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	c, err := s.o.Sessions.ValidateSpec(r.Context(), body)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"valid": true, "strategy": c.Strategy, "seed": u64(c.Snapshot.Seed), "unit_memory_bytes": i64(c.Limits.UnitMemoryBytes),
	})
	return nil
}

func (s *server) listSessions(w http.ResponseWriter, r *http.Request) error {
	f, err := SessionFilterFrom(r)
	if err != nil {
		return err
	}
	after, limit, err := s.page(r)
	if err != nil {
		return err
	}
	f.After, f.Limit = after, limit+1
	rows, err := s.o.Sessions.List(r.Context(), f)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, MakePage(rows, limit, func(row *store.Session) string { return row.ID }, s.dto))
	return nil
}

func (s *server) getSession(w http.ResponseWriter, r *http.Request) error {
	row, err := s.o.Sessions.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	d, err := s.detail(r.Context(), row)
	if err != nil {
		return err
	}
	if r.URL.Query().Get("include") == "spec" && json.Valid([]byte(row.SpecJSON)) {
		d.Spec = json.RawMessage(row.SpecJSON)
	}
	w.Header().Set("ETag", strconv.Quote(i64(row.Version)))
	s.writeJSON(w, http.StatusOK, d)
	return nil
}

func (s *server) deleteSession(w http.ResponseWriter, r *http.Request) error {
	purge := r.URL.Query().Get("purge_artifacts") == "true"
	if purge && !RoleAllows(principalOf(r).Role, RoleAdmin) {
		return apperr.New(apperr.PermissionDenied, "ERR_FORBIDDEN", "purging output needs the admin role")
	}
	if err := s.o.Sessions.Delete(r.Context(), r.PathValue("id"), purge); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// IfMatchVersion reads the version a change is based on.
func IfMatchVersion(header string) (int64, error) {
	if header == "" {
		return 0, apperr.New(apperr.InvalidArgument, "ERR_BAD_REQUEST", "If-Match with the session's version is required")
	}
	version, err := strconv.ParseInt(strings.Trim(strings.TrimPrefix(header, "W/"), `"`), 10, 64)
	if err != nil {
		return 0, apperr.New(apperr.InvalidArgument, "ERR_BAD_REQUEST", "If-Match must hold a version number")
	}
	return version, nil
}

func (s *server) configureSession(w http.ResponseWriter, r *http.Request) error {
	version, err := IfMatchVersion(r.Header.Get("If-Match"))
	if err != nil {
		return err
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	row, err := s.o.Sessions.Configure(r.Context(), r.PathValue("id"), body, version, principalOf(r).Name)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, s.dto(row))
	return nil
}

func (s *server) lifecycle(w http.ResponseWriter, row *store.Session, err error, status int) error {
	if err != nil {
		return err
	}
	s.writeJSON(w, status, s.dto(row))
	return nil
}

func (s *server) startSession(w http.ResponseWriter, r *http.Request) error {
	row, err := s.o.Sessions.Start(r.Context(), r.PathValue("id"), principalOf(r).Name)
	return s.lifecycle(w, row, err, http.StatusAccepted)
}

func (s *server) pauseSession(w http.ResponseWriter, r *http.Request) error {
	row, err := s.o.Sessions.Pause(r.Context(), r.PathValue("id"), principalOf(r).Name)
	return s.lifecycle(w, row, err, http.StatusAccepted)
}

func (s *server) resumeSession(w http.ResponseWriter, r *http.Request) error {
	row, err := s.o.Sessions.Resume(r.Context(), r.PathValue("id"), principalOf(r).Name)
	return s.lifecycle(w, row, err, http.StatusAccepted)
}

func (s *server) stopSession(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		Mode string `json:"mode"`
	}
	if err := readOptionalJSON(r, &req); err != nil {
		return err
	}
	if req.Mode != "" && req.Mode != "graceful" && req.Mode != "immediate" {
		return badRequest("mode must be graceful or immediate")
	}
	row, err := s.o.Sessions.Stop(r.Context(), r.PathValue("id"), req.Mode == "immediate", principalOf(r).Name)
	return s.lifecycle(w, row, err, http.StatusAccepted)
}

func (s *server) cloneSession(w http.ResponseWriter, r *http.Request) error {
	row, err := s.o.Sessions.Clone(r.Context(), r.PathValue("id"), principalOf(r).Name)
	return s.lifecycle(w, row, err, http.StatusCreated)
}

func (s *server) retrySession(w http.ResponseWriter, r *http.Request) error {
	row, err := s.o.Sessions.Retry(r.Context(), r.PathValue("id"), principalOf(r).Name)
	return s.lifecycle(w, row, err, http.StatusCreated)
}

func (s *server) sessionProgress(w http.ResponseWriter, r *http.Request) error {
	row, err := s.o.Sessions.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	d, err := s.detail(r.Context(), row)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"state": d.State, "progress": d.Progress, "rates": d.Rates, "units": d.Units, "pressure": d.Pressure, "errors": d.Errors,
	})
	return nil
}

func (s *server) sessionMetrics(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	if _, err := s.o.Sessions.Get(r.Context(), id); err != nil {
		return err
	}
	history := time.Hour
	if v := r.URL.Query().Get("history"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return badRequest("history must be a duration such as 1h")
		}
		history = d
	}
	type sample struct {
		At      time.Time `json:"at"`
		Records string    `json:"records"`
		Bytes   string    `json:"bytes"`
	}
	out := []sample{}
	err := s.o.DB.Read(r.Context(), func(ctx context.Context, tx store.Tx) error {
		rows, err := tx.ListSamples(ctx, id, s.o.Clock.Now().Add(-history))
		for i := range rows {
			out = append(out, sample{rows[i].Bucket, u64(rows[i].Records), i64(rows[i].Bytes)})
		}
		return err
	})
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"bucket_seconds": 10, "samples": out})
	return nil
}

func (s *server) sessionLogs(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	if _, err := s.o.Sessions.Get(r.Context(), id); err != nil {
		return err
	}
	q := r.URL.Query()
	query := obs.LogQuery{Limit: 1000}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return badRequest("limit must be a positive number")
		}
		query.Limit = min(n, 10000)
	}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return badRequest("since must be an RFC 3339 time")
		}
		query.Since = t
	}
	if v := q.Get("level"); v != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(strings.ToUpper(v))); err != nil {
			return badRequest("level must be debug, info, warn or error")
		}
		query.MinLevel = level
	}
	entries, err := s.o.Logs.Read(id, query)
	if err != nil {
		return err
	}
	if entries == nil {
		entries = []obs.LogEntry{}
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"items": entries})
	return nil
}

func (s *server) sessionEvents(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	if _, err := s.o.Sessions.Get(r.Context(), id); err != nil {
		return err
	}
	rows, err := s.o.Sessions.Events(r.Context(), id, 1000)
	if err != nil {
		return err
	}
	type event struct {
		At     time.Time `json:"at"`
		From   string    `json:"from,omitempty"`
		To     string    `json:"to"`
		Event  string    `json:"event"`
		Actor  string    `json:"actor,omitempty"`
		Reason string    `json:"reason,omitempty"`
	}
	out := make([]event, len(rows))
	for i := range rows {
		e := &rows[i]
		out[i] = event{e.At, e.From, e.To, e.Event, e.Actor, e.Reason}
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"items": out})
	return nil
}

func (s *server) listArtifacts(w http.ResponseWriter, r *http.Request) error {
	row, parts, err := s.sessionParts(r)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"status": ArtifactStatus(row), "items": ToArtifacts(row, parts)})
	return nil
}

func (s *server) artifactManifest(w http.ResponseWriter, r *http.Request) error {
	row, parts, err := s.sessionParts(r)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, coord.ManifestFor(row.Format, parts))
	return nil
}

func (s *server) purgeArtifacts(w http.ResponseWriter, r *http.Request) error {
	if err := s.o.Core.PurgeArtifacts(r.Context(), r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) sessionParts(r *http.Request) (*store.Session, []store.Part, error) {
	id := r.PathValue("id")
	row, err := s.o.Sessions.Get(r.Context(), id)
	if err != nil {
		return nil, nil, err
	}
	parts, err := s.o.Sessions.Parts(r.Context(), id)
	return row, parts, err
}

// streamParts returns one stream's parts, in order.
func streamParts(format string, parts []store.Part, stream string) []artifact.Part {
	m := coord.ManifestFor(format, parts)
	var out []artifact.Part
	for i := range m.Parts {
		if m.Parts[i].Stream == stream {
			out = append(out, m.Parts[i])
		}
	}
	return out
}

// noWriteDeadline lifts the server's write timeout, which is meant for ordinary requests.
func noWriteDeadline(w http.ResponseWriter) {
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{}) // not every writer can; then the limit stands
}

// ContentTypeFor picks a part's media type from its name.
func ContentTypeFor(name string) string {
	switch strings.TrimSuffix(path.Ext(strings.TrimSuffix(name, ".gz")), ".") {
	case ".json":
		return "application/json"
	case ".ndjson":
		return "application/x-ndjson"
	case ".xml":
		return "application/xml"
	case ".csv":
		return "text/csv; charset=utf-8"
	}
	return "application/octet-stream"
}

func (s *server) downloadPart(w http.ResponseWriter, r *http.Request) error {
	row, parts, err := s.sessionParts(r)
	if err != nil {
		return err
	}
	stream := r.PathValue("stream")
	selected := streamParts(row.Format, parts, stream)
	n, err := strconv.Atoi(r.PathValue("n"))
	if err != nil || n < 0 || n >= len(selected) {
		return notFound("part", r.PathValue("n"))
	}
	f, err := s.o.Artifacts.Open(selected[n].RelPath)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	name := path.Base(selected[n].RelPath)
	if strings.HasSuffix(name, ".gz") {
		w.Header().Set("Content-Type", "application/gzip")
	} else {
		w.Header().Set("Content-Type", ContentTypeFor(name))
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, name))
	w.Header().Set("ETag", `"`+selected[n].SHA256+`"`)
	noWriteDeadline(w)
	http.ServeContent(w, r, name, info.ModTime(), f)
	return nil
}

func (s *server) downloadMerged(w http.ResponseWriter, r *http.Request) error {
	row, parts, err := s.sessionParts(r)
	if err != nil {
		return err
	}
	stream := r.PathValue("stream")
	selected := streamParts(row.Format, parts, stream)
	layout, ext, err := s.layoutFor(r.Context(), row, stream)
	if err != nil {
		return err
	}
	if len(selected) == 0 && !session.IsTerminal(row.State) {
		return notFound("output yet for stream", stream)
	}
	merged, err := s.o.Artifacts.Merged(selected, layout)
	if err != nil {
		return apperr.Wrap(err, apperr.Conflict, "ERR_NOT_MERGEABLE", err.Error())
	}
	defer merged.Close()
	name := row.ID + "-" + output.StreamDir(stream) + "." + ext
	w.Header().Set("Content-Type", ContentTypeFor(name))
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, name))
	noWriteDeadline(w)
	http.ServeContent(w, r, name, time.Time{}, merged)
	return nil
}

// layoutFor finds how a stream's parts join, which depends on its format and fields.
func (s *server) layoutFor(ctx context.Context, row *store.Session, stream string) (artifact.Layout, string, error) {
	snap, err := s.o.Sessions.LoadSnapshot(ctx, row.SnapshotHash)
	if err != nil {
		return artifact.Layout{}, "", err
	}
	compiled, err := snap.Compile(s.o.Sessions.Vocab)
	if err != nil {
		return artifact.Layout{}, "", err
	}
	spec, ok := compiled.Stream(stream)
	if !ok {
		return artifact.Layout{}, "", notFound("stream", stream)
	}
	enc, err := compiled.NewEncoder(spec)
	if err != nil {
		return artifact.Layout{}, "", err
	}
	return artifact.Layout{Separator: enc.Separator(), Empty: serial.EmptyDocument(enc)}, enc.Extension(), nil
}

// generate answers with data directly: small, bounded, and with no session, for trying a
// schema. It runs under the request's context, so a client that leaves stops it.
func (s *server) generate(w http.ResponseWriter, r *http.Request) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return s.syncGenerate(w, r, body, r.URL.Query().Get("table"))
}

func (s *server) syncGenerate(w http.ResponseWriter, r *http.Request, spec []byte, table string) error {
	api := &s.o.Config.API
	c, err := s.o.Sessions.ValidateSpec(r.Context(), spec)
	if err != nil {
		return err
	}
	if c.Limits.Count == 0 && c.Strategy != "relational" {
		return badRequest("limits.count is required")
	}
	if c.Limits.Count > uint64(api.SyncMaxRecords) {
		return apperr.New(apperr.PayloadTooLarge, "ERR_TOO_LARGE_FOR_SYNC", fmt.Sprintf("at most %d records can be generated in one request: create a session instead", api.SyncMaxRecords))
	}
	compiled, err := c.Snapshot.Compile(s.o.Sessions.Vocab)
	if err != nil {
		return err
	}
	streams := compiled.Engine.Streams()
	var selected *gen.StreamSpec
	for i := range streams {
		if table == "" && len(streams) == 1 || table != "" && streams[i].ID == gen.TableStream(table) {
			selected = &streams[i]
		}
	}
	if selected == nil {
		return badRequest("choose a table with ?table= for a schema with several streams")
	}
	if selected.Space > uint64(api.SyncMaxRecords)*max(selected.PerParent, 1) {
		return apperr.New(apperr.PayloadTooLarge, "ERR_TOO_LARGE_FOR_SYNC", "this table is too large for one request: create a session instead")
	}
	enc, err := compiled.NewEncoder(*selected)
	if err != nil {
		return err
	}
	generator, err := compiled.Engine.NewStreamGenerator(selected.ID)
	if err != nil {
		return err
	}
	// Bytes are limited as they go, since the sizes of records are not known ahead.
	limited := &limitWriter{w: w, left: int64(api.SyncMaxBytes)}
	w.Header().Set("Content-Type", ContentTypeFor("x."+enc.Extension()))
	w.Header().Set("X-Seed", u64(c.Snapshot.Seed))
	_, err = pipeline.Run(r.Context(), pipeline.Unit{
		Generators: []gen.StreamGenerator{generator}, Stream: string(selected.ID), End: selected.Space, BatchSize: c.Snapshot.BatchSize, QueueCap: c.Snapshot.QueueCapacity,
		Encoder: enc, Sink: &output.WriterSink{W: limited}, Clock: s.o.Clock, MaxRecordBytes: selected.MaxRecordBytes,
	})
	if err != nil && limited.wrote {
		// The status has gone out. Cutting the connection tells the client the body is incomplete.
		s.o.Logger.Warn("sync generation stopped", "err", err)
		panic(http.ErrAbortHandler)
	}
	return err
}

// limitWriter fails once its allowance is used up.
type limitWriter struct {
	w     io.Writer
	left  int64
	wrote bool
}

func (l *limitWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > l.left {
		return 0, apperr.New(apperr.PayloadTooLarge, "ERR_TOO_LARGE_FOR_SYNC", "the output exceeds what one request may return: create a session instead")
	}
	l.left -= int64(len(b))
	l.wrote = true
	return l.w.Write(b)
}
