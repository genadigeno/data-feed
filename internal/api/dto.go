package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/coord"
	"github.com/genadigeno/data-feed/internal/session"
	"github.com/genadigeno/data-feed/internal/store"
)

// Counts that can exceed 2^53 travel as strings, because JavaScript would round them.

// CatalogRefDTO names a pinned catalog.
type CatalogRefDTO struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
}

// LimitsDTO is a session's limits.
type LimitsDTO struct {
	Count      string  `json:"count,omitempty"`
	DurationMS int64   `json:"duration_ms,omitempty"`
	Rate       float64 `json:"rate,omitempty"`
	MaxWorkers int     `json:"max_workers,omitempty"`
	MaxBytes   string  `json:"max_bytes,omitempty"`
}

// ProgressDTO says how far a session has come.
type ProgressDTO struct {
	Records         string   `json:"records"`
	Bytes           string   `json:"bytes"`
	InFlightRecords string   `json:"in_flight_records"`
	ExpectedRecords string   `json:"expected_records,omitempty"`
	Fraction        *float64 `json:"fraction,omitempty"`
}

// RatesDTO is records per second.
type RatesDTO struct {
	Current float64 `json:"current"`
	Average float64 `json:"average"`
	Peak    float64 `json:"peak"`
}

// UnitsDTO counts work units by state.
type UnitsDTO struct {
	Pending   int64 `json:"pending"`
	Leased    int64 `json:"leased"`
	Committed int64 `json:"committed"`
	Failed    int64 `json:"failed"`
}

// ErrorsDTO summarises failures.
type ErrorsDTO struct {
	Count int64  `json:"count"`
	Last  string `json:"last,omitempty"`
}

// PressureDTO names the bottleneck, by seconds spent blocked across running units.
type PressureDTO struct {
	OutputBlockedSeconds     float64 `json:"output_blocked_seconds"`
	GenerationStarvedSeconds float64 `json:"generation_starved_seconds"`
	SinkBlockedSeconds       float64 `json:"sink_blocked_seconds"`
	RateLimitedSeconds       float64 `json:"rate_limited_seconds"`
}

// SessionDTO is a session.
type SessionDTO struct {
	ID               string          `json:"id"`
	Name             string          `json:"name"`
	Description      string          `json:"description,omitempty"`
	Strategy         string          `json:"strategy"`
	Mode             string          `json:"mode"`
	Format           string          `json:"format"`
	State            string          `json:"state"`
	StateReason      string          `json:"state_reason,omitempty"`
	AvailableActions []string        `json:"available_actions"`
	Seed             string          `json:"seed"`
	ReferenceTime    time.Time       `json:"reference_time"`
	EngineVersion    int             `json:"engine_version"`
	SnapshotHash     string          `json:"snapshot_hash"`
	Catalog          *CatalogRefDTO  `json:"catalog,omitempty"`
	Limits           LimitsDTO       `json:"limits"`
	CreatedAt        time.Time       `json:"created_at"`
	StartedAt        *time.Time      `json:"started_at,omitempty"`
	EndedAt          *time.Time      `json:"ended_at,omitempty"`
	PausedTotalMS    int64           `json:"paused_total_ms"`
	LastActivityAt   *time.Time      `json:"last_activity_at,omitempty"`
	Progress         ProgressDTO     `json:"progress"`
	Rates            RatesDTO        `json:"rates"`
	Units            *UnitsDTO       `json:"units,omitempty"`
	Pressure         *PressureDTO    `json:"pressure,omitempty"`
	Errors           ErrorsDTO       `json:"errors"`
	CreatedBy        string          `json:"created_by,omitempty"`
	RetryOf          string          `json:"retry_of,omitempty"`
	Pinned           bool            `json:"pinned,omitempty"`
	ArtifactsDegrade bool            `json:"artifacts_degraded,omitempty"`
	Version          int64           `json:"version"`
	Spec             json.RawMessage `json:"spec,omitempty"`
}

func u64(v uint64) string { return strconv.FormatUint(v, 10) }

func i64(v int64) string { return strconv.FormatInt(v, 10) }

// AverageRate is records per second of running time.
func AverageRate(s *store.Session, now time.Time) float64 {
	if s.StartedAt == nil {
		return 0
	}
	end := now
	if s.EndedAt != nil {
		end = *s.EndedAt
	}
	running := end.Sub(*s.StartedAt) - s.PausedTotal
	if s.PausedAt != nil && s.EndedAt == nil {
		running -= now.Sub(*s.PausedAt)
	}
	if running <= 0 {
		return 0
	}
	return float64(s.Records) / running.Seconds()
}

// ToSessionDTO maps a session and what is known about it right now.
func ToSessionDTO(s *store.Session, live coord.Live, now time.Time) SessionDTO {
	limits := coord.ParseLimits(s)
	d := SessionDTO{
		ID: s.ID, Name: s.Name, Description: s.Description, Strategy: s.Strategy, Mode: s.Mode, Format: s.Format, State: s.State, StateReason: s.StateReason,
		AvailableActions: session.AvailableActions(s.State), Seed: u64(s.Seed), ReferenceTime: s.ReferenceTime, EngineVersion: s.EngineVersion,
		SnapshotHash: s.SnapshotHash, CreatedAt: s.CreatedAt, StartedAt: s.StartedAt, EndedAt: s.EndedAt, PausedTotalMS: s.PausedTotal.Milliseconds(),
		LastActivityAt: s.LastActivityAt, CreatedBy: s.CreatedBy, RetryOf: s.RetryOf, Pinned: s.Pinned, ArtifactsDegrade: s.ArtifactsDegraded, Version: s.Version,
		Limits:   LimitsDTO{DurationMS: limits.DurationMS, Rate: s.Rate, MaxWorkers: s.MaxWorkers},
		Progress: ProgressDTO{Records: u64(s.Records), Bytes: i64(s.Bytes), InFlightRecords: u64(live.InFlight)},
		Rates:    RatesDTO{Current: live.Rate, Average: AverageRate(s, now), Peak: s.PeakRate},
		Errors:   ErrorsDTO{Count: s.ErrorCount, Last: s.LastError},
	}
	if d.AvailableActions == nil {
		d.AvailableActions = []string{}
	}
	if limits.Count > 0 {
		d.Limits.Count = u64(limits.Count)
	}
	if limits.MaxBytes > 0 {
		d.Limits.MaxBytes = i64(limits.MaxBytes)
	}
	if s.CatalogID != "" {
		d.Catalog = &CatalogRefDTO{ID: s.CatalogID, Version: s.CatalogVersion}
	}
	return d
}

// WithProgress adds unit counts and the expected size, for a detailed view.
func WithProgress(d *SessionDTO, p session.Progress, live coord.Live, records uint64) {
	d.Units = &UnitsDTO{
		Pending: p.Units[store.UnitPending], Leased: p.Units[store.UnitLeased], Committed: p.Units[store.UnitCommitted], Failed: p.Units[store.UnitFailed],
	}
	d.Pressure = &PressureDTO{
		OutputBlockedSeconds: live.GenBlocked.Seconds(), GenerationStarvedSeconds: live.EncWait.Seconds(),
		SinkBlockedSeconds: live.SinkBlocked.Seconds(), RateLimitedSeconds: live.RateWait.Seconds(),
	}
	if p.Expected > 0 {
		d.Progress.ExpectedRecords = strconv.FormatFloat(p.Expected, 'f', 0, 64)
		fraction := min(float64(records)/p.Expected, 1)
		d.Progress.Fraction = &fraction
	}
}

// PageDTO is a page of a listing.
type PageDTO[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// EncodeCursor makes an opaque cursor.
func EncodeCursor(last string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(last))
}

// DecodeCursor reads one back.
func DecodeCursor(cursor string) (string, error) {
	if cursor == "" {
		return "", nil
	}
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", apperr.New(apperr.InvalidArgument, "ERR_BAD_CURSOR", "invalid cursor")
	}
	return string(data), nil
}

// PageLimit reads a page size, within its bounds.
func PageLimit(raw string, def, maxLimit int) (int, error) {
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, apperr.New(apperr.InvalidArgument, "ERR_BAD_REQUEST", "limit must be a positive number")
	}
	return min(n, maxLimit), nil
}

// page reads paging parameters; one extra row is asked for, to learn if more follow.
func (s *server) page(r *http.Request) (after string, limit int, err error) {
	api := &s.o.Config.API
	limit, err = PageLimit(r.URL.Query().Get("limit"), api.PageDefaultLimit, api.PageMaxLimit)
	if err != nil {
		return "", 0, err
	}
	after, err = DecodeCursor(r.URL.Query().Get("cursor"))
	return after, limit, err
}

// MakePage cuts the extra row and builds the cursor.
func MakePage[R, T any](rows []R, limit int, id func(*R) string, convert func(*R) T) PageDTO[T] {
	out := PageDTO[T]{Items: make([]T, 0, min(len(rows), limit))}
	for i := 0; i < len(rows) && i < limit; i++ {
		out.Items = append(out.Items, convert(&rows[i]))
	}
	if len(rows) > limit {
		out.NextCursor = EncodeCursor(id(&rows[limit-1]))
	}
	return out
}

// SessionFilterFrom reads a listing's filters.
func SessionFilterFrom(r *http.Request) (store.SessionFilter, error) {
	q := r.URL.Query()
	f := store.SessionFilter{Mode: q.Get("mode"), Query: q.Get("q"), Sort: q.Get("sort")}
	if v := q.Get("state"); v != "" {
		f.States = strings.Split(v, ",")
	}
	switch f.Sort {
	case "", "-created_at", "created_at":
	default:
		return f, apperr.New(apperr.InvalidArgument, "ERR_BAD_REQUEST", "sort must be created_at or -created_at")
	}
	return f, nil
}

// PartDTO is a downloadable part.
type PartDTO struct {
	N           int    `json:"n"`
	UnitSeq     uint64 `json:"unit_seq"`
	Seq         int    `json:"seq"`
	FirstIndex  string `json:"first_index"`
	EndIndex    string `json:"end_index"`
	Records     string `json:"records"`
	Bytes       string `json:"bytes"`
	SHA256      string `json:"sha256"`
	DownloadURL string `json:"download_url"`
}

// ArtifactDTO is one stream's output.
type ArtifactDTO struct {
	Stream    string    `json:"stream"`
	Format    string    `json:"format"`
	Status    string    `json:"status"`
	Records   string    `json:"records"`
	Bytes     string    `json:"bytes"`
	Parts     []PartDTO `json:"parts"`
	MergedURL string    `json:"merged_url,omitempty"`
}

// Artifact statuses.
const (
	ArtifactComplete = "COMPLETE"
	ArtifactPartial  = "PARTIAL"
	ArtifactDegraded = "DEGRADED"
)

// ArtifactStatus says what can be relied on.
func ArtifactStatus(s *store.Session) string {
	switch {
	case s.ArtifactsDegraded:
		return ArtifactDegraded
	case s.State == session.Completed:
		return ArtifactComplete
	}
	return ArtifactPartial
}

// ToArtifacts groups parts into streams, in order.
func ToArtifacts(s *store.Session, parts []store.Part) []ArtifactDTO {
	m := coord.ManifestFor(s.Format, parts)
	out := []ArtifactDTO{}
	var cur *ArtifactDTO
	var records uint64
	var bytes int64
	flush := func() {
		if cur != nil {
			cur.Records, cur.Bytes = u64(records), i64(bytes)
		}
	}
	for i := range m.Parts {
		p := &m.Parts[i]
		if cur == nil || cur.Stream != p.Stream {
			flush()
			base := prefix + "/sessions/" + s.ID + "/artifacts/" + p.Stream
			out = append(out, ArtifactDTO{Stream: p.Stream, Format: s.Format, Status: ArtifactStatus(s), Parts: []PartDTO{}})
			cur = &out[len(out)-1]
			if m.Compression == "none" {
				cur.MergedURL = base + "/merged"
			}
			records, bytes = 0, 0
		}
		cur.Parts = append(cur.Parts, PartDTO{
			N: len(cur.Parts), UnitSeq: uint64(p.UnitSeq), Seq: p.Seq, FirstIndex: u64(p.FirstIndex), EndIndex: u64(p.EndIndex), Records: u64(p.Records), Bytes: i64(p.Bytes), SHA256: p.SHA256,
			DownloadURL: prefix + "/sessions/" + s.ID + "/artifacts/" + p.Stream + "/parts/" + strconv.Itoa(len(cur.Parts)),
		})
		records += p.Records
		bytes += p.Bytes
	}
	flush()
	return out
}

// WorkerDTO is a Worker. Its credential is never included.
type WorkerDTO struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	Host            string          `json:"host,omitempty"`
	State           string          `json:"state"`
	Slots           int             `json:"slots"`
	Capabilities    json.RawMessage `json:"capabilities,omitempty"`
	RegisteredAt    time.Time       `json:"registered_at"`
	LastHeartbeatAt *time.Time      `json:"last_heartbeat_at,omitempty"`
	LastError       string          `json:"last_error,omitempty"`
}

// ToWorkerDTO maps a Worker.
func ToWorkerDTO(w *store.Worker) WorkerDTO {
	d := WorkerDTO{ID: w.ID, Name: w.Name, Host: w.Host, State: w.State, Slots: w.Slots, RegisteredAt: w.RegisteredAt, LastHeartbeatAt: w.LastHeartbeatAt, LastError: w.LastError}
	if json.Valid([]byte(w.CapabilitiesJSON)) {
		d.Capabilities = json.RawMessage(w.CapabilitiesJSON)
	}
	return d
}

// TokenDTO is a token, without its secret.
type TokenDTO struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Role      string     `json:"role"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
	// Token is present only in the response that created it.
	Token string `json:"token,omitempty"`
}

// ToTokenDTO maps a token.
func ToTokenDTO(t *store.APIToken) TokenDTO {
	return TokenDTO{ID: t.ID, Name: t.Name, Role: t.Role, CreatedAt: t.CreatedAt, ExpiresAt: t.ExpiresAt, RevokedAt: t.RevokedAt}
}

// DomainDTO is a domain.
type DomainDTO struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Locale      string    `json:"locale,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// ToDomainDTO maps a domain.
func ToDomainDTO(d *store.Domain) DomainDTO {
	return DomainDTO{ID: d.ID, Name: d.Name, Description: d.Description, Locale: d.Locale, CreatedAt: d.CreatedAt}
}

// CatalogDTO is a catalog version.
type CatalogDTO struct {
	ID            string     `json:"id"`
	Version       int        `json:"version"`
	DomainID      string     `json:"domain_id"`
	Status        string     `json:"status"`
	ParentVersion int        `json:"parent_version,omitempty"`
	ContentHash   string     `json:"content_hash,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	PublishedAt   *time.Time `json:"published_at,omitempty"`
}

// ToCatalogDTO maps a catalog.
func ToCatalogDTO(c *store.Catalog) CatalogDTO {
	return CatalogDTO{
		ID: c.ID, Version: c.Version, DomainID: c.DomainID, Status: c.Status, ParentVersion: c.ParentVersion,
		ContentHash: c.ContentHash, CreatedAt: c.CreatedAt, PublishedAt: c.PublishedAt,
	}
}

// JobDTO is a vocabulary job.
type JobDTO struct {
	ID        string          `json:"id"`
	CatalogID string          `json:"catalog_id"`
	Version   int             `json:"version"`
	State     string          `json:"state"`
	Progress  json.RawMessage `json:"progress,omitempty"`
	Usage     json.RawMessage `json:"usage,omitempty"`
	Error     string          `json:"error,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	EndedAt   *time.Time      `json:"ended_at,omitempty"`
}

// ToJobDTO maps a job.
func ToJobDTO(j *store.VocabJob) JobDTO {
	d := JobDTO{ID: j.ID, CatalogID: j.CatalogID, Version: j.TargetVersion, State: j.State, Error: j.Error, CreatedAt: j.CreatedAt, EndedAt: j.EndedAt}
	if json.Valid([]byte(j.ProgressJSON)) {
		d.Progress = json.RawMessage(j.ProgressJSON)
	}
	if json.Valid([]byte(j.UsageJSON)) {
		d.Usage = json.RawMessage(j.UsageJSON)
	}
	return d
}

// SchemaDTO is a saved schema version.
type SchemaDTO struct {
	ID         string          `json:"id"`
	Version    int             `json:"version"`
	Name       string          `json:"name"`
	Kind       string          `json:"kind"`
	Definition json.RawMessage `json:"definition,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

// ToSchemaDTO maps a schema.
func ToSchemaDTO(s *store.SchemaRow, withDefinition bool) SchemaDTO {
	d := SchemaDTO{ID: s.ID, Version: s.Version, Name: s.Name, Kind: s.Kind, CreatedAt: s.CreatedAt}
	if withDefinition && json.Valid([]byte(s.Definition)) {
		d.Definition = json.RawMessage(s.Definition)
	}
	return d
}

// DashboardDTO is the overview.
type DashboardDTO struct {
	Sessions         map[string]int64 `json:"sessions"`
	Workers          map[string]int   `json:"workers"`
	RecordsCommitted string           `json:"records_committed"`
	BytesCommitted   string           `json:"bytes_committed"`
	CurrentRate      float64          `json:"current_rate"`
	Recent           []SessionDTO     `json:"recent"`
}
