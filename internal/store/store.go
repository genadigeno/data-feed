// Package store declares the persistence port: plain row types and transactions. Only
// store/sqlite implements it, and services never see SQL.
package store

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound means no row matched.
var ErrNotFound = errors.New("not found")

// ErrConflict means a compare-and-set lost, or a key already exists.
var ErrConflict = errors.New("conflict")

// Session is a session row.
type Session struct {
	ID, Name, Description  string
	Strategy, Mode, Format string
	SpecJSON               string // the specification as submitted
	SnapshotHash           string
	CatalogID              string
	CatalogVersion         int
	Seed                   uint64
	ReferenceTime          time.Time
	EngineVersion          int
	State, StateReason     string
	Version                int64
	LimitsJSON             string
	CreatedAt              time.Time
	QueuedAt, StartedAt    *time.Time
	EndedAt, PausedAt      *time.Time
	PausedTotal            time.Duration
	Records                uint64 // committed
	Bytes                  int64  // committed
	PeakRate               float64
	ErrorCount             int64
	LastError              string
	LastActivityAt         *time.Time
	CreatedBy              string
	RetryOf                string
	Pinned                 bool
	Purging                bool
	ArtifactsDegraded      bool
	StopImmediate          bool
	Finishing              bool   // a stop condition was reached: no new units
	WorkerArch             string // the architecture this session runs on, once chosen
	Rate                   float64
	MaxWorkers             int
}

// Event is one lifecycle transition.
type Event struct {
	ID        int64
	SessionID string
	At        time.Time
	From, To  string
	Event     string
	Actor     string
	Reason    string
}

// SessionFilter narrows a listing.
type SessionFilter struct {
	States []string
	Mode   string
	Query  string
	Sort   string // "created_at" or "-created_at" (default) or "name"
	After  string // cursor: the last ID seen
	Limit  int
}

// StreamState tracks lazy planning of one stream of a session.
type StreamState struct {
	SessionID      string
	Stream         string
	End            uint64 // end of the index space; always known, unbounded streams use 2^63
	PerParent      uint64
	ExpectedRows   float64
	MaxRecordBytes int64
	NextIndex      uint64
	NextSeq        uint64
	Exhausted      bool
	Position       int // order among the session's streams
}

// Unit states.
const (
	UnitPending   = "PENDING"
	UnitLeased    = "LEASED"
	UnitCommitted = "COMMITTED"
	UnitFailed    = "FAILED"
	UnitCancelled = "CANCELLED"
)

// Unit is a work unit row.
type Unit struct {
	ID               string
	SessionID        string
	Stream           string
	Seq              uint64
	RangeStart       uint64
	RangeEnd         uint64
	CommittedThrough uint64
	NextPartSeq      int
	State            string
	FencingToken     uint64
	LeaseID          string
	WorkerID         string
	LeaseExpiresAt   time.Time
	GrantedAt        time.Time
	Attempts         int
	LastError        string
	Records          uint64
	Bytes            int64
}

// Part is a committed output part.
type Part struct {
	UnitID       string
	Seq          int
	SessionID    string
	Stream       string
	UnitSeq      uint64
	FirstIndex   uint64
	EndIndex     uint64
	Records      uint64
	Bytes        int64
	SHA256       string
	RelPath      string
	HeaderLen    int
	FooterLen    int
	FencingToken uint64
	WorkerID     string
	CommittedAt  time.Time
}

// Worker states.
const (
	WorkerActive       = "ACTIVE"
	WorkerDraining     = "DRAINING"
	WorkerDisconnected = "DISCONNECTED"
	WorkerRevoked      = "REVOKED"
)

// Worker is a registered Worker.
type Worker struct {
	ID               string
	Name             string
	Host             string
	CredentialHash   []byte
	CapabilitiesJSON string
	Slots            int
	State            string
	RegisteredAt     time.Time
	LastHeartbeatAt  *time.Time
	LastError        string
}

// JoinToken lets new Workers register.
type JoinToken struct {
	ID        string
	Hash      []byte
	CreatedAt time.Time
	ExpiresAt *time.Time
	MaxUses   int // 0 means no limit
	Uses      int
}

// Sample is a bucket of throughput history.
type Sample struct {
	SessionID string
	Bucket    time.Time
	Records   uint64
	Bytes     int64
}

// SchemaRow is one version of a saved schema.
type SchemaRow struct {
	ID         string
	Version    int
	Name       string
	Kind       string
	Definition string // canonical JSON
	CreatedAt  time.Time
}

// APIToken authenticates the public API.
type APIToken struct {
	ID        string
	Name      string
	Hash      []byte
	Role      string
	CreatedAt time.Time
	ExpiresAt *time.Time
	RevokedAt *time.Time
}

// IdempotencyRecord replays the response of a repeated request.
type IdempotencyRecord struct {
	Principal    string
	Key          string
	RequestHash  []byte
	Status       int
	ResponseJSON string
	CreatedAt    time.Time
}

// Domain is a business area that catalogs belong to.
type Domain struct {
	ID, Name, Description, Locale string
	CreatedAt                     time.Time
}

// Catalog statuses.
const (
	CatalogDraft     = "DRAFT"
	CatalogPublished = "PUBLISHED"
)

// Catalog is one version of a vocabulary catalog.
type Catalog struct {
	ID            string
	Version       int
	DomainID      string
	Status        string
	ParentVersion int // 0 means none
	ContentHash   string
	CreatedAt     time.Time
	PublishedAt   *time.Time
}

// CatalogGroup is a vocabulary group's definition.
type CatalogGroup struct {
	CatalogID    string
	Version      int
	Key          string
	Name         string
	Description  string
	DataType     string
	Category     string
	Locale       string
	MetadataJSON string
}

// CatalogValue is one vocabulary entry.
type CatalogValue struct {
	CatalogID    string
	Version      int
	GroupKey     string
	Value        string
	Weight       float64
	MetadataJSON string
}

// VocabJob is a vocabulary generation job.
type VocabJob struct {
	ID            string
	CatalogID     string
	TargetVersion int
	RequestJSON   string
	State         string
	ProgressJSON  string
	UsageJSON     string
	Error         string
	CreatedAt     time.Time
	EndedAt       *time.Time
}

// Tx is what a transaction can do. A write transaction serializes with every other, so
// check-then-act inside one callback is atomic.
type Tx interface {
	// Snapshots
	PutSnapshot(ctx context.Context, hash string, engineVersion int, body []byte, at time.Time) error
	GetSnapshot(ctx context.Context, hash string) ([]byte, error)
	DeleteUnreferencedSnapshots(ctx context.Context, olderThan time.Time) (int64, error)

	// Sessions
	InsertSession(ctx context.Context, s *Session) error
	GetSession(ctx context.Context, id string) (*Session, error)
	UpdateSession(ctx context.Context, s *Session, expectVersion int64) error
	ListSessions(ctx context.Context, f SessionFilter) ([]Session, error)
	SessionsInStates(ctx context.Context, states ...string) ([]Session, error)
	CountSessionsByState(ctx context.Context) (map[string]int64, error)
	SumCommitted(ctx context.Context) (records uint64, bytes int64, err error)
	DeleteSession(ctx context.Context, id string) error
	InsertEvent(ctx context.Context, e *Event) error
	ListEvents(ctx context.Context, session string, limit int) ([]Event, error)
	PutSample(ctx context.Context, s Sample) error
	ListSamples(ctx context.Context, session string, since time.Time) ([]Sample, error)
	TrimSamples(ctx context.Context, session string, keep int) error

	// Streams and units
	InsertStreamState(ctx context.Context, s *StreamState) error
	UpdateStreamState(ctx context.Context, s *StreamState) error
	StreamStates(ctx context.Context, session string) ([]StreamState, error)
	InsertUnit(ctx context.Context, u *Unit) error
	GetUnit(ctx context.Context, id string) (*Unit, error)
	UpdateUnit(ctx context.Context, u *Unit) error
	NextPendingUnit(ctx context.Context, session string) (*Unit, error)
	UnitsInState(ctx context.Context, session string, state string) ([]Unit, error)
	CountUnits(ctx context.Context, session string) (map[string]int64, error)
	LeasedUnits(ctx context.Context) ([]Unit, error)
	LeasedByWorker(ctx context.Context, worker string) ([]Unit, error)

	// Parts
	InsertPart(ctx context.Context, p *Part) error
	GetPart(ctx context.Context, unit string, seq int) (*Part, error)
	SessionParts(ctx context.Context, session string) ([]Part, error)
	AllParts(ctx context.Context) ([]Part, error)
	DeletePart(ctx context.Context, unit string, seq int) error

	// Workers
	PutWorker(ctx context.Context, w *Worker) error
	GetWorker(ctx context.Context, id string) (*Worker, error)
	ListWorkers(ctx context.Context) ([]Worker, error)
	CountWorkers(ctx context.Context) (int64, error)
	DeleteWorker(ctx context.Context, id string) error
	InsertJoinToken(ctx context.Context, t *JoinToken) error
	FindJoinToken(ctx context.Context, hash []byte) (*JoinToken, error)
	UpdateJoinTokenUses(ctx context.Context, id string, uses int) error

	// Schemas
	PutSchema(ctx context.Context, s *SchemaRow) error
	GetSchema(ctx context.Context, id string, version int) (*SchemaRow, error) // version 0 means the latest
	ListSchemas(ctx context.Context, after string, limit int) ([]SchemaRow, error)
	DeleteSchema(ctx context.Context, id string) error

	// Vocabulary
	PutDomain(ctx context.Context, d *Domain) error
	GetDomain(ctx context.Context, id string) (*Domain, error)
	ListDomains(ctx context.Context, after string, limit int) ([]Domain, error)
	DeleteDomain(ctx context.Context, id string) error
	PutCatalog(ctx context.Context, c *Catalog) error
	GetCatalog(ctx context.Context, id string, version int) (*Catalog, error) // version 0 means the latest
	LatestPublished(ctx context.Context, id string) (*Catalog, error)
	CatalogVersions(ctx context.Context, id string) ([]Catalog, error)
	ListCatalogs(ctx context.Context, domain, after string, limit int) ([]Catalog, error)
	PutGroup(ctx context.Context, g *CatalogGroup) error
	DeleteGroup(ctx context.Context, catalog string, version int, key string) error
	Groups(ctx context.Context, catalog string, version int) ([]CatalogGroup, error)
	PutValue(ctx context.Context, v *CatalogValue) error
	DeleteValue(ctx context.Context, catalog string, version int, group, value string) error
	Values(ctx context.Context, catalog string, version int, group string) ([]CatalogValue, error)
	PutVocabJob(ctx context.Context, j *VocabJob) error
	GetVocabJob(ctx context.Context, id string) (*VocabJob, error)
	VocabJobsInState(ctx context.Context, state string) ([]VocabJob, error)
	GetAICache(ctx context.Context, key string) (string, error)
	PutAICache(ctx context.Context, key, provider, model, response string, at time.Time) error
	DeleteAICacheBefore(ctx context.Context, before time.Time) error

	// API
	InsertAPIToken(ctx context.Context, t *APIToken) error
	FindAPIToken(ctx context.Context, hash []byte) (*APIToken, error)
	ListAPITokens(ctx context.Context) ([]APIToken, error)
	RevokeAPIToken(ctx context.Context, id string, at time.Time) error
	CountAPITokens(ctx context.Context) (int64, error)
	GetIdempotency(ctx context.Context, principal, key string) (*IdempotencyRecord, error)
	PutIdempotency(ctx context.Context, r *IdempotencyRecord) error
	DeleteIdempotencyBefore(ctx context.Context, before time.Time) error
}

// DB runs transactions.
type DB interface {
	// Write runs fn in the single write transaction. fn must do no network, generation,
	// AI or slow file work, may run more than once, and must pass on its context.
	Write(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
	// Read runs fn on a consistent snapshot.
	Read(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
}
