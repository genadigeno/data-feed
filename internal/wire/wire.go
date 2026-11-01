// Package wire defines what Workers and the Coordinator say to each other: the messages
// and the Coordinator interface as a Worker sees it. Over a network, package protocol
// carries these calls as HTTP; the embedded local worker calls the Coordinator directly.
// Both run the same Worker and the same Coordinator code.
// See docs/01-architecture/coordinator-worker-protocol.md.
package wire

import (
	"context"
	"io"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
)

// ProtocolVersion is the protocol's major version.
const ProtocolVersion = 1

// Error codes specific to the protocol.
const (
	CodeLeaseFenced      = "ERR_LEASE_FENCED"
	CodeLeaseUnknown     = "ERR_LEASE_UNKNOWN"
	CodePartConflict     = "ERR_PART_CONFLICT"
	CodePartOutOfOrder   = "ERR_PART_OUT_OF_ORDER"
	CodePartTooLarge     = "ERR_PART_TOO_LARGE"
	CodeChecksumMismatch = "ERR_CHECKSUM_MISMATCH"
	CodeUnitIncomplete   = "ERR_UNIT_INCOMPLETE"
	CodeUploadInProgress = "ERR_UPLOAD_IN_PROGRESS"
	CodeWorkerRevoked    = "ERR_WORKER_REVOKED"
	CodeOverloaded       = "ERR_OVERLOADED"
	CodeDiskLow          = "ERR_DISK_LOW"
	CodeProtocolVersion  = "ERR_PROTOCOL_VERSION"
)

// Capabilities say what a Worker can run.
type Capabilities struct {
	EngineVersions []int             `json:"engine_versions"`
	Formats        []string          `json:"formats"`
	Strategies     []string          `json:"strategies"`
	OS             string            `json:"os"`
	Arch           string            `json:"arch"`
	Slots          int               `json:"slots"`
	MaxMemoryBytes int64             `json:"max_memory_bytes"`
	SpoolMaxBytes  int64             `json:"spool_max_bytes"`
	BinaryVersion  string            `json:"binary_version"`
	Labels         map[string]string `json:"labels,omitempty"`
}

// RegisterRequest introduces a Worker. A new Worker presents a join token; one that has
// registered before presents its credential instead.
type RegisterRequest struct {
	ProtocolVersion int          `json:"protocol_version"`
	Name            string       `json:"name"`
	Host            string       `json:"host,omitempty"`
	JoinToken       string       `json:"join_token,omitempty"`
	Credential      string       `json:"credential,omitempty"` // worker ID "." secret
	Capabilities    Capabilities `json:"capabilities"`
}

// RegisterResponse tells a Worker who it is and how to behave.
type RegisterResponse struct {
	WorkerID          string `json:"worker_id"`
	Credential        string `json:"credential,omitempty"` // only when newly issued
	HeartbeatInterval int64  `json:"heartbeat_interval_ms"`
	LeaseDuration     int64  `json:"lease_duration_ms"`
}

// LeaseRef identifies one lease on every submission.
type LeaseRef struct {
	UnitID       string `json:"unit_id"`
	LeaseID      string `json:"lease_id"`
	FencingToken uint64 `json:"fencing_token"`
}

// PartLimits say when a Worker must rotate parts.
type PartLimits struct {
	MaxBytes   int64 `json:"max_bytes"`
	MaxRecords int64 `json:"max_records"`
	MaxAgeMS   int64 `json:"max_age_ms"`
}

// Assignment is a leased unit.
type Assignment struct {
	LeaseRef
	SessionID      string     `json:"session_id"`
	LeaseTTLMillis int64      `json:"lease_ttl_ms"`
	SnapshotHash   string     `json:"snapshot_hash"`
	Stream         string     `json:"stream"`
	UnitSeq        uint64     `json:"unit_seq"`
	RangeStart     uint64     `json:"range_start"`
	RangeEnd       uint64     `json:"range_end"`
	ResumeFrom     uint64     `json:"resume_from"`
	NextPartSeq    int        `json:"next_part_seq"`
	PartLimits     PartLimits `json:"part_limits"`
	Rate           float64    `json:"rate"`
}

// UnitProgress reports a unit during a heartbeat.
type UnitProgress struct {
	LeaseRef
	Records          uint64 `json:"records"`
	Bytes            int64  `json:"bytes"`
	GenBlockedNanos  int64  `json:"gen_blocked_ns"`
	EncWaitNanos     int64  `json:"enc_wait_ns"`
	SinkBlockedNanos int64  `json:"sink_blocked_ns"`
	RateWaitNanos    int64  `json:"rate_wait_ns"`
}

// Heartbeat is a Worker's periodic report.
type Heartbeat struct {
	FreeSlots  int            `json:"free_slots"`
	SpoolBytes int64          `json:"spool_bytes"`
	Draining   bool           `json:"draining"`
	Units      []UnitProgress `json:"units"`
}

// Directive actions.
const (
	DirectiveRelease = "release" // finish the current part, upload it, give the unit back
	DirectiveFinish  = "finish"  // finish the current part, upload it, complete the unit there
	DirectiveCancel  = "cancel"  // abort and discard
	DirectiveSetRate = "set_rate"
	DirectiveDrain   = "drain" // take no new work; applies to the whole Worker
)

// Directive tells a Worker what to do.
type Directive struct {
	UnitID string  `json:"unit_id,omitempty"`
	Action string  `json:"action"`
	Rate   float64 `json:"rate,omitempty"`
}

// HeartbeatResponse answers a heartbeat.
type HeartbeatResponse struct {
	Lost           []string    `json:"lost"` // lease IDs the Worker no longer holds
	LeaseTTLMillis int64       `json:"lease_ttl_ms"`
	Directives     []Directive `json:"directives,omitempty"`
}

// PartMeta describes a part being uploaded.
type PartMeta struct {
	LeaseRef
	Seq        int    `json:"seq"`
	FirstIndex uint64 `json:"first_index"`
	EndIndex   uint64 `json:"end_index"`
	Records    uint64 `json:"records"`
	Bytes      int64  `json:"bytes"`
	SHA256     string `json:"sha256"`
	HeaderLen  int    `json:"header_len"`
	FooterLen  int    `json:"footer_len"`
	Extension  string `json:"extension"`
}

// UploadResult acknowledges a part.
type UploadResult struct {
	CommittedThrough uint64 `json:"committed_through"`
	Duplicate        bool   `json:"duplicate"`
}

// PartBody is a part's content. LocalPath is set only by the embedded worker, whose
// spool is on the Coordinator's filesystem, so that promoting a part needs no copy.
type PartBody struct {
	Reader    io.Reader
	LocalPath string
}

// Coordinator is the Coordinator as a Worker sees it. Implementations authenticate the
// caller: worker is who the transport proved it to be.
type Coordinator interface {
	Register(ctx context.Context, r RegisterRequest) (RegisterResponse, error)
	Heartbeat(ctx context.Context, worker string, h Heartbeat) (HeartbeatResponse, error)
	// Lease waits up to wait for work, returning up to max assignments, possibly none.
	Lease(ctx context.Context, worker string, max int, wait time.Duration) ([]Assignment, error)
	Snapshot(ctx context.Context, worker, hash string) ([]byte, error)
	// PartCommitted reports whether a part is already stored, and its checksum.
	PartCommitted(ctx context.Context, worker, unit string, seq int) (bool, string, error)
	UploadPart(ctx context.Context, worker string, m PartMeta, body PartBody) (UploadResult, error)
	Complete(ctx context.Context, worker string, ref LeaseRef, end uint64, parts int) error
	Release(ctx context.Context, worker string, ref LeaseRef) error
	Fail(ctx context.Context, worker string, ref LeaseRef, message string, retryable bool) error
	Deregister(ctx context.Context, worker string) error
}

// IsFenced reports an error meaning the Worker no longer owns what it is working on, so
// it must stop and discard rather than retry.
func IsFenced(err error) bool {
	e, ok := apperr.As(err)
	if !ok {
		return false
	}
	switch e.Code {
	case CodeLeaseFenced, CodeLeaseUnknown, CodeWorkerRevoked:
		return true
	}
	return false
}

// IsRetryable reports an error worth retrying with backoff. Anything that is not a known
// application error, such as a network failure, may well pass.
func IsRetryable(err error) bool {
	e, ok := apperr.As(err)
	if !ok {
		return true
	}
	return e.Retryable || e.Code == CodeUploadInProgress || e.Kind == apperr.Internal
}
