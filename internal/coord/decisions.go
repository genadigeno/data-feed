// Package coord is the control plane: it plans sessions into work units, leases them to
// Workers, commits their output, and moves sessions through their lifecycle. Decisions
// live here as pure functions; the methods on Core load rows, decide and store.
// See docs/01-architecture/distributed-execution.md.
package coord

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/genadigeno/data-feed/internal/session"
	"github.com/genadigeno/data-feed/internal/store"
	"github.com/genadigeno/data-feed/internal/wire"
)

// memoryBudgetPercent matches the configuration's rule for what units may use.
const memoryBudgetPercent = 80

// Candidate is a running session as the scheduler sees it.
type Candidate struct {
	SessionID  string
	Leased     int
	MaxWorkers int // effective limit on concurrent units
	Engine     int
	Format     string
	Strategy   string
	Arch       string // required architecture, once chosen
	Selector   map[string]string
	UnitMemory int64
	Rate       float64
	Empty      bool // no more units to hand out for now
}

// Eligible decides whether a Worker may run a session's units.
func Eligible(c *Candidate, caps *wire.Capabilities) bool {
	if !slices.Contains(caps.EngineVersions, c.Engine) || !slices.Contains(caps.Formats, c.Format) || !slices.Contains(caps.Strategies, c.Strategy) {
		return false
	}
	// Float-based values can differ in their last bit between architectures, which would
	// break byte-identical output, so a session stays on one.
	if c.Arch != "" && c.Arch != caps.Arch {
		return false
	}
	for k, v := range c.Selector {
		if caps.Labels[k] != v {
			return false
		}
	}
	if c.UnitMemory > 0 && caps.MaxMemoryBytes > 0 && c.UnitMemory > caps.MaxMemoryBytes/100*memoryBudgetPercent {
		return false
	}
	return true
}

// Pick chooses the next session to get a unit, taking turns from rotation so that one busy
// session cannot starve another. It returns the index, or -1 if none qualifies.
func Pick(cands []Candidate, caps *wire.Capabilities, rotation int) int {
	n := len(cands)
	for i := range n {
		at := (rotation + i) % n
		c := &cands[at]
		if c.Empty || c.Leased >= c.MaxWorkers || !Eligible(c, caps) {
			continue
		}
		return at
	}
	return -1
}

// UnitRate splits a session's rate over its running units.
func UnitRate(rate float64, leased int) float64 {
	if rate <= 0 {
		return 0
	}
	return rate / float64(max(leased, 1))
}

// DirectiveFor says what a Worker should do with a unit, given its session.
func DirectiveFor(s *store.Session, unitID string) (wire.Directive, bool) {
	switch {
	case s.State == session.Pausing || s.State == session.Paused:
		return wire.Directive{UnitID: unitID, Action: wire.DirectiveRelease}, true
	case s.State == session.Stopping && s.StopImmediate, s.State == session.Failed || s.State == session.Stopped:
		return wire.Directive{UnitID: unitID, Action: wire.DirectiveCancel}, true
	case s.State == session.Stopping || s.Finishing:
		return wire.Directive{UnitID: unitID, Action: wire.DirectiveFinish}, true
	}
	return wire.Directive{}, false
}

// ShouldFinish reports whether a running session reached a stop condition. Its running
// units are then told to close up, and it completes when they have.
func ShouldFinish(s *store.Session, l *session.StoredLimits, now time.Time) bool {
	if s.Finishing || s.State != session.Running {
		return false
	}
	if l.MaxBytes > 0 && s.Bytes >= l.MaxBytes {
		return true
	}
	if l.DurationMS > 0 && s.StartedAt != nil {
		deadline := s.StartedAt.Add(s.PausedTotal + time.Duration(l.DurationMS)*time.Millisecond)
		return !now.Before(deadline)
	}
	return false
}

// DrainEvent decides whether a session has settled, given its units: the event to apply,
// or "" if it is still busy.
func DrainEvent(s *store.Session, leased, pending int64, exhausted bool) string {
	switch s.State {
	case session.Pausing:
		if leased == 0 {
			return session.EvDrained
		}
	case session.Stopping:
		if leased == 0 {
			return session.EvDrained
		}
	case session.Running:
		if leased == 0 && (s.Finishing || (exhausted && pending == 0)) {
			return session.EvDone
		}
	}
	return ""
}

// HashSecret is how credentials and tokens are stored.
func HashSecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// ParseCredential splits "worker-id.secret".
func ParseCredential(c string) (id, secret string, ok bool) {
	id, secret, ok = strings.Cut(c, ".")
	return id, secret, ok && id != "" && secret != ""
}

// CredentialMatches compares in constant time.
func CredentialMatches(stored []byte, secret string) bool {
	return subtle.ConstantTimeCompare(stored, HashSecret(secret)) == 1
}

// JoinTokenUsable checks a join token's limits.
func JoinTokenUsable(t *store.JoinToken, now time.Time) bool {
	if t.ExpiresAt != nil && !now.Before(*t.ExpiresAt) {
		return false
	}
	return t.MaxUses == 0 || t.Uses < t.MaxUses
}

// ParseLimits reads a session's stored limits.
func ParseLimits(s *store.Session) session.StoredLimits {
	var l session.StoredLimits
	_ = json.Unmarshal([]byte(s.LimitsJSON), &l) // written by this program, and only ever narrows behaviour
	return l
}

// ShortHash shortens a checksum for logs.
func ShortHash(h []byte) string {
	if len(h) > 8 {
		h = h[:8]
	}
	return hex.EncodeToString(h)
}

// RepairPlan is what reconciliation does to the database.
type RepairPlan struct {
	DropParts []PartKey
	Rollbacks []Rollback
}

// PartKey names a part.
type PartKey struct {
	Unit string
	Seq  int
}

// Rollback moves a unit back to before a damaged part.
type Rollback struct {
	Unit    string
	To      uint64
	NextSeq int
	Records uint64
	Bytes   int64
}

// CountBefore adds up what a unit's parts before seq hold, so a rollback can restore the
// unit's totals.
func CountBefore(parts []store.Part, unit string, seq int) (records uint64, bytes int64) {
	for i := range parts {
		if parts[i].UnitID == unit && parts[i].Seq < seq {
			records += parts[i].Records
			bytes += parts[i].Bytes
		}
	}
	return records, bytes
}
