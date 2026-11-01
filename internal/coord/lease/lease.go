// Package lease holds the decisions behind leases and fencing, as pure functions over a
// unit's row. The SQL around them only loads and stores: every race scenario can be
// replayed here with no database. See docs/01-architecture/lease-and-fencing.md.
package lease

import (
	"errors"
	"fmt"
	"time"

	"github.com/genadigeno/data-feed/internal/store"
)

// Errors a submission can get. Stale workers see these, and must stop.
var (
	ErrFenced     = errors.New("lease is no longer current")
	ErrNotPending = errors.New("unit is not pending")
	ErrIncomplete = errors.New("parts do not cover the range")
)

// Ref identifies one lease.
type Ref struct {
	UnitID, LeaseID, WorkerID string
	FencingToken              uint64
}

// Grant gives a pending unit to a worker. The token increases on every grant, so any
// older holder is fenced out whatever it believes.
func Grant(u store.Unit, worker, leaseID string, now time.Time, d time.Duration) (store.Unit, error) {
	if u.State != store.UnitPending {
		return u, ErrNotPending
	}
	u.State = store.UnitLeased
	u.FencingToken++
	u.LeaseID = leaseID
	u.WorkerID = worker
	u.LeaseExpiresAt = now.Add(d)
	u.GrantedAt = now
	u.Attempts++
	return u, nil
}

// Check validates a submission: the lease must be the current one, and unexpired.
func Check(u store.Unit, r Ref, now time.Time) error {
	if u.State != store.UnitLeased || u.LeaseID != r.LeaseID || u.WorkerID != r.WorkerID || u.FencingToken != r.FencingToken {
		return ErrFenced
	}
	if !now.Before(u.LeaseExpiresAt) {
		return ErrFenced
	}
	return nil
}

// Renew extends a current lease.
func Renew(u store.Unit, r Ref, now time.Time, d time.Duration) (store.Unit, error) {
	if err := Check(u, r, now); err != nil {
		return u, err
	}
	u.LeaseExpiresAt = now.Add(d)
	return u, nil
}

func clearLease(u store.Unit) store.Unit {
	u.LeaseID = ""
	u.WorkerID = ""
	u.LeaseExpiresAt = time.Time{}
	return u
}

// Expired decides whether the reaper may take a lease back. Both clocks must agree: the
// persisted expiry has passed, and, if this process renewed the lease, enough monotonic
// time has elapsed since. A jump of the wall clock therefore cannot expire leases en masse.
func Expired(u store.Unit, lastRenewal time.Time, hasRenewal bool, now time.Time, d time.Duration) bool {
	if u.State != store.UnitLeased || now.Before(u.LeaseExpiresAt) {
		return false
	}
	return !hasRenewal || now.Sub(lastRenewal) >= d
}

// Expire returns an expired unit to the queue.
func Expire(u store.Unit) store.Unit {
	u = clearLease(u)
	u.State = store.UnitPending
	return u
}

// Revoke takes a lease away at once. The token increases, so a submission already on its
// way is fenced even though no new lease exists yet.
func Revoke(u store.Unit) store.Unit {
	if u.State != store.UnitLeased {
		return u
	}
	u = clearLease(u)
	u.FencingToken++
	u.State = store.UnitPending
	return u
}

// Cancel withdraws a unit that will not run, as when a session stops.
func Cancel(u store.Unit) store.Unit {
	if u.State == store.UnitCommitted || u.State == store.UnitFailed {
		return u
	}
	u = clearLease(u)
	u.FencingToken++
	u.State = store.UnitCancelled
	return u
}

// PartMeta is what a worker says about a part it is submitting.
type PartMeta struct {
	Seq                  int
	FirstIndex, EndIndex uint64
	Records              uint64
	Bytes                int64
	SHA256               string
}

// Outcome classifies a part submission.
type Outcome uint8

const (
	// PartNew should be stored.
	PartNew Outcome = iota
	// PartDuplicate was already committed with the same content: answer success again.
	PartDuplicate
	// PartConflict was committed with other content: a determinism bug or a damaged spool.
	PartConflict
	// PartOutOfOrder does not continue the unit.
	PartOutOfOrder
)

// ClassifyPart decides what to do with a part, given the row for that sequence, if any.
func ClassifyPart(u store.Unit, existing *store.Part, m PartMeta) Outcome {
	if existing != nil {
		if existing.SHA256 == m.SHA256 {
			return PartDuplicate
		}
		return PartConflict
	}
	if m.Seq != u.NextPartSeq || m.FirstIndex != u.CommittedThrough || m.EndIndex <= m.FirstIndex || m.EndIndex > u.RangeEnd {
		return PartOutOfOrder
	}
	return PartNew
}

// ApplyPart records a stored part on the unit.
func ApplyPart(u store.Unit, m PartMeta) store.Unit {
	u.CommittedThrough = m.EndIndex
	u.NextPartSeq = m.Seq + 1
	u.Records += m.Records
	u.Bytes += m.Bytes
	return u
}

// Complete finishes a unit. Parts must reach end, or end must follow only empty indices,
// which the worker confirms by naming how many parts it sent. Ending below the unit's
// range is accepted only while finishing, as for a graceful stop or a deadline.
func Complete(u store.Unit, r Ref, end uint64, parts int, finishing bool, now time.Time) (store.Unit, error) {
	if err := Check(u, r, now); err != nil {
		return u, err
	}
	if parts != u.NextPartSeq {
		return u, fmt.Errorf("%w: worker sent %d parts, %d were committed", ErrIncomplete, parts, u.NextPartSeq)
	}
	if end < u.CommittedThrough || end > u.RangeEnd {
		return u, fmt.Errorf("%w: end %d is outside %d..%d", ErrIncomplete, end, u.CommittedThrough, u.RangeEnd)
	}
	if end < u.RangeEnd {
		if !finishing {
			return u, fmt.Errorf("%w: ended at %d of %d without being asked to finish", ErrIncomplete, end, u.RangeEnd)
		}
		u.RangeEnd = end
	}
	u = clearLease(u)
	u.CommittedThrough = end
	u.State = store.UnitCommitted
	return u, nil
}

// Release gives a unit back, as for a pause or a worker draining. Progress stays: the next
// lease continues from what was committed.
func Release(u store.Unit, r Ref, now time.Time) (store.Unit, error) {
	if err := Check(u, r, now); err != nil {
		return u, err
	}
	u = clearLease(u)
	u.State = store.UnitPending
	return u, nil
}

// FailAttempt records a failed attempt. It returns true if the unit has failed for good,
// which fails its session.
func FailAttempt(u store.Unit, r Ref, message string, retryable bool, maxAttempts int, now time.Time) (store.Unit, bool, error) {
	if err := Check(u, r, now); err != nil {
		return u, false, err
	}
	u = clearLease(u)
	u.LastError = message
	if !retryable || u.Attempts >= maxAttempts {
		u.State = store.UnitFailed
		return u, true, nil
	}
	u.State = store.UnitPending
	return u, false, nil
}

// Rollback moves a unit back to a committed position, for repair after a part file was
// lost. Whatever the unit's state was, it is pending again unless it had been cancelled.
func Rollback(u store.Unit, to uint64, nextSeq int, records uint64, bytes int64) store.Unit {
	u.CommittedThrough = to
	u.NextPartSeq = nextSeq
	u.Records = records
	u.Bytes = bytes
	if u.State == store.UnitCommitted || u.State == store.UnitFailed {
		u.State = store.UnitPending
	}
	return u
}

// OrphanTracker notices leases a worker does not know it holds: the grant's response was
// lost on the network. A lease granted before the worker's previous heartbeat, and absent
// from enough consecutive heartbeats, is orphaned. It keeps no clock of its own.
type OrphanTracker struct {
	limit  int
	misses map[string]int // lease ID → consecutive heartbeats without it
	last   map[string]time.Time
}

// NewOrphanTracker returns a tracker revoking after limit missed heartbeats.
func NewOrphanTracker(limit int) *OrphanTracker {
	return &OrphanTracker{limit: limit, misses: map[string]int{}, last: map[string]time.Time{}}
}

// Observe takes one heartbeat of a worker: the leases the database says it holds, and the
// leases it reported. It returns the units whose lease is orphaned.
func (o *OrphanTracker) Observe(worker string, held []store.Unit, reported map[string]bool, now time.Time) []store.Unit {
	previous, seen := o.last[worker]
	o.last[worker] = now

	var orphans []store.Unit
	for _, u := range held {
		if reported[u.LeaseID] {
			delete(o.misses, u.LeaseID)
			continue
		}
		// A grant newer than the previous heartbeat may simply not have arrived yet.
		if !seen || u.GrantedAt.After(previous) {
			continue
		}
		o.misses[u.LeaseID]++
		if o.misses[u.LeaseID] >= o.limit {
			delete(o.misses, u.LeaseID)
			orphans = append(orphans, u)
		}
	}
	return orphans
}

// Forget drops a worker's state.
func (o *OrphanTracker) Forget(worker string) { delete(o.last, worker) }
