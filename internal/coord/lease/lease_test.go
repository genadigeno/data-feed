package lease

import (
	"errors"
	"testing"
	"time"

	"github.com/genadigeno/data-feed/internal/store"
)

var t0 = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

const d = 30 * time.Second

func pending() store.Unit {
	return store.Unit{ID: "u", State: store.UnitPending, RangeStart: 100, RangeEnd: 200, CommittedThrough: 100}
}

func granted(t *testing.T, u store.Unit, worker, id string, at time.Time) (store.Unit, Ref) {
	t.Helper()
	u, err := Grant(u, worker, id, at, d)
	if err != nil {
		t.Fatal(err)
	}
	return u, Ref{UnitID: u.ID, LeaseID: id, WorkerID: worker, FencingToken: u.FencingToken}
}

func TestGrantAdvancesTheToken(t *testing.T) {
	u, r1 := granted(t, pending(), "w1", "l1", t0)
	if u.State != store.UnitLeased || r1.FencingToken != 1 || u.Attempts != 1 || !u.LeaseExpiresAt.Equal(t0.Add(d)) {
		t.Fatalf("%+v", u)
	}
	if _, err := Grant(u, "w2", "l2", t0, d); !errors.Is(err, ErrNotPending) {
		t.Fatalf("second grant: %v", err)
	}
	u = Expire(u)
	u, r2 := granted(t, u, "w2", "l2", t0.Add(time.Minute))
	if r2.FencingToken <= r1.FencingToken {
		t.Fatalf("token did not grow: %d then %d", r1.FencingToken, r2.FencingToken)
	}
}

// W1 holds a lease, is cut off, and it expires. W2 gets the unit. W1's late submission
// must fail, and W2's must succeed.
func TestStaleWorkerIsFenced(t *testing.T) {
	u, stale := granted(t, pending(), "w1", "l1", t0)
	later := t0.Add(d)
	if !Expired(u, t0, true, later, d) {
		t.Fatal("not expired")
	}
	u = Expire(u)
	u, fresh := granted(t, u, "w2", "l2", later)

	if err := Check(u, stale, later.Add(time.Second)); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale: %v", err)
	}
	if err := Check(u, fresh, later.Add(time.Second)); err != nil {
		t.Fatalf("fresh: %v", err)
	}
	if _, err := Renew(u, stale, later, d); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale renew: %v", err)
	}
	if _, err := Complete(u, stale, 200, 0, false, later); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale complete: %v", err)
	}
	if _, err := Release(u, stale, later); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale release: %v", err)
	}
	if _, _, err := FailAttempt(u, stale, "x", true, 5, later); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale fail: %v", err)
	}
}

func TestEachIdentityFieldIsChecked(t *testing.T) {
	u, r := granted(t, pending(), "w1", "l1", t0)
	for name, bad := range map[string]Ref{
		"lease":  {UnitID: r.UnitID, LeaseID: "other", WorkerID: r.WorkerID, FencingToken: r.FencingToken},
		"worker": {UnitID: r.UnitID, LeaseID: r.LeaseID, WorkerID: "other", FencingToken: r.FencingToken},
		"token":  {UnitID: r.UnitID, LeaseID: r.LeaseID, WorkerID: r.WorkerID, FencingToken: r.FencingToken + 1},
	} {
		if err := Check(u, bad, t0); !errors.Is(err, ErrFenced) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := Check(u, r, t0.Add(d)); !errors.Is(err, ErrFenced) {
		t.Errorf("at expiry: %v", err)
	}
	if err := Check(u, r, t0.Add(d-time.Nanosecond)); err != nil {
		t.Errorf("just before expiry: %v", err)
	}
}

func TestRenewExtends(t *testing.T) {
	u, r := granted(t, pending(), "w1", "l1", t0)
	u, err := Renew(u, r, t0.Add(20*time.Second), d)
	if err != nil {
		t.Fatal(err)
	}
	if err := Check(u, r, t0.Add(45*time.Second)); err != nil {
		t.Fatalf("renewal did not extend: %v", err)
	}
	// Once expired, renewal is too late: the unit may be reassigned at any moment.
	if _, err := Renew(u, r, t0.Add(time.Hour), d); !errors.Is(err, ErrFenced) {
		t.Fatalf("renew after expiry: %v", err)
	}
}

// A retry after a lost response must not store the part twice, and a part with the same
// identity but other bytes is a bug to report, not to overwrite.
func TestClassifyPart(t *testing.T) {
	u, _ := granted(t, pending(), "w1", "l1", t0)
	m := PartMeta{Seq: 0, FirstIndex: 100, EndIndex: 150, Records: 50, Bytes: 500, SHA256: "aa"}

	if got := ClassifyPart(u, nil, m); got != PartNew {
		t.Fatalf("first: %d", got)
	}
	u = ApplyPart(u, m)
	if u.CommittedThrough != 150 || u.NextPartSeq != 1 || u.Records != 50 || u.Bytes != 500 {
		t.Fatalf("%+v", u)
	}

	stored := &store.Part{SHA256: "aa"}
	if got := ClassifyPart(u, stored, m); got != PartDuplicate {
		t.Fatalf("retry: %d", got)
	}
	other := m
	other.SHA256 = "bb"
	if got := ClassifyPart(u, stored, other); got != PartConflict {
		t.Fatalf("different content: %d", got)
	}

	cases := map[string]PartMeta{
		"skips a sequence":   {Seq: 2, FirstIndex: 150, EndIndex: 160},
		"repeats a sequence": {Seq: 0, FirstIndex: 150, EndIndex: 160},
		"leaves a gap":       {Seq: 1, FirstIndex: 151, EndIndex: 160},
		"overlaps":           {Seq: 1, FirstIndex: 149, EndIndex: 160},
		"runs past the unit": {Seq: 1, FirstIndex: 150, EndIndex: 201},
		"covers nothing":     {Seq: 1, FirstIndex: 150, EndIndex: 150},
	}
	for name, bad := range cases {
		if got := ClassifyPart(u, nil, bad); got != PartOutOfOrder {
			t.Errorf("%s: %d", name, got)
		}
	}
}

func TestCompleteRules(t *testing.T) {
	base, r := granted(t, pending(), "w1", "l1", t0)
	base = ApplyPart(base, PartMeta{Seq: 0, FirstIndex: 100, EndIndex: 190, Records: 90})

	// Indices after the last part may hold no row: complete still reaches the end.
	u, err := Complete(base, r, 200, 1, false, t0)
	if err != nil {
		t.Fatal(err)
	}
	if u.State != store.UnitCommitted || u.CommittedThrough != 200 || u.RangeEnd != 200 || u.LeaseID != "" {
		t.Fatalf("%+v", u)
	}

	if _, err := Complete(base, r, 200, 2, false, t0); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("wrong part count: %v", err)
	}
	if _, err := Complete(base, r, 195, 1, false, t0); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("ending early unasked: %v", err)
	}
	if _, err := Complete(base, r, 150, 1, true, t0); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("ending before committed data: %v", err)
	}
	if _, err := Complete(base, r, 201, 1, false, t0); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("past the range: %v", err)
	}

	// While finishing, ending early shrinks the unit so the manifest stays contiguous.
	u, err = Complete(base, r, 190, 1, true, t0)
	if err != nil {
		t.Fatal(err)
	}
	if u.RangeEnd != 190 || u.State != store.UnitCommitted {
		t.Fatalf("%+v", u)
	}
}

// Complete and expiry racing are serialized by the write transaction: whichever comes
// second sees the other's result, never both.
func TestCompleteAndExpiryCannotBothWin(t *testing.T) {
	base, r := granted(t, pending(), "w1", "l1", t0)
	base.CommittedThrough = 200
	base.NextPartSeq = 1

	// Expiry first: complete is fenced.
	if _, err := Complete(Expire(base), r, 200, 1, false, t0); !errors.Is(err, ErrFenced) {
		t.Fatalf("complete after expiry: %v", err)
	}
	// Complete first: the unit is no longer leased, so there is nothing to expire.
	done, err := Complete(base, r, 200, 1, false, t0)
	if err != nil {
		t.Fatal(err)
	}
	if Expired(done, t0, false, t0.Add(time.Hour), d) {
		t.Fatal("a committed unit expired")
	}
}

// Stop immediate during an upload: revoking first means the upload is fenced, so no part
// from that attempt becomes visible.
func TestRevokeFencesSubmissionsInFlight(t *testing.T) {
	u, r := granted(t, pending(), "w1", "l1", t0)
	revoked := Revoke(u)
	if revoked.FencingToken != r.FencingToken+1 || revoked.State != store.UnitPending {
		t.Fatalf("%+v", revoked)
	}
	if err := Check(revoked, r, t0); !errors.Is(err, ErrFenced) {
		t.Fatalf("after revoke: %v", err)
	}
	if again := Revoke(revoked); again.FencingToken != revoked.FencingToken {
		t.Fatal("revoking an unleased unit changed its token")
	}
}

func TestReleaseKeepsProgressAndResumes(t *testing.T) {
	u, r := granted(t, pending(), "w1", "l1", t0)
	u = ApplyPart(u, PartMeta{Seq: 0, FirstIndex: 100, EndIndex: 150, Records: 50, Bytes: 1})
	u, err := Release(u, r, t0)
	if err != nil {
		t.Fatal(err)
	}
	if u.State != store.UnitPending || u.CommittedThrough != 150 || u.NextPartSeq != 1 {
		t.Fatalf("%+v", u)
	}

	// The next attempt continues where the last one stopped; earlier parts stay valid
	// because their content is deterministic.
	u, r2 := granted(t, u, "w2", "l2", t0)
	if r2.FencingToken != 2 {
		t.Fatalf("token %d", r2.FencingToken)
	}
	if got := ClassifyPart(u, nil, PartMeta{Seq: 1, FirstIndex: 150, EndIndex: 200}); got != PartNew {
		t.Fatalf("continuing part: %d", got)
	}
}

func TestFailAttempt(t *testing.T) {
	u, r := granted(t, pending(), "w1", "l1", t0)

	retried, final, err := FailAttempt(u, r, "boom", true, 3, t0)
	if err != nil || final || retried.State != store.UnitPending || retried.LastError != "boom" {
		t.Fatalf("retryable: %+v %v %v", retried, final, err)
	}

	failed, final, err := FailAttempt(u, r, "boom", false, 3, t0)
	if err != nil || !final || failed.State != store.UnitFailed {
		t.Fatalf("not retryable: %+v %v %v", failed, final, err)
	}

	// Out of attempts.
	last := u
	last.Attempts = 3
	failed, final, err = FailAttempt(last, r, "boom", true, 3, t0)
	if err != nil || !final || failed.State != store.UnitFailed {
		t.Fatalf("exhausted: %+v %v %v", failed, final, err)
	}
}

func TestExpiredNeedsBothClocks(t *testing.T) {
	u, _ := granted(t, pending(), "w1", "l1", t0)
	cases := []struct {
		name        string
		now         time.Time
		lastRenewal time.Time
		hasRenewal  bool
		want        bool
	}{
		{"before expiry", t0.Add(10 * time.Second), t0, true, false},
		{"at expiry with an old renewal", t0.Add(d), t0, true, true},
		// A forward jump of the wall clock: the persisted time passed, but by the monotonic
		// clock the worker renewed moments ago. A real Time keeps both readings; a fresh
		// renewal here models that.
		{"wall clock jumped", t0.Add(time.Hour), t0.Add(time.Hour - time.Second), true, false},
		// After a restart there is no in-memory renewal: the persisted time decides.
		{"after a restart", t0.Add(d), time.Time{}, false, true},
		{"after a restart, before expiry", t0.Add(time.Second), time.Time{}, false, false},
	}
	for _, c := range cases {
		if got := Expired(u, c.lastRenewal, c.hasRenewal, c.now, d); got != c.want {
			t.Errorf("%s: got %v", c.name, got)
		}
	}
	if Expired(Expire(u), t0, false, t0.Add(time.Hour), d) {
		t.Error("a pending unit expired")
	}
}

func TestExpirePreservesProgressAndToken(t *testing.T) {
	u, _ := granted(t, pending(), "w1", "l1", t0)
	u.CommittedThrough, u.NextPartSeq = 150, 2
	e := Expire(u)
	if e.State != store.UnitPending || e.LeaseID != "" || e.WorkerID != "" || e.CommittedThrough != 150 || e.NextPartSeq != 2 || e.FencingToken != u.FencingToken {
		t.Fatalf("%+v", e)
	}
}

func TestCancelAndRollback(t *testing.T) {
	u, _ := granted(t, pending(), "w1", "l1", t0)
	c := Cancel(u)
	if c.State != store.UnitCancelled || c.FencingToken != u.FencingToken+1 || c.LeaseID != "" {
		t.Fatalf("%+v", c)
	}
	done := u
	done.State = store.UnitCommitted
	if Cancel(done).State != store.UnitCommitted {
		t.Fatal("a committed unit was cancelled")
	}

	r := Rollback(done, 120, 1, 20, 200)
	if r.State != store.UnitPending || r.CommittedThrough != 120 || r.NextPartSeq != 1 || r.Records != 20 || r.Bytes != 200 {
		t.Fatalf("%+v", r)
	}
	if got := Rollback(c, 100, 0, 0, 0); got.State != store.UnitCancelled {
		t.Fatalf("a cancelled unit came back: %+v", got)
	}
}

func TestOrphanDetection(t *testing.T) {
	o := NewOrphanTracker(2)
	lost := store.Unit{ID: "a", LeaseID: "lost", GrantedAt: t0}
	known := store.Unit{ID: "b", LeaseID: "known", GrantedAt: t0}
	held := []store.Unit{lost, known}
	reported := map[string]bool{"known": true}
	beat := func(i int) []store.Unit {
		return o.Observe("w", held, reported, t0.Add(time.Duration(i)*5*time.Second))
	}

	// The first heartbeat after the grant only sets the baseline.
	if got := beat(1); len(got) != 0 {
		t.Fatalf("first: %+v", got)
	}
	if got := beat(2); len(got) != 0 {
		t.Fatalf("one miss: %+v", got)
	}
	got := beat(3)
	if len(got) != 1 || got[0].LeaseID != "lost" {
		t.Fatalf("two misses: %+v", got)
	}
}

func TestOrphanToleratesGrantsInFlight(t *testing.T) {
	o := NewOrphanTracker(1)
	o.Observe("w", nil, nil, t0)
	// Granted after the previous heartbeat: its response may still be on the way.
	fresh := store.Unit{LeaseID: "new", GrantedAt: t0.Add(time.Second)}
	if got := o.Observe("w", []store.Unit{fresh}, nil, t0.Add(5*time.Second)); len(got) != 0 {
		t.Fatalf("fresh grant revoked: %+v", got)
	}
	// By the next heartbeat the worker has had a whole interval to report it.
	if got := o.Observe("w", []store.Unit{fresh}, nil, t0.Add(10*time.Second)); len(got) != 1 {
		t.Fatalf("not revoked: %+v", got)
	}
}

func TestOrphanResetsWhenReported(t *testing.T) {
	o := NewOrphanTracker(2)
	u := store.Unit{LeaseID: "l", GrantedAt: t0}
	o.Observe("w", nil, nil, t0.Add(time.Second))
	o.Observe("w", []store.Unit{u}, nil, t0.Add(2*time.Second))                        // one miss
	o.Observe("w", []store.Unit{u}, map[string]bool{"l": true}, t0.Add(3*time.Second)) // seen: reset
	if got := o.Observe("w", []store.Unit{u}, nil, t0.Add(4*time.Second)); len(got) != 0 {
		t.Fatalf("revoked after a reset: %+v", got)
	}
	o.Forget("w")
	if got := o.Observe("w", []store.Unit{u}, nil, t0.Add(5*time.Second)); len(got) != 0 {
		t.Fatalf("baseline forgotten, yet revoked: %+v", got)
	}
}
