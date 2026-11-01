package coord

import (
	"testing"
	"time"

	"github.com/genadigeno/data-feed/internal/session"
	"github.com/genadigeno/data-feed/internal/store"
	"github.com/genadigeno/data-feed/internal/wire"
)

var epoch = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func caps() *wire.Capabilities {
	return &wire.Capabilities{
		EngineVersions: []int{1}, Formats: []string{"ndjson"}, Strategies: []string{"random"}, Arch: "amd64",
		MaxMemoryBytes: 1000, Labels: map[string]string{"zone": "a"},
	}
}

func cand() Candidate {
	return Candidate{MaxWorkers: 2, Engine: 1, Format: "ndjson", Strategy: "random"}
}

func TestEligible(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Candidate)
		want   bool
	}{
		{"matches", func(*Candidate) {}, true},
		{"engine", func(c *Candidate) { c.Engine = 2 }, false},
		{"format", func(c *Candidate) { c.Format = "xml" }, false},
		{"strategy", func(c *Candidate) { c.Strategy = "relational" }, false},
		{"same architecture", func(c *Candidate) { c.Arch = "amd64" }, true},
		{"other architecture", func(c *Candidate) { c.Arch = "arm64" }, false},
		{"label", func(c *Candidate) { c.Selector = map[string]string{"zone": "a"} }, true},
		{"wrong label", func(c *Candidate) { c.Selector = map[string]string{"zone": "b"} }, false},
		{"missing label", func(c *Candidate) { c.Selector = map[string]string{"gpu": "yes"} }, false},
		{"memory within budget", func(c *Candidate) { c.UnitMemory = 800 }, true},
		{"memory over budget", func(c *Candidate) { c.UnitMemory = 801 }, false},
	}
	for _, tc := range cases {
		c := cand()
		tc.change(&c)
		if got := Eligible(&c, caps()); got != tc.want {
			t.Errorf("%s: got %v", tc.name, got)
		}
	}
}

func TestPickTakesTurnsAndSkips(t *testing.T) {
	cands := []Candidate{cand(), cand(), cand()}
	if got := Pick(cands, caps(), 1); got != 1 {
		t.Fatalf("rotation 1: %d", got)
	}
	if got := Pick(cands, caps(), 4); got != 1 {
		t.Fatalf("rotation wraps: %d", got)
	}
	cands[1].Empty = true
	if got := Pick(cands, caps(), 1); got != 2 {
		t.Fatalf("skips empty: %d", got)
	}
	cands[2].Leased = 2
	if got := Pick(cands, caps(), 1); got != 0 {
		t.Fatalf("skips full: %d", got)
	}
	cands[0].Format = "xml"
	if got := Pick(cands, caps(), 1); got != -1 {
		t.Fatalf("nothing eligible: %d", got)
	}
	if got := Pick(nil, caps(), 3); got != -1 {
		t.Fatalf("empty: %d", got)
	}
}

func TestPickIsFairAcrossSessions(t *testing.T) {
	cands := []Candidate{cand(), cand(), cand()}
	counts := make([]int, len(cands))
	for i := range 300 {
		counts[Pick(cands, caps(), i)]++
	}
	for i, n := range counts {
		if n != 100 {
			t.Fatalf("session %d got %d of 300", i, n)
		}
	}
}

func TestUnitRate(t *testing.T) {
	if got := UnitRate(0, 3); got != 0 {
		t.Fatal(got)
	}
	if got := UnitRate(90, 3); got != 30 {
		t.Fatal(got)
	}
	if got := UnitRate(90, 0); got != 90 {
		t.Fatal(got)
	}
}

func TestDirectiveFor(t *testing.T) {
	cases := []struct {
		s    store.Session
		want string
	}{
		{store.Session{State: session.Running}, ""},
		{store.Session{State: session.Running, Finishing: true}, wire.DirectiveFinish},
		{store.Session{State: session.Pausing}, wire.DirectiveRelease},
		{store.Session{State: session.Stopping}, wire.DirectiveFinish},
		{store.Session{State: session.Stopping, StopImmediate: true}, wire.DirectiveCancel},
		{store.Session{State: session.Failed}, wire.DirectiveCancel},
	}
	for _, tc := range cases {
		d, ok := DirectiveFor(&tc.s, "u")
		if ok != (tc.want != "") || d.Action != tc.want {
			t.Errorf("%+v: got %+v", tc.s.State, d)
		}
	}
}

func TestShouldFinish(t *testing.T) {
	started := epoch
	running := func() *store.Session { return &store.Session{State: session.Running, StartedAt: &started} }

	if ShouldFinish(running(), &session.StoredLimits{}, epoch.Add(time.Hour)) {
		t.Fatal("no limits")
	}
	limit := &session.StoredLimits{DurationMS: 60000}
	if ShouldFinish(running(), limit, epoch.Add(59*time.Second)) {
		t.Fatal("before the deadline")
	}
	if !ShouldFinish(running(), limit, epoch.Add(60*time.Second)) {
		t.Fatal("at the deadline")
	}
	paused := running()
	paused.PausedTotal = 30 * time.Second
	if ShouldFinish(paused, limit, epoch.Add(80*time.Second)) {
		t.Fatal("time paused does not count")
	}
	full := running()
	full.Bytes = 100
	if !ShouldFinish(full, &session.StoredLimits{MaxBytes: 100}, epoch) {
		t.Fatal("byte limit")
	}
	done := running()
	done.Finishing = true
	if ShouldFinish(done, limit, epoch.Add(time.Hour)) {
		t.Fatal("already finishing")
	}
	other := running()
	other.State = session.Paused
	if ShouldFinish(other, limit, epoch.Add(time.Hour)) {
		t.Fatal("not running")
	}
}

func TestDrainEvent(t *testing.T) {
	cases := []struct {
		state           string
		finishing       bool
		leased, pending int64
		exhausted       bool
		want            string
	}{
		{session.Pausing, false, 1, 0, false, ""},
		{session.Pausing, false, 0, 5, false, session.EvDrained},
		{session.Stopping, false, 1, 0, false, ""},
		{session.Stopping, false, 0, 0, false, session.EvDrained},
		{session.Running, false, 0, 0, false, ""}, // more units can still be planned
		{session.Running, false, 1, 0, true, ""},
		{session.Running, false, 0, 1, true, ""},
		{session.Running, false, 0, 0, true, session.EvDone},
		{session.Running, true, 1, 0, false, ""},
		{session.Running, true, 0, 3, false, session.EvDone},
		{session.Paused, false, 0, 0, true, ""},
	}
	for _, tc := range cases {
		s := &store.Session{State: tc.state, Finishing: tc.finishing}
		if got := DrainEvent(s, tc.leased, tc.pending, tc.exhausted); got != tc.want {
			t.Errorf("%+v: got %q", tc, got)
		}
	}
}

func TestCredentials(t *testing.T) {
	id, secret, ok := ParseCredential("wrk_1.abc")
	if !ok || id != "wrk_1" || secret != "abc" {
		t.Fatal(id, secret, ok)
	}
	for _, bad := range []string{"", "abc", ".abc", "wrk."} {
		if _, _, ok := ParseCredential(bad); ok {
			t.Errorf("accepted %q", bad)
		}
	}
	stored := HashSecret("abc")
	if !CredentialMatches(stored, "abc") || CredentialMatches(stored, "abd") || CredentialMatches(nil, "abc") {
		t.Fatal("comparison is wrong")
	}
}

func TestJoinTokenUsable(t *testing.T) {
	later := epoch.Add(time.Hour)
	cases := []struct {
		t    store.JoinToken
		want bool
	}{
		{store.JoinToken{}, true},
		{store.JoinToken{ExpiresAt: &later}, true},
		{store.JoinToken{ExpiresAt: &epoch}, false},
		{store.JoinToken{MaxUses: 2, Uses: 1}, true},
		{store.JoinToken{MaxUses: 2, Uses: 2}, false},
	}
	for i, tc := range cases {
		if got := JoinTokenUsable(&tc.t, epoch); got != tc.want {
			t.Errorf("case %d: got %v", i, got)
		}
	}
}

func TestCountBefore(t *testing.T) {
	parts := []store.Part{
		{UnitID: "a", Seq: 0, Records: 10, Bytes: 100},
		{UnitID: "a", Seq: 1, Records: 20, Bytes: 200},
		{UnitID: "a", Seq: 2, Records: 40, Bytes: 400},
		{UnitID: "b", Seq: 0, Records: 80, Bytes: 800},
	}
	records, bytes := CountBefore(parts, "a", 2)
	if records != 30 || bytes != 300 {
		t.Fatal(records, bytes)
	}
}

func TestManifestForIsOrderedAndTotalled(t *testing.T) {
	m := ManifestFor("ndjson", []store.Part{
		{Stream: "main", UnitSeq: 1, Seq: 0, Records: 1, Bytes: 10, RelPath: "s/main/b.ndjson.gz"},
		{Stream: "main", UnitSeq: 0, Seq: 0, Records: 2, Bytes: 20, RelPath: "s/main/a.ndjson.gz"},
	})
	if m.Records != 3 || m.Bytes != 30 || m.Compression != "gzip" {
		t.Fatalf("%+v", m)
	}
	if m.Parts[0].UnitSeq != 0 {
		t.Fatalf("order: %+v", m.Parts)
	}
}
