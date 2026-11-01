package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/coord"
	"github.com/genadigeno/data-feed/internal/session"
	"github.com/genadigeno/data-feed/internal/store"
)

var at = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

func testServer() *server {
	cfg := config.Defaults(config.EnvDevelopment)
	return &server{o: Options{Config: &cfg}}
}

func TestOpenAPIDocumentsExactlyTheRoutes(t *testing.T) {
	var doc struct {
		OpenAPI string                    `yaml:"openapi"`
		Paths   map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(openAPIDocument, &doc); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(doc.OpenAPI, "3.1") {
		t.Fatalf("openapi %q", doc.OpenAPI)
	}
	var documented []string
	for path, ops := range doc.Paths {
		for method := range ops {
			if method != "parameters" {
				documented = append(documented, strings.ToUpper(method)+" "+path)
			}
		}
	}
	var served []string
	for _, rt := range testServer().routes() {
		served = append(served, rt.Method+" "+rt.Path)
	}
	slices.Sort(documented)
	slices.Sort(served)
	for _, r := range served {
		if !slices.Contains(documented, r) {
			t.Errorf("not documented: %s", r)
		}
	}
	for _, r := range documented {
		if !slices.Contains(served, r) {
			t.Errorf("documented but not served: %s", r)
		}
	}
}

func TestRoutesAreUniqueAndHandled(t *testing.T) {
	seen := map[string]bool{}
	for _, rt := range testServer().routes() {
		key := rt.Method + " " + rt.Path
		if seen[key] {
			t.Errorf("twice: %s", key)
		}
		seen[key] = true
		if rt.Handler == nil {
			t.Errorf("no handler: %s", key)
		}
	}
	// The server would panic at startup on a conflict; this finds it sooner.
	mux := http.NewServeMux()
	for key := range seen {
		mux.HandleFunc(strings.Replace(key, " ", " "+prefix, 1), func(http.ResponseWriter, *http.Request) {})
	}
}

func TestChangingRoutesNeedAtLeastOperator(t *testing.T) {
	open := map[string]bool{
		"POST /sessions/validate": true, "POST /schemas/validate": true, "POST /schemas/{id}/preview": true,
		"POST /auth/login": true, "POST /auth/logout": true, // what signing in and out needs
	}
	for _, rt := range testServer().routes() {
		key := rt.Method + " " + rt.Path
		if rt.Method == "GET" || open[key] {
			continue
		}
		if !RoleAllows(rt.Role, RoleOperator) {
			t.Errorf("%s is allowed for %q", key, rt.Role)
		}
	}
	for _, rt := range testServer().routes() {
		if strings.HasPrefix(rt.Path, "/tokens") || rt.Path == "/worker-tokens" {
			if rt.Role != RoleAdmin {
				t.Errorf("%s %s needs admin", rt.Method, rt.Path)
			}
		}
	}
}

func TestOnlyHealthAndDocsAreOpen(t *testing.T) {
	s := testServer()
	s.o.Config.API.MetricsRequireAuth = true
	want := []string{"/auth/login", "/auth/logout", "/health", "/openapi.yaml", "/ready"}
	var got []string
	for _, rt := range s.routes() {
		if rt.Role == "" {
			got = append(got, rt.Path)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("open routes: %v", got)
	}
	s.o.Config.API.MetricsRequireAuth = false
	if s.metricsRole() != "" {
		t.Fatal("metrics stayed closed")
	}
}

func TestAllowedMethods(t *testing.T) {
	routes := testServer().routes()
	if got := AllowedMethods(routes, "/health"); !slices.Equal(got, []string{"GET"}) {
		t.Fatal(got)
	}
	if got := AllowedMethods(routes, "/sessions/abc"); !slices.Equal(got, []string{"DELETE", "GET"}) {
		t.Fatal(got)
	}
	if got := AllowedMethods(routes, "/sessions/abc/nothing"); len(got) != 0 {
		t.Fatal(got)
	}
	if got := AllowedMethods(routes, "/nothing"); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestHostAllowed(t *testing.T) {
	loop, open := "127.0.0.1:8080", "0.0.0.0:8080"
	for host, want := range map[string]bool{
		"127.0.0.1:8080": true, "localhost:8080": true, "LOCALHOST": true, "[::1]:8080": true, "ui.localhost:8080": true,
		"evil.example:8080": false, "evil.example": false, "10.0.0.5:8080": false, "localhost.evil.example": false, "": false,
	} {
		if got := HostAllowed(loop, host); got != want {
			t.Errorf("%q: %v", host, got)
		}
	}
	if !HostAllowed(open, "anything.example") {
		t.Fatal("a server on the network can have any name")
	}
}

func TestCrossOrigin(t *testing.T) {
	host := "127.0.0.1:8080"
	cases := []struct {
		method, origin string
		want           bool
	}{
		{"POST", "", false}, // not a browser
		{"POST", "http://127.0.0.1:8080", false},
		{"POST", "https://evil.example", true},
		{"POST", "null", true}, // a sandboxed frame
		{"DELETE", "http://127.0.0.1:9999", true},
		{"GET", "https://evil.example", false}, // it can't read the answer
	}
	for _, tc := range cases {
		if got := CrossOrigin(tc.method, tc.origin, host); got != tc.want {
			t.Errorf("%+v: %v", tc, got)
		}
	}
}

func TestClientIP(t *testing.T) {
	if ClientIP("10.0.0.1:5000") != "10.0.0.1" || ClientIP("[::1]:80") != "::1" || ClientIP("weird") != "weird" {
		t.Fatal("ClientIP")
	}
}

func TestRoles(t *testing.T) {
	cases := []struct {
		have, need string
		want       bool
	}{
		{RoleViewer, RoleViewer, true},
		{RoleViewer, RoleOperator, false},
		{RoleOperator, RoleViewer, true},
		{RoleOperator, RoleAdmin, false},
		{RoleAdmin, RoleOperator, true},
		{"", RoleViewer, false},
		{"root", RoleViewer, false},
	}
	for _, tc := range cases {
		if got := RoleAllows(tc.have, tc.need); got != tc.want {
			t.Errorf("%q needing %q: %v", tc.have, tc.need, got)
		}
	}
	if ValidRole("root") || !ValidRole(RoleAdmin) {
		t.Fatal("ValidRole")
	}
}

func TestTokens(t *testing.T) {
	a, b := NewToken(), NewToken()
	if a == b || !strings.HasPrefix(a, TokenPrefix) {
		t.Fatal(a, b)
	}
	if bytes.Equal(HashToken(a), HashToken(b)) {
		t.Fatal("same hash")
	}
	later, earlier := at.Add(time.Hour), at.Add(-time.Hour)
	cases := []struct {
		t    store.APIToken
		want bool
	}{
		{store.APIToken{}, true},
		{store.APIToken{ExpiresAt: &later}, true},
		{store.APIToken{ExpiresAt: &earlier}, false},
		{store.APIToken{ExpiresAt: &at}, false},
		{store.APIToken{RevokedAt: &earlier}, false},
	}
	for i, tc := range cases {
		if got := TokenUsable(&tc.t, at); got != tc.want {
			t.Errorf("case %d: %v", i, got)
		}
	}
}

func TestBearer(t *testing.T) {
	r, _ := http.NewRequest("GET", "/", nil)
	if token, _ := bearer(r); token != "" {
		t.Fatal("found a token in nothing")
	}
	r.AddCookie(&http.Cookie{Name: SessionCookie, Value: "from-cookie"})
	if token, cookie := bearer(r); token != "from-cookie" || !cookie {
		t.Fatal("cookie")
	}
	r.Header.Set("Authorization", "Bearer from-header")
	if token, cookie := bearer(r); token != "from-header" || cookie {
		t.Fatal("the header goes first")
	}
	r.Header.Set("Authorization", "Basic abc")
	if token, _ := bearer(r); token != "from-cookie" {
		t.Fatal("other schemes are not tokens")
	}
}

func TestCrossSiteRisk(t *testing.T) {
	cases := []struct {
		method     string
		fromCookie bool
		header     string
		want       bool
	}{
		{"POST", true, "", true},
		{"DELETE", true, "", true},
		{"PUT", true, "", true},
		{"POST", true, "1", false},
		{"GET", true, "", false},
		{"HEAD", true, "", false},
		{"POST", false, "", false}, // a token in a header can't be sent by another site
	}
	for _, tc := range cases {
		if got := CrossSiteRisk(tc.method, tc.fromCookie, tc.header); got != tc.want {
			t.Errorf("%+v: %v", tc, got)
		}
	}
}

func TestSessionCookieIsLockedDown(t *testing.T) {
	c := sessionCookie("v", time.Hour, true)
	if !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteStrictMode || c.MaxAge != 3600 || c.Path != "/" {
		t.Fatalf("%+v", c)
	}
	if sessionCookie("v", time.Hour, false).Secure {
		t.Fatal("Secure over plain HTTP would keep the browser from sending it")
	}
	if gone := sessionCookie("", -time.Hour, false); gone.MaxAge >= 0 {
		t.Fatalf("%+v", gone)
	}
}

func TestLimiterIsPerCaller(t *testing.T) {
	l := newLimiters(1, 2)
	for i := range 2 {
		if ok, _ := l.allow("a", at); !ok {
			t.Fatalf("burst %d refused", i)
		}
	}
	ok, wait := l.allow("a", at)
	if ok || wait <= 0 {
		t.Fatalf("%v %v", ok, wait)
	}
	if ok, _ := l.allow("b", at); !ok {
		t.Fatal("one caller limited another")
	}
	if ok, _ := l.allow("a", at.Add(time.Second)); !ok {
		t.Fatal("did not refill")
	}
	off := newLimiters(0, 0)
	if ok, _ := off.allow("a", at); !ok {
		t.Fatal("limiting is off")
	}
}

func TestIdempotencyDecision(t *testing.T) {
	rec := &store.IdempotencyRecord{RequestHash: []byte{1}, CreatedAt: at}
	ttl := time.Hour
	if got := IdempotencyDecision(nil, []byte{1}, at, ttl); got != idempotencyRun {
		t.Fatal("first", got)
	}
	if got := IdempotencyDecision(rec, []byte{1}, at.Add(time.Minute), ttl); got != idempotencyReplay {
		t.Fatal("same", got)
	}
	if got := IdempotencyDecision(rec, []byte{2}, at.Add(time.Minute), ttl); got != idempotencyMismatch {
		t.Fatal("different", got)
	}
	if got := IdempotencyDecision(rec, []byte{2}, at.Add(ttl), ttl); got != idempotencyRun {
		t.Fatal("expired", got)
	}
}

func TestKeyLocksClearUp(t *testing.T) {
	var k keyLocks
	unlock := k.lock("a")
	unlock()
	if len(k.locks) != 0 {
		t.Fatal("kept a lock")
	}
}

func TestCursor(t *testing.T) {
	c := EncodeCursor("ses_123")
	if got, err := DecodeCursor(c); err != nil || got != "ses_123" {
		t.Fatal(got, err)
	}
	if got, err := DecodeCursor(""); err != nil || got != "" {
		t.Fatal(got, err)
	}
	if _, err := DecodeCursor("!!!"); err == nil {
		t.Fatal("accepted garbage")
	}
}

func TestPageLimit(t *testing.T) {
	if n, _ := PageLimit("", 50, 500); n != 50 {
		t.Fatal(n)
	}
	if n, _ := PageLimit("9999", 50, 500); n != 500 {
		t.Fatal(n)
	}
	for _, bad := range []string{"0", "-1", "x"} {
		if _, err := PageLimit(bad, 50, 500); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestMakePage(t *testing.T) {
	id := func(s *string) string { return *s }
	same := func(s *string) string { return *s }
	p := MakePage([]string{"a", "b", "c"}, 2, id, same)
	if len(p.Items) != 2 || p.NextCursor != EncodeCursor("b") {
		t.Fatalf("%+v", p)
	}
	p = MakePage([]string{"a", "b"}, 2, id, same)
	if len(p.Items) != 2 || p.NextCursor != "" {
		t.Fatalf("%+v", p)
	}
	p = MakePage(nil, 2, id, same)
	data, _ := json.Marshal(p)
	if !strings.Contains(string(data), `"items":[]`) {
		t.Fatalf("an empty page must have an empty list: %s", data)
	}
}

func TestFilterFromQuery(t *testing.T) {
	r := &http.Request{URL: &url.URL{RawQuery: "state=RUNNING,PAUSED&mode=stream&q=abc&sort=created_at"}}
	f, err := SessionFilterFrom(r)
	if err != nil || len(f.States) != 2 || f.Mode != "stream" || f.Query != "abc" || f.Sort != "created_at" {
		t.Fatalf("%+v %v", f, err)
	}
	r = &http.Request{URL: &url.URL{RawQuery: "sort=name"}}
	if _, err := SessionFilterFrom(r); err == nil {
		t.Fatal("accepted an unknown sort")
	}
}

func TestIfMatch(t *testing.T) {
	for in, want := range map[string]int64{`"7"`: 7, "7": 7, `W/"7"`: 7} {
		if got, err := IfMatchVersion(in); err != nil || got != want {
			t.Errorf("%q: %d %v", in, got, err)
		}
	}
	for _, bad := range []string{"", `"x"`} {
		if _, err := IfMatchVersion(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestSessionDTO(t *testing.T) {
	started := at.Add(-10 * time.Second)
	big := uint64(1) << 60
	row := &store.Session{
		ID: "ses_1", State: session.Running, Seed: big, Records: big, Bytes: 1 << 40, StartedAt: &started, CatalogID: "cat_1", CatalogVersion: 2,
		LimitsJSON: `{"count":5000,"duration_ms":1000}`, Rate: 9, MaxWorkers: 3,
	}
	d := ToSessionDTO(row, coord.Live{InFlight: 7, Rate: 2.5}, at)
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{`"seed":"1152921504606846976"`, `"records":"1152921504606846976"`, `"bytes":"1099511627776"`, `"count":"5000"`,
		`"in_flight_records":"7"`, `"catalog":{"id":"cat_1","version":2}`, `"current":2.5`, `"rate":9`} {
		if !strings.Contains(text, want) {
			t.Errorf("no %s in %s", want, text)
		}
	}
	if !slices.Contains(d.AvailableActions, "pause") {
		t.Fatalf("%v", d.AvailableActions)
	}
	done := ToSessionDTO(&store.Session{State: session.Created}, coord.Live{}, at)
	if done.AvailableActions == nil {
		t.Fatal("actions must never be null")
	}
}

func TestWithProgress(t *testing.T) {
	var d SessionDTO
	p := session.Progress{Expected: 200, Units: map[string]int64{store.UnitPending: 2, store.UnitLeased: 3, store.UnitCommitted: 4}}
	WithProgress(&d, p, coord.Live{GenBlocked: 2 * time.Second}, 50)
	if d.Progress.ExpectedRecords != "200" || *d.Progress.Fraction != 0.25 || d.Units.Leased != 3 || d.Pressure.OutputBlockedSeconds != 2 {
		t.Fatalf("%+v", d)
	}
	var over SessionDTO
	WithProgress(&over, p, coord.Live{}, 900)
	if *over.Progress.Fraction != 1 {
		t.Fatal("a sparse stream can pass its estimate, but not 100 %")
	}
	var unbounded SessionDTO
	WithProgress(&unbounded, session.Progress{}, coord.Live{}, 900)
	if unbounded.Progress.Fraction != nil {
		t.Fatal("no fraction without an expected size")
	}
}

func TestLimitedBy(t *testing.T) {
	cases := []struct {
		live coord.Live
		want string
	}{
		{coord.Live{}, ""},
		{coord.Live{EncWait: 100 * time.Millisecond}, ""},
		{coord.Live{GenBlocked: 5 * time.Second, EncWait: 2 * time.Second}, "output"},
		{coord.Live{EncWait: 5 * time.Second}, "generation"},
		{coord.Live{SinkBlocked: 5 * time.Second, GenBlocked: 4 * time.Second}, "delivery"},
		// Generators held back by the rate starve the encoder: that is the rate's doing.
		{coord.Live{EncWait: 10 * time.Second, RateWait: 10 * time.Second}, "rate"},
		{coord.Live{EncWait: 20 * time.Second, RateWait: 5 * time.Second}, "generation"},
	}
	for i, tc := range cases {
		if got := LimitedBy(tc.live); got != tc.want {
			t.Errorf("case %d: %q", i, got)
		}
	}
}

func TestPeakIsNeverBelowAverage(t *testing.T) {
	started := at.Add(-time.Second)
	d := ToSessionDTO(&store.Session{Records: 1000, StartedAt: &started, PeakRate: 0}, coord.Live{}, at)
	if d.Rates.Peak != 1000 {
		t.Fatal(d.Rates)
	}
}

func TestAverageRateIgnoresPausedTime(t *testing.T) {
	started := at.Add(-20 * time.Second)
	s := &store.Session{Records: 1000, StartedAt: &started, PausedTotal: 10 * time.Second}
	if got := AverageRate(s, at); got != 100 {
		t.Fatal(got)
	}
	paused := at.Add(-5 * time.Second)
	s.PausedAt = &paused
	if got := AverageRate(s, at); got != 200 {
		t.Fatal(got)
	}
	if got := AverageRate(&store.Session{}, at); got != 0 {
		t.Fatal(got)
	}
}

func TestArtifactsGroupByStream(t *testing.T) {
	row := &store.Session{ID: "ses_1", Format: "csv", State: session.Completed}
	parts := []store.Part{
		{Stream: "table:b", UnitSeq: 0, Seq: 0, Records: 1, Bytes: 10, RelPath: "x/y.csv"},
		{Stream: "table:a", UnitSeq: 1, Seq: 0, Records: 2, Bytes: 20, RelPath: "x/y.csv"},
		{Stream: "table:a", UnitSeq: 0, Seq: 1, Records: 4, Bytes: 40, RelPath: "x/y.csv"},
		{Stream: "table:a", UnitSeq: 0, Seq: 0, Records: 8, Bytes: 80, RelPath: "x/y.csv"},
	}
	got := ToArtifacts(row, parts)
	if len(got) != 2 || got[0].Stream != "table:a" || got[0].Records != "14" || got[0].Bytes != "140" || got[1].Records != "1" {
		t.Fatalf("%+v", got)
	}
	a := got[0]
	if a.Parts[0].Records != "8" || a.Parts[2].Records != "2" || a.Parts[2].N != 2 {
		t.Fatalf("part order: %+v", a.Parts)
	}
	if !strings.HasSuffix(a.Parts[2].DownloadURL, "/artifacts/table:a/parts/2") || !strings.HasSuffix(a.MergedURL, "/artifacts/table:a/merged") {
		t.Fatalf("%+v", a)
	}
	if a.Status != ArtifactComplete {
		t.Fatal(a.Status)
	}
	gz := ToArtifacts(row, []store.Part{{Stream: "main", RelPath: "x/y.csv.gz"}})
	if gz[0].MergedURL != "" {
		t.Fatal("compressed output can't be merged")
	}
	if got := ToArtifacts(row, nil); got == nil || len(got) != 0 {
		t.Fatal("must be an empty list")
	}
}

func TestArtifactStatus(t *testing.T) {
	cases := map[string]store.Session{
		ArtifactComplete: {State: session.Completed},
		ArtifactPartial:  {State: session.Stopped},
		ArtifactDegraded: {State: session.Completed, ArtifactsDegraded: true},
	}
	for want, row := range cases {
		if got := ArtifactStatus(&row); got != want {
			t.Errorf("%+v: %s", row, got)
		}
	}
}

func TestWorkerDTOHasNoCredential(t *testing.T) {
	data, err := json.Marshal(ToWorkerDTO(&store.Worker{ID: "w", CredentialHash: []byte("secret-hash"), CapabilitiesJSON: `{"slots":2}`}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "c2VjcmV0") || strings.Contains(strings.ToLower(string(data)), "credential") {
		t.Fatalf("leaked: %s", data)
	}
	if !strings.Contains(string(data), `"slots":2`) {
		t.Fatalf("%s", data)
	}
}

func TestTokenDTOHasNoHash(t *testing.T) {
	data, err := json.Marshal(ToTokenDTO(&store.APIToken{ID: "t", Hash: []byte("secret-hash"), Name: "n"}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "hash") || strings.Contains(string(data), `"token"`) {
		t.Fatalf("leaked: %s", data)
	}
}

func TestRenderMetrics(t *testing.T) {
	text := RenderMetrics(&MetricsSnapshot{
		Version: "1.2", Sessions: map[string]int64{"RUNNING": 2, "COMPLETED": 5}, Workers: map[string]int64{"ACTIVE": 1},
		Records: 9, Bytes: 8, SSEClients: 3, Requests: map[int]int64{2: 10, 5: 1},
	})
	for _, want := range []string{
		`datafeed_build_info{version="1.2"} 1`, `datafeed_sessions{state="RUNNING"} 2`, `datafeed_workers{state="ACTIVE"} 1`,
		"datafeed_records_committed 9", "datafeed_bytes_committed 8", "datafeed_sse_clients 3", `datafeed_http_requests_total{class="5xx"} 1`,
		"# TYPE datafeed_http_requests_total counter",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("no %q in:\n%s", want, text)
		}
	}
	if strings.Index(text, `state="COMPLETED"`) > strings.Index(text, `state="RUNNING"`) {
		t.Error("unstable order")
	}
}

func TestWriteSSE(t *testing.T) {
	var b bytes.Buffer
	if err := WriteSSE(&b, "state", map[string]string{"a": "line\nbreak"}); err != nil {
		t.Fatal(err)
	}
	want := "event: state\ndata: {\"a\":\"line\\nbreak\"}\n\n"
	if b.String() != want {
		t.Fatalf("%q", b.String())
	}
}

func TestStatusCounts(t *testing.T) {
	var c statusCounts
	c.add(200)
	c.add(204)
	c.add(503)
	got := c.snapshot()
	if got[2] != 2 || got[5] != 1 {
		t.Fatalf("%v", got)
	}
}

func TestContentTypes(t *testing.T) {
	for name, want := range map[string]string{
		"a.json": "application/json", "a.ndjson": "application/x-ndjson", "a.xml": "application/xml", "a.csv": "text/csv; charset=utf-8", "a.bin": "application/octet-stream",
	} {
		if got := ContentTypeFor(name); got != want {
			t.Errorf("%s: %s", name, got)
		}
	}
}

func TestLongLivedPaths(t *testing.T) {
	for path, want := range map[string]bool{
		"/api/v1/events": true, "/api/v1/sessions/x/artifacts/main/merged": true, "/api/v1/sessions/x/artifacts/main/parts/1": true,
		"/api/v1/sessions/x": false, "/api/v1/sessions/x/artifacts": false, "/api/v1/sessions/x/events": false, "/api/v1/sessions/x/artifacts/manifest": false,
		"/api/v1/admin/backup": true, "/debug/pprof/profile": true,
	} {
		if got := isLongLived(path); got != want {
			t.Errorf("%s: %v", path, got)
		}
	}
}

func TestLimitWriter(t *testing.T) {
	var b bytes.Buffer
	l := &limitWriter{w: &b, left: 5}
	if _, err := l.Write([]byte("abc")); err != nil || !l.wrote {
		t.Fatal(err)
	}
	if _, err := l.Write([]byte("def")); err == nil {
		t.Fatal("went over")
	}
	if b.String() != "abc" {
		t.Fatal(b.String())
	}
}

func TestAsAppError(t *testing.T) {
	if e := asAppError(store.ErrNotFound); e.Kind != apperr.NotFound {
		t.Fatalf("%+v", e)
	}
	if e := asAppError(store.ErrConflict); e.Kind != apperr.Conflict {
		t.Fatalf("%+v", e)
	}
	if e := asAppError(&http.MaxBytesError{Limit: 10}); e.Kind != apperr.PayloadTooLarge {
		t.Fatalf("%+v", e)
	}
	if e := asAppError(errors.New("boom")); e.Kind != apperr.Internal {
		t.Fatalf("%+v", e)
	}
	own := apperr.New(apperr.Conflict, "ERR_X", "x")
	if e := asAppError(own); e != own {
		t.Fatalf("%+v", e)
	}
}
