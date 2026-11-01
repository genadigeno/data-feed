package api

import (
	"errors"
	"testing"

	"github.com/genadigeno/data-feed/internal/apperr"
)

func TestSummarize(t *testing.T) {
	ok := Check{Name: "a", OK: true}
	bad := Check{Name: "b", OK: false, Message: "down"}

	cases := []struct {
		checks []Check
		want   bool
	}{
		{nil, false},
		{[]Check{ok}, true},
		{[]Check{ok, ok}, true},
		{[]Check{ok, bad}, false},
	}
	for _, c := range cases {
		if got := summarize(c.checks).Ready; got != c.want {
			t.Errorf("summarize(%v).Ready = %v", c.checks, got)
		}
	}
}

func TestRequestIDKeepsOnlyWellFormedValues(t *testing.T) {
	if got := requestID("abc-123_X.y"); got != "abc-123_X.y" {
		t.Errorf("good ID replaced: %q", got)
	}
	long := make([]byte, 65)
	for i := range long {
		long[i] = 'a'
	}
	for _, bad := range []string{"", "has space", "new\nline", `quo"te`, string(long)} {
		got := requestID(bad)
		if got == bad || !validRequestID.MatchString(got) {
			t.Errorf("requestID(%q) = %q", bad, got)
		}
	}
	first, second := requestID(""), requestID("")
	if first == second {
		t.Errorf("generated IDs repeat: %q", first)
	}
}

func TestInternalErrorsRevealNothing(t *testing.T) {
	e := apperr.Wrap(errors.New("secret path /etc/x"), apperr.Internal, "ERR_DB_BROKEN", "write to /etc/x failed")
	e.Details = map[string]any{"path": "/etc/x"}

	d := toErrorBody(e, "req1").Error
	if d.Code != apperr.CodeInternal || d.Message != "internal error" || d.Details != nil {
		t.Fatalf("leaked internals: %+v", d)
	}
	if d.RequestID != "req1" {
		t.Fatalf("request ID lost: %+v", d)
	}
}

func TestOtherErrorsKeepTheirDetail(t *testing.T) {
	e := apperr.Validation("ERR_VALIDATION", "bad input", []apperr.Problem{{Path: "a", Code: "X", Message: "m"}})
	d := toErrorBody(e, "r").Error
	if d.Code != "ERR_VALIDATION" || d.Message != "bad input" || len(d.Problems) != 1 {
		t.Fatalf("detail lost: %+v", d)
	}
}
