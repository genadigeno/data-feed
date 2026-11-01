package apperr

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestAsFindsErrorThroughWrapping(t *testing.T) {
	inner := New(Conflict, "ERR_INVALID_TRANSITION", "cannot start")
	wrapped := fmt.Errorf("start session: %w", inner)

	got, ok := As(wrapped)
	if !ok || got != inner {
		t.Fatalf("As() = %v, %v; want the inner error", got, ok)
	}
	if _, ok := As(errors.New("plain")); ok {
		t.Fatal("As() found an *Error in a plain error")
	}
}

func TestWrapKeepsCauseForErrorsIs(t *testing.T) {
	cause := errors.New("disk gone")
	e := Wrap(cause, Internal, CodeInternal, "write failed")
	if !errors.Is(e, cause) {
		t.Fatal("errors.Is did not reach the cause")
	}
}

func TestOnlyUnavailableIsRetryableByDefault(t *testing.T) {
	for k := Internal; k <= Canceled; k++ {
		if got, want := New(k, "C", "m").Retryable, k == Unavailable; got != want {
			t.Errorf("kind %d: Retryable = %v, want %v", k, got, want)
		}
	}
}

func TestMappings(t *testing.T) {
	cases := []struct {
		kind Kind
		http int
		exit int
	}{
		{Internal, http.StatusInternalServerError, ExitGeneric},
		{InvalidArgument, http.StatusBadRequest, ExitUsage},
		{NotFound, http.StatusNotFound, ExitNotFound},
		{Conflict, http.StatusConflict, ExitConflict},
		{PreconditionFailed, http.StatusPreconditionFailed, ExitConflict},
		{Unauthenticated, http.StatusUnauthorized, ExitAuth},
		{PermissionDenied, http.StatusForbidden, ExitAuth},
		{ResourceExhausted, http.StatusTooManyRequests, ExitGeneric},
		{Unavailable, http.StatusServiceUnavailable, ExitUnavailable},
		{Canceled, 499, ExitInterrupted},
	}
	for _, c := range cases {
		if got := HTTPStatus(c.kind); got != c.http {
			t.Errorf("HTTPStatus(%d) = %d, want %d", c.kind, got, c.http)
		}
		if got := ExitCode(c.kind); got != c.exit {
			t.Errorf("ExitCode(%d) = %d, want %d", c.kind, got, c.exit)
		}
	}
}

func TestValidationProblemsMapTo422(t *testing.T) {
	e := Validation("ERR_VALIDATION", "bad", []Problem{{Path: "a", Code: "X", Message: "m"}})
	if got := HTTPStatusFor(e); got != http.StatusUnprocessableEntity {
		t.Fatalf("HTTPStatusFor = %d, want 422", got)
	}
	if got := HTTPStatusFor(New(InvalidArgument, "ERR_BAD", "bad")); got != http.StatusBadRequest {
		t.Fatalf("HTTPStatusFor malformed = %d, want 400", got)
	}
}
