package obs

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/genadigeno/data-feed/internal/security"
)

func jsonLogger(t *testing.T, b *strings.Builder) *slog.Logger {
	t.Helper()
	l, err := NewLogger(b, LogOptions{Level: "info", Format: "json"})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestSensitiveKeysAreRedacted(t *testing.T) {
	for _, key := range []string{"token", "api_key", "API_KEY", "password", "join_token", "authorization", "worker_credential"} {
		var b strings.Builder
		jsonLogger(t, &b).Info("x", key, "hunter2")
		if strings.Contains(b.String(), "hunter2") {
			t.Errorf("key %q leaked: %s", key, b.String())
		}
	}
}

func TestOrdinaryAttributesPassThrough(t *testing.T) {
	var b strings.Builder
	jsonLogger(t, &b).Info("x", "session_id", "ses_1", "records", 42)

	var got map[string]any
	if err := json.Unmarshal([]byte(b.String()), &got); err != nil {
		t.Fatal(err)
	}
	if got["session_id"] != "ses_1" || got["records"] != float64(42) {
		t.Fatalf("attributes changed: %v", got)
	}
}

func TestRedactionCoversWithAttrsAndGroups(t *testing.T) {
	var b strings.Builder
	l := jsonLogger(t, &b).With("token", "hunter2").WithGroup("g")
	l.Info("x", slog.Group("inner", "secret", "hunter2", "plain", "visible"))

	out := b.String()
	if strings.Contains(out, "hunter2") {
		t.Fatalf("leaked: %s", out)
	}
	if !strings.Contains(out, "visible") {
		t.Fatalf("non-sensitive group attribute missing: %s", out)
	}
}

func TestSecretTypeIsRedactedUnderAnyKey(t *testing.T) {
	s, err := security.RawSecret("hunter2").Resolve(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	jsonLogger(t, &b).Info("x", "value", s)
	if strings.Contains(b.String(), "hunter2") {
		t.Fatalf("leaked: %s", b.String())
	}
}

func TestLevelFilteringAndInvalidOptions(t *testing.T) {
	var b strings.Builder
	l, err := NewLogger(&b, LogOptions{Level: "warn", Format: "text"})
	if err != nil {
		t.Fatal(err)
	}
	l.Info("hidden")
	l.Warn("shown")
	if strings.Contains(b.String(), "hidden") || !strings.Contains(b.String(), "shown") {
		t.Fatalf("level filter wrong: %s", b.String())
	}

	if _, err := NewLogger(&b, LogOptions{Level: "loud", Format: "text"}); err == nil {
		t.Error("invalid level accepted")
	}
	if _, err := NewLogger(&b, LogOptions{Level: "info", Format: "xml"}); err == nil {
		t.Error("invalid format accepted")
	}
}
