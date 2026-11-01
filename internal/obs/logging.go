// Package obs sets up logging. Metrics are added with the features they measure.
package obs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
)

const redactedValue = "[REDACTED]"

var sensitiveKey = regexp.MustCompile(`(?i)(token|secret|password|api_key|apikey|authorization|credential)`)

// LogOptions configures NewLogger.
type LogOptions struct {
	Level  string // debug | info | warn | error
	Format string // text | json
}

// NewLogger returns a logger whose output passes through the redaction handler.
func NewLogger(w io.Writer, o LogOptions) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(o.Level))); err != nil {
		return nil, fmt.Errorf("log level %q: %w", o.Level, err)
	}
	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler
	switch o.Format {
	case "text":
		h = slog.NewTextHandler(w, opts)
	case "json":
		h = slog.NewJSONHandler(w, opts)
	default:
		return nil, fmt.Errorf("log format %q: want text or json", o.Format)
	}
	return slog.New(redactHandler{h}), nil
}

// redactHandler replaces values whose keys look sensitive. security.Secret needs
// no help here: its LogValue already redacts.
type redactHandler struct{ next slog.Handler }

func (h redactHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

func (h redactHandler) Handle(ctx context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(redact(a))
		return true
	})
	return h.next.Handle(ctx, out)
}

func (h redactHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	safe := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		safe[i] = redact(a)
	}
	return redactHandler{h.next.WithAttrs(safe)}
}

func (h redactHandler) WithGroup(name string) slog.Handler {
	return redactHandler{h.next.WithGroup(name)}
}

func redact(a slog.Attr) slog.Attr {
	if sensitiveKey.MatchString(a.Key) {
		return slog.String(a.Key, redactedValue)
	}
	v := a.Value.Resolve()
	if v.Kind() == slog.KindGroup {
		group := v.Group()
		safe := make([]slog.Attr, len(group))
		for i, g := range group {
			safe[i] = redact(g)
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(safe...)}
	}
	return slog.Attr{Key: a.Key, Value: v}
}
