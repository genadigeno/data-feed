// Package security holds secret handling and TLS configuration.
package security

import (
	"fmt"
	"log/slog"
	"strings"

	"go.yaml.in/yaml/v3"
)

const redacted = "[REDACTED]"

// Secret holds a sensitive value. Every way of printing or serialising it yields
// [REDACTED]; only Reveal returns the value.
type Secret struct {
	raw   string // as written in configuration: ${ENV}, file:/path or a literal
	value string
}

// RawSecret returns an unresolved Secret from its configured form.
func RawSecret(raw string) Secret { return Secret{raw: raw} }

// Reveal returns the secret value. Call it only where the value is used.
func (s Secret) Reveal() string { return s.value }

// IsZero reports whether nothing was configured.
func (s Secret) IsZero() bool { return s.raw == "" && s.value == "" }

// IsLiteral reports whether the value is written directly in the configuration
// rather than referenced from the environment or a file.
func (s Secret) IsLiteral() bool {
	return s.raw != "" && !strings.HasPrefix(s.raw, "${") && !strings.HasPrefix(s.raw, "file:")
}

// Resolve loads the value behind a ${ENV} or file: reference.
func (s Secret) Resolve(getenv func(string) (string, bool), readFile func(string) ([]byte, error)) (Secret, error) {
	switch {
	case s.raw == "":
		return s, nil
	case strings.HasPrefix(s.raw, "${"):
		if !strings.HasSuffix(s.raw, "}") {
			return s, fmt.Errorf("malformed environment reference (want ${NAME})")
		}
		name := s.raw[2 : len(s.raw)-1]
		v, ok := getenv(name)
		if !ok || v == "" {
			return s, fmt.Errorf("environment variable %s is not set", name)
		}
		s.value = v
	case strings.HasPrefix(s.raw, "file:"):
		b, err := readFile(strings.TrimPrefix(s.raw, "file:"))
		if err != nil {
			// The path is not sensitive, the contents are; err never holds contents.
			return s, fmt.Errorf("read secret file: %w", err)
		}
		s.value = strings.TrimRight(string(b), "\r\n")
		if s.value == "" {
			return s, fmt.Errorf("secret file is empty")
		}
	default:
		s.value = s.raw
	}
	return s, nil
}

// String implements fmt.Stringer.
func (s Secret) String() string {
	if s.IsZero() {
		return ""
	}
	return redacted
}

// GoString implements fmt.GoStringer so %#v does not expose fields.
func (s Secret) GoString() string { return "security.Secret(" + s.String() + ")" }

// Format prevents %v, %+v and %s from reaching the struct fields.
func (s Secret) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(s.String())) }

// LogValue implements slog.LogValuer.
func (s Secret) LogValue() slog.Value { return slog.StringValue(s.String()) }

// MarshalText implements encoding.TextMarshaler (JSON and map keys).
func (s Secret) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// MarshalYAML implements yaml.Marshaler.
func (s Secret) MarshalYAML() (any, error) { return s.String(), nil }

// UnmarshalYAML captures the configured form; Resolve loads the value later.
func (s *Secret) UnmarshalYAML(n *yaml.Node) error {
	var raw string
	if err := n.Decode(&raw); err != nil {
		return err
	}
	*s = Secret{raw: raw}
	return nil
}
