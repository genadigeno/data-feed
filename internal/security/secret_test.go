package security

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

const hidden = "s3cr3t-value"

func resolved(t *testing.T) Secret {
	t.Helper()
	s, err := RawSecret("${KEY}").Resolve(
		func(string) (string, bool) { return hidden, true },
		func(string) ([]byte, error) { return nil, errors.New("unused") },
	)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestEveryOutputPathRedacts(t *testing.T) {
	s := resolved(t)
	wrapper := struct{ Key Secret }{s}

	jsonBytes, err := json.Marshal(wrapper)
	if err != nil {
		t.Fatal(err)
	}
	yamlBytes, err := yaml.Marshal(wrapper)
	if err != nil {
		t.Fatal(err)
	}
	var logged strings.Builder
	slog.New(slog.NewTextHandler(&logged, nil)).Info("x", "key", s)

	outputs := map[string]string{
		"%v":   fmt.Sprintf("%v", s),
		"%+v":  fmt.Sprintf("%+v", wrapper),
		"%#v":  fmt.Sprintf("%#v", s),
		"%s":   fmt.Sprintf("%s", s),
		"json": string(jsonBytes),
		"yaml": string(yamlBytes),
		"slog": logged.String(),
	}
	for name, out := range outputs {
		if strings.Contains(out, hidden) {
			t.Errorf("%s leaked the secret: %q", name, out)
		}
		if !strings.Contains(out, redacted) {
			t.Errorf("%s does not show the redaction marker: %q", name, out)
		}
	}
	if s.Reveal() != hidden {
		t.Fatalf("Reveal() = %q", s.Reveal())
	}
}

func TestResolve(t *testing.T) {
	getenv := func(k string) (string, bool) {
		if k == "SET" {
			return "from-env", true
		}
		return "", false
	}
	readFile := func(p string) ([]byte, error) {
		switch p {
		case "/ok":
			return []byte("from-file\r\n"), nil
		case "/empty":
			return []byte("\n"), nil
		}
		return nil, errors.New("no such file")
	}

	cases := []struct {
		raw     string
		want    string
		literal bool
		wantErr bool
	}{
		{raw: "", want: ""},
		{raw: "${SET}", want: "from-env"},
		{raw: "${MISSING}", wantErr: true},
		{raw: "${SET", wantErr: true},
		{raw: "file:/ok", want: "from-file"},
		{raw: "file:/empty", wantErr: true},
		{raw: "file:/missing", wantErr: true},
		{raw: "plain", want: "plain", literal: true},
	}
	for _, c := range cases {
		s := RawSecret(c.raw)
		if s.IsLiteral() != c.literal {
			t.Errorf("%q: IsLiteral = %v", c.raw, s.IsLiteral())
		}
		got, err := s.Resolve(getenv, readFile)
		if (err != nil) != c.wantErr {
			t.Errorf("%q: err = %v, wantErr %v", c.raw, err, c.wantErr)
			continue
		}
		if err == nil && got.Reveal() != c.want {
			t.Errorf("%q: value = %q, want %q", c.raw, got.Reveal(), c.want)
		}
	}
}

func TestZeroSecretPrintsEmpty(t *testing.T) {
	var s Secret
	if !s.IsZero() || s.String() != "" {
		t.Fatalf("zero secret: IsZero=%v String=%q", s.IsZero(), s.String())
	}
}

func TestUnmarshalYAMLKeepsReferenceUnresolved(t *testing.T) {
	var s Secret
	if err := yaml.Unmarshal([]byte(`"${KEY}"`), &s); err != nil {
		t.Fatal(err)
	}
	if s.IsLiteral() || s.Reveal() != "" {
		t.Fatalf("got literal=%v value=%q", s.IsLiteral(), s.Reveal())
	}
}
