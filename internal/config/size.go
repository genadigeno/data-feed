package config

import (
	"fmt"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Size is a byte count written as 64MiB, 2GiB, 512KiB or plain bytes.
type Size int64

const (
	KiB Size = 1 << 10
	MiB Size = 1 << 20
	GiB Size = 1 << 30
	TiB Size = 1 << 40
)

var sizeUnits = []struct {
	suffix string
	unit   Size
}{{"TiB", TiB}, {"GiB", GiB}, {"MiB", MiB}, {"KiB", KiB}, {"B", 1}}

// ParseSize parses a size string.
func ParseSize(s string) (Size, error) {
	s = strings.TrimSpace(s)
	unit := Size(1)
	number := s
	for _, u := range sizeUnits {
		if strings.HasSuffix(s, u.suffix) {
			unit = u.unit
			number = strings.TrimSpace(strings.TrimSuffix(s, u.suffix))
			break
		}
	}
	n, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q (want e.g. 64MiB, 2GiB or plain bytes)", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid size %q: negative", s)
	}
	if n > int64((1<<63-1)/unit) {
		return 0, fmt.Errorf("invalid size %q: too large", s)
	}
	return Size(n) * unit, nil
}

// String renders the largest unit that divides the value exactly.
func (s Size) String() string {
	if s != 0 {
		for _, u := range sizeUnits[:4] {
			if s%u.unit == 0 {
				return strconv.FormatInt(int64(s/u.unit), 10) + u.suffix
			}
		}
	}
	return strconv.FormatInt(int64(s), 10)
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (s *Size) UnmarshalText(b []byte) error {
	v, err := ParseSize(string(b))
	if err != nil {
		return err
	}
	*s = v
	return nil
}

// UnmarshalYAML accepts both plain integers and suffixed strings.
func (s *Size) UnmarshalYAML(n *yaml.Node) error {
	var raw string
	if err := n.Decode(&raw); err != nil {
		return err
	}
	return s.UnmarshalText([]byte(raw))
}

// MarshalYAML renders the human-readable form.
func (s Size) MarshalYAML() (any, error) { return s.String(), nil }
