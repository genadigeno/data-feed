package config

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/genadigeno/data-feed/internal/security"
)

var (
	durationType = reflect.TypeFor[time.Duration]()
	sizeType     = reflect.TypeFor[Size]()
	secretType   = reflect.TypeFor[security.Secret]()
	labelsType   = reflect.TypeFor[map[string]string]()
)

// isLeaf reports whether a struct-typed value is treated as a single setting.
func isLeaf(t reflect.Type) bool { return t.Kind() != reflect.Struct || t == secretType }

// fieldByTag returns the field of struct v whose yaml tag is name.
func fieldByTag(v reflect.Value, name string) (reflect.Value, bool) {
	t := v.Type()
	for i := range t.NumField() {
		if tag, _, _ := strings.Cut(t.Field(i).Tag.Get("yaml"), ","); tag == name {
			return v.Field(i), true
		}
	}
	return reflect.Value{}, false
}

// SetPath assigns a textual value to the setting at a dotted path such as
// generation.queue_capacity. Environment variables and flags both go through it.
func (c *Config) SetPath(path, raw string) error {
	v := reflect.ValueOf(c).Elem()
	for _, part := range strings.Split(path, ".") {
		if v.Kind() != reflect.Struct || v.Type() == secretType {
			return fmt.Errorf("unknown setting %q", path)
		}
		f, ok := fieldByTag(v, part)
		if !ok {
			return fmt.Errorf("unknown setting %q", path)
		}
		v = f
	}
	if !isLeaf(v.Type()) {
		return fmt.Errorf("setting %q is a section, not a value", path)
	}
	if err := setLeaf(v, raw); err != nil {
		return fmt.Errorf("setting %q: %w", path, err)
	}
	return nil
}

func setLeaf(v reflect.Value, raw string) error {
	switch v.Type() {
	case durationType:
		d, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("invalid duration %q", raw)
		}
		v.SetInt(int64(d))
		return nil
	case sizeType:
		s, err := ParseSize(raw)
		if err != nil {
			return err
		}
		v.SetInt(int64(s))
		return nil
	case secretType:
		v.Set(reflect.ValueOf(security.RawSecret(raw)))
		return nil
	case labelsType:
		m, err := parseLabels(raw)
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(m))
		return nil
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(raw)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("invalid boolean %q", raw)
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer %q", raw)
		}
		v.SetInt(n)
	case reflect.Float64:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return fmt.Errorf("invalid number %q", raw)
		}
		v.SetFloat(f)
	default:
		return fmt.Errorf("unsupported type %s", v.Type())
	}
	return nil
}

// parseLabels reads comma-separated key=value pairs.
func parseLabels(raw string) (map[string]string, error) {
	m := map[string]string{}
	if strings.TrimSpace(raw) == "" {
		return m, nil
	}
	for _, pair := range strings.Split(raw, ",") {
		k, val, ok := strings.Cut(pair, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid label %q (want key=value)", pair)
		}
		m[k] = strings.TrimSpace(val)
	}
	return m, nil
}

// Entry is one setting in a flattened configuration.
type Entry struct {
	Path   string `json:"path"`
	Value  string `json:"value"`
	Source string `json:"source"`
}

// Flatten lists every setting with its printable value; secrets stay redacted.
func (c *Config) Flatten() []Entry {
	var out []Entry
	var walk func(prefix string, v reflect.Value)
	walk = func(prefix string, v reflect.Value) {
		t := v.Type()
		for i := range t.NumField() {
			tag, _, _ := strings.Cut(t.Field(i).Tag.Get("yaml"), ",")
			path := tag
			if prefix != "" {
				path = prefix + "." + tag
			}
			f := v.Field(i)
			if isLeaf(f.Type()) {
				out = append(out, Entry{Path: path, Value: leafString(f)})
			} else {
				walk(path, f)
			}
		}
	}
	walk("", reflect.ValueOf(*c))
	return out
}

func leafString(v reflect.Value) string {
	if m, ok := v.Interface().(map[string]string); ok {
		pairs := make([]string, 0, len(m))
		for k, val := range m {
			pairs = append(pairs, k+"="+val)
		}
		slices.Sort(pairs)
		return strings.Join(pairs, ",")
	}
	return fmt.Sprint(v.Interface())
}
