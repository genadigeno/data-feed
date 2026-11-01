// Package vocab is catalog content: the values generation draws from. It depends only on
// the engine, as Workers decode catalogs out of snapshots and never call AI or a database.
// See docs/02-domain/domain-vocabulary.md.
package vocab

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"regexp"
	"slices"

	"github.com/genadigeno/data-feed/internal/gen/value"
)

// DataTypes a group can hold.
const (
	TypeString  = "string"
	TypeInteger = "integer"
	TypeDecimal = "decimal"
	TypeEnum    = "enum"
)

// ExportFormat identifies an exported catalog document.
const ExportFormat = "datafeed.catalog/v1"

var groupKey = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// ValidKey reports whether a group key can be referenced from a schema.
func ValidKey(key string) bool { return groupKey.MatchString(key) }

// ValidType reports whether a data type is known.
func ValidType(t string) bool {
	return t == TypeString || t == TypeInteger || t == TypeDecimal || t == TypeEnum
}

// Entry is one value.
type Entry struct {
	Value    string            `json:"value"`
	Weight   float64           `json:"weight,omitempty"` // 0 means 1
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Group is a named list of values.
type Group struct {
	Key         string            `json:"key"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	DataType    string            `json:"data_type,omitempty"`
	Category    string            `json:"category,omitempty"`
	Locale      string            `json:"locale,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Values      []Entry           `json:"values"`
}

// Content is a catalog's groups, as pinned into snapshots.
type Content struct {
	Groups []Group `json:"groups"`
}

// Canonicalize orders content, so that equal catalogs encode, and hash, the same however
// they were assembled. Sampling depends on this order: it is part of reproducibility.
func Canonicalize(c *Content) {
	slices.SortFunc(c.Groups, func(a, b Group) int { return cmp.Compare(a.Key, b.Key) })
	for i := range c.Groups {
		slices.SortFunc(c.Groups[i].Values, func(a, b Entry) int { return cmp.Compare(a.Value, b.Value) })
	}
}

// Encode renders the canonical JSON and its hash.
func Encode(c *Content) (json.RawMessage, string, error) {
	Canonicalize(c)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(c); err != nil {
		return nil, "", err
	}
	body := bytes.TrimRight(buf.Bytes(), "\n")
	sum := sha256.Sum256(body)
	return body, hex.EncodeToString(sum[:]), nil
}

// Check finds structural problems: what must hold for content to be sampled at all.
func Check(c *Content) error {
	seen := make(map[string]bool, len(c.Groups))
	for i := range c.Groups {
		g := &c.Groups[i]
		if !ValidKey(g.Key) {
			return fmt.Errorf("group key %q must be lower_snake_case, up to 64 characters", g.Key)
		}
		if seen[g.Key] {
			return fmt.Errorf("group %q appears twice", g.Key)
		}
		seen[g.Key] = true
		if g.DataType != "" && !ValidType(g.DataType) {
			return fmt.Errorf("group %q has unknown data type %q", g.Key, g.DataType)
		}
		if len(g.Values) == 0 {
			return fmt.Errorf("group %q has no values", g.Key)
		}
		for j := range g.Values {
			if w := g.Values[j].Weight; w < 0 {
				return fmt.Errorf("group %q value %q has negative weight", g.Key, g.Values[j].Value)
			}
		}
	}
	return nil
}

// sampler draws from one group, with weights, in constant time.
type sampler struct {
	values []string
	alias  *value.Alias // nil when all weights are equal
}

func newSampler(g *Group) *sampler {
	s := &sampler{values: make([]string, len(g.Values))}
	weights := make([]float64, len(g.Values))
	uniform := true
	for i := range g.Values {
		s.values[i] = g.Values[i].Value
		weights[i] = g.Values[i].Weight
		if weights[i] == 0 {
			weights[i] = 1
		}
		uniform = uniform && weights[i] == weights[0]
	}
	if !uniform {
		s.alias = value.NewAlias(weights)
	}
	return s
}

func (s *sampler) Sample(r *rand.Rand) string {
	if s.alias != nil {
		return s.values[s.alias.Sample(r)]
	}
	return s.values[r.IntN(len(s.values))]
}

func (s *sampler) Len() int { return len(s.values) }

func (s *sampler) At(i int) string { return s.values[i] }

// Vocabulary is decoded content, ready to generate from.
type Vocabulary struct{ groups map[string]*sampler }

// Group implements value.Vocabulary.
func (v *Vocabulary) Group(name string) (value.Sampler, bool) {
	s, ok := v.groups[name]
	if !ok {
		return nil, false
	}
	return s, true
}

var _ value.Vocabulary = (*Vocabulary)(nil)

// Compile prepares content for sampling.
func Compile(c *Content) (*Vocabulary, error) {
	if err := Check(c); err != nil {
		return nil, err
	}
	Canonicalize(c)
	v := &Vocabulary{groups: make(map[string]*sampler, len(c.Groups))}
	for i := range c.Groups {
		v.groups[c.Groups[i].Key] = newSampler(&c.Groups[i])
	}
	return v, nil
}

// Decode turns a snapshot's pinned catalog into a Vocabulary.
func Decode(content json.RawMessage) (value.Vocabulary, error) {
	var c Content
	if err := json.Unmarshal(content, &c); err != nil {
		return nil, fmt.Errorf("decode catalog: %w", err)
	}
	if len(c.Groups) == 0 {
		return nil, errors.New("the catalog has no groups")
	}
	return Compile(&c)
}

// Export is the portable form of a catalog.
type Export struct {
	Format  string  `json:"format"`
	Domain  string  `json:"domain,omitempty"`
	Catalog string  `json:"catalog,omitempty"`
	Groups  []Group `json:"groups"`
}
