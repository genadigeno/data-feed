package vocab

import (
	"encoding/json"
	"math/rand/v2"
	"strings"
	"testing"
)

func sample() *Content {
	return &Content{Groups: []Group{
		{Key: "b", Values: []Entry{{Value: "z"}, {Value: "y", Weight: 2}}},
		{Key: "a", Values: []Entry{{Value: "x"}}},
	}}
}

func TestEncodeIsCanonical(t *testing.T) {
	first, hash1, err := Encode(sample())
	if err != nil {
		t.Fatal(err)
	}
	other := &Content{Groups: []Group{
		{Key: "a", Values: []Entry{{Value: "x"}}},
		{Key: "b", Values: []Entry{{Value: "y", Weight: 2}, {Value: "z"}}},
	}}
	second, hash2, err := Encode(other)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) || hash1 != hash2 {
		t.Fatalf("order changed the encoding:\n%s\n%s", first, second)
	}
	changed := sample()
	changed.Groups[0].Values[0].Value = "changed"
	if _, hash3, _ := Encode(changed); hash3 == hash1 {
		t.Fatal("different content has the same hash")
	}
}

func TestCheckFindsProblems(t *testing.T) {
	cases := []struct {
		name string
		c    Content
		want string
	}{
		{"bad key", Content{Groups: []Group{{Key: "Bad Key", Values: []Entry{{Value: "x"}}}}}, "lower_snake_case"},
		{"duplicate", Content{Groups: []Group{{Key: "a", Values: []Entry{{Value: "x"}}}, {Key: "a", Values: []Entry{{Value: "x"}}}}}, "twice"},
		{"bad type", Content{Groups: []Group{{Key: "a", DataType: "blob", Values: []Entry{{Value: "x"}}}}}, "data type"},
		{"empty", Content{Groups: []Group{{Key: "a"}}}, "no values"},
		{"negative", Content{Groups: []Group{{Key: "a", Values: []Entry{{Value: "x", Weight: -1}}}}}, "negative"},
	}
	for _, tc := range cases {
		err := Check(&tc.c)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v", tc.name, err)
		}
	}
	if err := Check(sample()); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeSamplesByWeight(t *testing.T) {
	content, _, err := Encode(&Content{Groups: []Group{{Key: "g", Values: []Entry{{Value: "rare", Weight: 1}, {Value: "common", Weight: 9}}}}})
	if err != nil {
		t.Fatal(err)
	}
	v, err := Decode(content)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.Group("g")
	if !ok || s.Len() != 2 || s.At(0) != "common" {
		t.Fatalf("%v %v", ok, s)
	}
	if _, ok := v.Group("missing"); ok {
		t.Fatal("found a missing group")
	}
	r := rand.New(rand.NewPCG(1, 2))
	common := 0
	for range 10000 {
		if s.Sample(r) == "common" {
			common++
		}
	}
	if common < 8700 || common > 9300 {
		t.Fatalf("common drawn %d of 10000", common)
	}
}

func TestSamplingIsReproducible(t *testing.T) {
	content, _, err := Encode(sample())
	if err != nil {
		t.Fatal(err)
	}
	draw := func() []string {
		v, err := Decode(content)
		if err != nil {
			t.Fatal(err)
		}
		s, _ := v.Group("b")
		r := rand.New(rand.NewPCG(7, 7))
		out := make([]string, 50)
		for i := range out {
			out[i] = s.Sample(r)
		}
		return out
	}
	first, second := draw(), draw()
	if strings.Join(first, ",") != strings.Join(second, ",") {
		t.Fatal("same seed, different values")
	}
}

func TestDecodeRejectsBadContent(t *testing.T) {
	for _, doc := range []string{"not json", `{"groups":[]}`, `{"groups":[{"key":"a","values":[]}]}`} {
		if _, err := Decode(json.RawMessage(doc)); err == nil {
			t.Errorf("accepted %s", doc)
		}
	}
}

func TestBuiltinsAreUsable(t *testing.T) {
	names := Builtins()
	if len(names) != 11 {
		t.Fatalf("%d built-in catalogs: %v", len(names), names)
	}
	for _, name := range names {
		doc, ok := Builtin(name)
		if !ok {
			t.Fatalf("%s did not load", name)
		}
		if doc.Domain != name || doc.Name == "" || doc.Description == "" {
			t.Errorf("%s header: %+v", name, doc.Domain)
		}
		if len(doc.Groups) < 9 {
			t.Errorf("%s has only %d groups", name, len(doc.Groups))
		}
		c := &Content{Groups: doc.Groups}
		if err := Check(c); err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		for i := range doc.Groups {
			g := &doc.Groups[i]
			if g.Name == "" || g.Description == "" || g.DataType == "" {
				t.Errorf("%s/%s lacks a description", name, g.Key)
			}
			seen := map[string]bool{}
			for j := range g.Values {
				if seen[g.Values[j].Value] {
					t.Errorf("%s/%s repeats %q", name, g.Key, g.Values[j].Value)
				}
				seen[g.Values[j].Value] = true
			}
		}
		if _, _, err := Encode(c); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestBuiltinRejectsPaths(t *testing.T) {
	for _, name := range []string{"../vocab", "a/b", "nope", `a\b`} {
		if _, ok := Builtin(name); ok {
			t.Errorf("found %q", name)
		}
	}
}
