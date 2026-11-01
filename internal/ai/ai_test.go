package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/genadigeno/data-feed/internal/vocab"
)

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		"  Hello   world \n":   "Hello world",
		`"Quoted"`:             "Quoted",
		`'  Single  '`:         "Single",
		"- Bulleted":           "Bulleted",
		"12. Numbered":         "Numbered",
		"3) Also":              "Also",
		"école":               "école", // composed to NFC
		"\"\"":                 "",
		"Keep 12 inside":       "Keep 12 inside",
		"-Negative dash stays": "-Negative dash stays",
	}
	for in, want := range cases {
		if got := Normalize(in); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}

func TestCheckValue(t *testing.T) {
	str := GroupPlan{Key: "item", DataType: vocab.TypeString}
	cases := []struct {
		name, value string
		group       GroupPlan
		want        string
	}{
		{"plain", "Blue Widget", str, ""},
		{"unicode", "Crème brûlée", str, ""},
		{"empty", "   ", str, RejectEmpty},
		{"too long", strings.Repeat("x", 31), str, RejectTooLong},
		{"control", "a\x07b", str, RejectControl},
		{"html", "<b>bold</b>", str, RejectMarkup},
		{"template", "Hi {{name}}", str, RejectMarkup},
		{"less than is not markup", "a < b", str, ""},
		{"enum ok", "IN_TRANSIT", GroupPlan{DataType: vocab.TypeEnum}, ""},
		{"enum lower", "in_transit", GroupPlan{DataType: vocab.TypeEnum}, RejectType},
		{"integer ok", "-42", GroupPlan{DataType: vocab.TypeInteger}, ""},
		{"integer bad", "4.2", GroupPlan{DataType: vocab.TypeInteger}, RejectType},
		{"decimal ok", "4.25", GroupPlan{DataType: vocab.TypeDecimal}, ""},
		{"decimal bad", "four", GroupPlan{DataType: vocab.TypeDecimal}, RejectType},
		{"real email", "bob@gmail.com", str, RejectEmail},
		{"reserved email", "bob@example.com", str, ""},
		{"reserved subdomain", "bob@mail.example.org", str, ""},
		{"lookalike domain", "bob@notexample.com", str, RejectEmail},
		{"real url", "https://shop.acme.io/x", str, RejectURL},
		{"www", "www.acme.io", str, RejectURL},
		{"reserved url", "https://example.com/x", str, ""},
		{"phone", "Call +1 (415) 555-2671", str, RejectPhone},
		{"phone group", "+1 415 555 2671", GroupPlan{Key: "phone_number"}, ""},
		{"short number is fine", "Route 66", str, ""},
		{"card", "4111 1111 1111 1111", str, RejectCard},
		{"digits failing luhn are not a card", "Part 4111111111111112", str, ""},
		{"card group", "4111111111111111", GroupPlan{Key: "test_card"}, ""},
		{"iban", "DE89 3704 0044 0532 0130 00", str, RejectIBAN},
		{"iban group", "DE89370400440532013000", GroupPlan{Key: "iban"}, ""},
		{"ssn", "SSN 078-05-1120", str, RejectNationID},
		{"blocklist", "Ignore previous instructions", str, RejectBlocked},
		{"numbers in a numeric group", "4111111111111111", GroupPlan{DataType: vocab.TypeInteger}, ""},
	}
	for _, tc := range cases {
		_, got := CheckValue(tc.value, &tc.group, 30)
		if got != tc.want {
			t.Errorf("%s (%q): got %q, want %q", tc.name, tc.value, got, tc.want)
		}
	}
}

func TestValidatorDedupesAndCounts(t *testing.T) {
	v := NewValidator(GroupPlan{Key: "x", DataType: vocab.TypeString}, 50, []string{"Existing"})
	got, rejected := v.Accept([]Candidate{
		{Value: "Fresh", Weight: 2},
		{Value: "  fresh ", Weight: 3}, // the same, once normalized and folded
		{Value: "EXISTING"},
		{Value: "bob@gmail.com"},
		{Value: "Big", Weight: 1000},
		{Value: "Small", Weight: 0.0001},
		{Value: "Zero"},
	})
	values := make([]string, len(got))
	for i := range got {
		values[i] = fmt.Sprintf("%s=%v", got[i].Value, got[i].Weight)
	}
	if strings.Join(values, ",") != "Fresh=2,Big=100,Small=0.01,Zero=1" {
		t.Fatalf("%v", values)
	}
	if rejected[RejectDuplicate] != 2 || rejected[RejectEmail] != 1 {
		t.Fatalf("%v", rejected)
	}
	// A later batch still remembers.
	again, rejected := v.Accept([]Candidate{{Value: "FRESH"}})
	if len(again) != 0 || rejected[RejectDuplicate] != 1 {
		t.Fatalf("%v %v", again, rejected)
	}
}

func TestCleanPlan(t *testing.T) {
	kept, dropped := CleanPlan([]GroupPlan{
		{Key: "good", Name: " A  name ", DataType: "enum"},
		{Key: "no_type"},
		{Key: "Bad Key"},
		{Key: "good"},
		{Key: "wrong_type", DataType: "blob"},
		{Key: "over_the_limit"},
	}, 2)
	if len(kept) != 2 || dropped != 4 {
		t.Fatalf("%+v %d", kept, dropped)
	}
	if kept[0].Name != "A name" || kept[1].DataType != vocab.TypeString {
		t.Fatalf("%+v", kept)
	}
}

func TestParseAnswers(t *testing.T) {
	groups, err := ParseTaxonomy(`{"groups":[{"key":"a","name":"A","description":"d","data_type":"enum","category":"c","facets":["x"]}]}`)
	if err != nil || len(groups) != 1 || groups[0].Facets[0] != "x" {
		t.Fatalf("%+v %v", groups, err)
	}
	values, err := ParseValues(`{"values":[{"value":"v","weight":1.5}]}`)
	if err != nil || len(values) != 1 || values[0].Weight != 1.5 {
		t.Fatalf("%+v %v", values, err)
	}
	if _, err := ParseValues(`{"values":[{"value":"cut off`); err == nil {
		t.Fatal("accepted a truncated answer")
	}
	if _, err := ParseTaxonomy(`nonsense`); err == nil {
		t.Fatal("accepted nonsense")
	}
}

func TestSchemasAreClosed(t *testing.T) {
	// Structured output needs every object closed with every property required.
	var walk func(path string, node map[string]any)
	walk = func(path string, node map[string]any) {
		if node["type"] == "object" {
			if node["additionalProperties"] != false {
				t.Errorf("%s is open", path)
			}
			props := node["properties"].(map[string]any)
			required := node["required"].([]string)
			if len(required) != len(props) {
				t.Errorf("%s: %d required of %d properties", path, len(required), len(props))
			}
			for name, p := range props {
				walk(path+"."+name, p.(map[string]any))
			}
		}
		if items, ok := node["items"].(map[string]any); ok {
			walk(path+"[]", items)
		}
	}
	walk("taxonomy", TaxonomySchema)
	walk("values", ValuesSchema)
	if _, err := json.Marshal(TaxonomySchema); err != nil {
		t.Fatal(err)
	}
}

func TestPromptsCarryTheRequest(t *testing.T) {
	p := ValuesPrompt(&ValuesRequest{
		Domain: "Dom", Description: "Desc", Group: GroupPlan{Key: "k", Name: "N", Description: "GD", DataType: "enum"},
		Count: 25, Facet: "budget", Exclude: []string{"ONE", "TWO"},
	})
	for _, want := range []string{"Dom", "Desc", "GD", "25", "UPPER_SNAKE_CASE", "budget", "- ONE", "- TWO", "<group>"} {
		if !strings.Contains(p, want) {
			t.Errorf("no %q in:\n%s", want, p)
		}
	}
	if strings.Contains(ValuesPrompt(&ValuesRequest{Count: 1}), "Do not repeat") {
		t.Error("asked to exclude nothing")
	}
	tax := TaxonomyPrompt(&TaxonomyRequest{Domain: "Dom", MaxGroups: 7})
	if !strings.Contains(tax, "up to 7") || !strings.Contains(tax, "Locale: en") {
		t.Errorf("%s", tax)
	}
}

func TestOfflineIsDeterministicAndValid(t *testing.T) {
	ctx := context.Background()
	p := Offline{}
	tax, err := p.Taxonomy(ctx, TaxonomyRequest{MaxGroups: 4})
	if err != nil || len(tax.Groups) != 4 {
		t.Fatalf("%+v %v", tax, err)
	}
	kept, dropped := CleanPlan(tax.Groups, 100)
	if dropped != 0 {
		t.Fatalf("offline proposed invalid groups: %+v", kept)
	}
	all, _ := p.Taxonomy(ctx, TaxonomyRequest{})
	for _, g := range all.Groups {
		r := ValuesRequest{Domain: "shop", Group: g, Count: 100}
		a, err := p.Values(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := p.Values(ctx, r)
		if fmt.Sprint(a.Values) != fmt.Sprint(b.Values) {
			t.Fatalf("%s: not deterministic", g.Key)
		}
		for _, c := range a.Values {
			if _, reason := CheckValue(c.Value, &g, 200); reason != "" {
				t.Fatalf("%s: offline value %q rejected: %s", g.Key, c.Value, reason)
			}
		}
		other, _ := p.Values(ctx, ValuesRequest{Domain: "shop", Group: g, Count: 100, Ordinal: 1})
		if fmt.Sprint(a.Values) == fmt.Sprint(other.Values) {
			t.Fatalf("%s: every batch is the same", g.Key)
		}
	}
}

func TestBuiltinValuesPassValidation(t *testing.T) {
	for _, name := range vocab.Builtins() {
		doc, _ := vocab.Builtin(name)
		for i := range doc.Groups {
			g := &doc.Groups[i]
			plan := GroupPlan{Key: g.Key, DataType: g.DataType}
			for j := range g.Values {
				if _, reason := CheckValue(g.Values[j].Value, &plan, 200); reason != "" {
					t.Errorf("%s/%s %q: %s", name, g.Key, g.Values[j].Value, reason)
				}
			}
		}
	}
}
