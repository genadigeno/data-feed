package schema

import (
	"encoding/json"
	"math/big"
	"os"
	"reflect"
	"strings"
	"testing"
)

func decode(t *testing.T, doc string) *Schema {
	t.Helper()
	s, problems := Decode([]byte(doc), FormatAuto)
	if len(problems) > 0 {
		t.Fatalf("Decode: %v", []Problem(problems))
	}
	return s
}

func testdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func has(ps Problems, path, code string) bool {
	for _, p := range ps {
		if p.Path == path && p.Code == code {
			return true
		}
	}
	return false
}

func wantProblem(t *testing.T, ps Problems, path, code string) {
	t.Helper()
	if !has(ps, path, code) {
		t.Fatalf("want %s at %q, got %v", code, path, []Problem(ps))
	}
}

func opts() Options { return Options{Limits: DefaultLimits()} }

func TestExampleSchemasAreValid(t *testing.T) {
	for _, name := range []string{"ecommerce.yaml", "customer.yaml"} {
		s, problems := Decode(testdata(t, name), FormatYAML)
		if len(problems) > 0 {
			t.Fatalf("%s: decode: %v", name, []Problem(problems))
		}
		if problems := Validate(s, opts()); len(problems) > 0 {
			t.Fatalf("%s: validate: %v", name, []Problem(problems))
		}
	}
}

func TestDecodeKeepsDocumentOrder(t *testing.T) {
	s := decode(t, "kind: hybrid\nfields:\n  zebra: {type: string}\n  apple: {type: string}\n  mango: {type: string}\n")
	var names []string
	for _, f := range s.Hybrid.Fields {
		names = append(names, f.Name)
	}
	if got := strings.Join(names, ","); got != "zebra,apple,mango" {
		t.Fatalf("order = %s", got)
	}

	s = decode(t, `{"kind":"hybrid","fields":{"zebra":{"type":"string"},"apple":{"type":"string"}}}`)
	if s.Hybrid.Fields[0].Name != "zebra" {
		t.Fatalf("JSON order lost: %+v", s.Hybrid.Fields)
	}
}

func TestDecodeDetailsOfExample(t *testing.T) {
	s, problems := Decode(testdata(t, "ecommerce.yaml"), FormatYAML)
	if problems != nil {
		t.Fatal(problems)
	}
	orders := s.Relational.Table("orders")
	if orders.Owner.Table != "customers" || orders.Owner.PerParent.Max != 8 || orders.Owner.PerParent.Distribution.Type != DistZipf {
		t.Fatalf("owner = %+v", orders.Owner)
	}
	fk := orders.Field("customer_id")
	if fk.Ref.Table != "customers" || fk.Ref.Field != "id" || !fk.Ref.Owner {
		t.Fatalf("ref = %+v", fk.Ref)
	}
	if d := orders.Field("created_at").Derive; d.From != "owner" || d.OffsetMax.Hours() != 72 {
		t.Fatalf("derive = %+v", d)
	}
	sel := s.Relational.Table("order_items").Field("product_id").Ref.Select
	if sel.Distribution.Type != DistZipf || *sel.Distribution.S != 1.1 {
		t.Fatalf("select = %+v", sel)
	}
	if g := s.Relational.Table("products").Field("id").Generator; g.Name != "sequence" || g.Params[0].Name != "start" {
		t.Fatalf("generator = %+v", g)
	}
	// A decimal stays exact: no float conversion on the way through.
	if m := orders.Field("total_amount").Min; m.Kind != LiteralNumber || m.Text != "1" {
		t.Fatalf("min = %+v", m)
	}
}

func TestRandomKeepsDefaultsWhereUnset(t *testing.T) {
	s := decode(t, "kind: random\nfields: {max: 5}\n")
	want := DefaultRandomSpec()
	want.Fields.Max = 5
	if !reflect.DeepEqual(*s.Random, want) {
		t.Fatalf("got %+v", *s.Random)
	}
	if problems := Validate(s, opts()); len(problems) > 0 {
		t.Fatal(problems)
	}
}

func TestRoundTripIsStable(t *testing.T) {
	docs := map[string]string{
		"ecommerce": string(testdata(t, "ecommerce.yaml")),
		"customer":  string(testdata(t, "customer.yaml")),
		"random": `kind: random
fields: {min: 1, max: 2}
float: {min: 0.5, max: 1e6, precision: 2}
types: {string: 3, integer: 1.5}
field_names: {style: snake, vocabulary_group: words, min_len: 2, max_len: 9, unique_per_object: false}
`,
		"probabilities": "kind: random\nprobabilities: {null: 0.1, object: 0.2, array: 0.3}\n",
		"tricky": `kind: hybrid
fields:
  "with space": {type: string, default: "123", pattern: "a|b"}
  quoted: {type: string, values: ["yes", "null", "1.5", "line\nbreak", "ünï<>&"]}
  big: {type: long, min: 9007199254740993, max: 9223372036854775807}
  exact: {type: decimal, precision: 30, scale: 10, min: 0.1234567890, max: 12345678901234567890.5}
  list: {type: string, generator: {name: g, params: {a: [1, "x", true, null], b: null, c: 1.5}}}
`,
	}
	for name, doc := range docs {
		t.Run(name, func(t *testing.T) {
			first := decode(t, doc)

			fromJSON, problems := Decode(EncodeJSON(first), FormatJSON)
			if len(problems) > 0 {
				t.Fatalf("re-decode JSON: %v\n%s", []Problem(problems), EncodeJSON(first))
			}
			if !reflect.DeepEqual(first, fromJSON) {
				t.Fatalf("JSON round trip changed the schema\n%s", EncodeJSON(first))
			}

			y, err := EncodeYAML(first)
			if err != nil {
				t.Fatal(err)
			}
			fromYAML, problems := Decode(y, FormatYAML)
			if len(problems) > 0 {
				t.Fatalf("re-decode YAML: %v\n%s", []Problem(problems), y)
			}
			if !reflect.DeepEqual(first, fromYAML) {
				t.Fatalf("YAML round trip changed the schema\n%s", y)
			}

			if string(EncodeJSON(first)) != string(EncodeJSON(fromYAML)) {
				t.Fatal("canonical JSON differs between round trips")
			}
			if !json.Valid(EncodeJSON(first)) {
				t.Fatalf("invalid JSON: %s", EncodeJSON(first))
			}
		})
	}
}

func TestHashDependsOnContentAndOrder(t *testing.T) {
	a := decode(t, "kind: hybrid\nfields: {x: {type: string}, y: {type: string}}\n")
	same := decode(t, `{"kind":"hybrid","fields":{"x":{"type":"string"},"y":{"type":"string"}}}`)
	reordered := decode(t, "kind: hybrid\nfields: {y: {type: string}, x: {type: string}}\n")

	if Hash(a) != Hash(same) {
		t.Fatal("same schema hashed differently across syntaxes")
	}
	if Hash(a) == Hash(reordered) {
		t.Fatal("field order is meaningful but did not change the hash")
	}
}

func TestDecodeProblems(t *testing.T) {
	cases := []struct {
		name, doc, path, code string
	}{
		{"not a document", ":\n\t- x", "", CodeSyntax},
		{"empty", "", "", CodeSyntax},
		{"two documents", "kind: random\n---\nkind: hybrid\n", "", CodeSyntax},
		{"duplicate key", "kind: hybrid\nkind: random\n", "", CodeSyntax},
		{"duplicate JSON key", `{"kind":"hybrid","kind":"random"}`, "", CodeSyntax},
		{"merge key", "a: &a {type: string}\nkind: hybrid\nfields: {<<: *a}\n", "", CodeSyntax},
		{"no kind", "fields: {}\n", "kind", CodeMissingKey},
		{"bad kind", "kind: fancy\n", "kind", CodeBadValue},
		{"kind wrong type", "kind: 3\n", "kind", CodeWrongType},
		{"version", "schema_version: 9\nkind: random\n", "schema_version", CodeBadValue},
		{"top-level typo", "kind: random\nfeilds: {}\n", "feilds", CodeUnknownKey},
		{"field typo", "kind: hybrid\nfields: {a: {type: string, uniqe: true}}\n", "fields.a.uniqe", CodeUnknownKey},
		{"tables in hybrid", "kind: hybrid\nfields: {a: {type: string}}\ntables: {}\n", "tables", CodeUnknownKey},
		{"no type", "kind: hybrid\nfields: {a: {nullable: true}}\n", "fields.a.type", CodeMissingKey},
		{"field not mapping", "kind: hybrid\nfields: {a: string}\n", "fields.a", CodeWrongType},
		{"wrong scalar", "kind: hybrid\nfields: {a: {type: string, nullable: 3}}\n", "fields.a.nullable", CodeWrongType},
		{"presence in relational", "kind: relational\ntables: {t: {count: 1, fields: {a: {type: string, presence: required}}}}\n", "tables.t.fields.a.presence", CodeKeyNotAllowed},
		{"ref in hybrid", "kind: hybrid\nfields: {a: {type: reference, ref: x.y}}\n", "fields.a.ref", CodeKeyNotAllowed},
		{"bad presence", "kind: hybrid\nfields: {a: {type: string, presence: maybe}}\n", "fields.a.presence", CodeBadValue},
		{"bad ref", "kind: relational\ntables: {t: {count: 1, fields: {a: {type: reference, ref: nodot}}}}\n", "tables.t.fields.a.ref", CodeBadValue},
		{"both types and probabilities", "kind: random\ntypes: {string: 1}\nprobabilities: {null: 0.1}\n", "probabilities", CodeKeyNotAllowed},
		{"bad offset", "kind: relational\ntables: {t: {count: 1, fields: {a: {type: date, derive: {from: owner, field: x, offset: {min: soon}}}}}}\n", "tables.t.fields.a.derive.offset.min", CodeBadValue},
		{"empty type", `kind: hybrid
fields: {a: {type: ""}}`, "fields.a.type", CodeBadValue},
		{"empty generator", `kind: hybrid
fields: {a: {type: string, generator: {name: ""}}}`, "fields.a.generator.name", CodeBadValue},
		{"empty generator shorthand", `kind: hybrid
fields: {a: {type: string, generator: ""}}`, "fields.a.generator", CodeBadValue},
		{"empty distribution", `kind: hybrid
fields: {a: {type: integer, distribution: ""}}`, "fields.a.distribution", CodeBadValue},
		{"huge integer", "kind: random\nmax_record_bytes: 99999999999999999999999\n", "max_record_bytes", CodeBadValue},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, problems := Decode([]byte(c.doc), FormatAuto)
			wantProblem(t, problems, c.path, c.code)
		})
	}
}

func TestDecodeReportsEveryProblem(t *testing.T) {
	_, problems := Decode([]byte("kind: hybrid\nfields: {a: {type: string, x: 1}, b: {y: 2}}\nz: 3\n"), FormatYAML)
	if len(problems) < 4 { // x, b.y, b.type, z
		t.Fatalf("got %v", []Problem(problems))
	}
}

func TestAliasBombIsRefused(t *testing.T) {
	var b strings.Builder
	b.WriteString("a0: &a0 [x, x, x, x, x, x, x, x, x, x]\n")
	for i := 1; i < 12; i++ {
		prev := "*a" + string(rune('a'+i-1))
		if i == 1 {
			prev = "*a0"
		}
		name := "a" + string(rune('a'+i))
		b.WriteString(name + ": &" + name + " [" + strings.Repeat(prev+",", 9) + prev + "]\n")
	}
	if _, problems := Decode([]byte(b.String()), FormatYAML); !has(problems, "", CodeSyntax) {
		t.Fatalf("alias bomb got through: %v", []Problem(problems))
	}
}

func TestHugeDocumentsStayLinear(t *testing.T) {
	// Quadratic key handling would take minutes on these; the test then hits its timeout.
	const n = 300_000
	var hybrid strings.Builder
	hybrid.WriteString(`{"kind":"hybrid","fields":{`)
	for i := range n {
		if i > 0 {
			hybrid.WriteByte(',')
		}
		hybrid.WriteString(`"f` + big.NewInt(int64(i)).String() + `":{"type":"string"}`)
	}
	hybrid.WriteString("}}")
	s, problems := Decode([]byte(hybrid.String()), FormatJSON)
	if len(problems) > 0 {
		t.Fatal(problems[0])
	}
	wantProblem(t, Validate(s, opts()), "fields", CodeLimit)

	var tables strings.Builder
	tables.WriteString(`{"kind":"relational","tables":{`)
	for i := range n {
		if i > 0 {
			tables.WriteByte(',')
		}
		tables.WriteString(`"t` + big.NewInt(int64(i)).String() + `":{"count":1,"fields":{"a":{"type":"string"}}}`)
	}
	tables.WriteString("}}")
	s, problems = Decode([]byte(tables.String()), FormatJSON)
	if len(problems) > 0 {
		t.Fatal(problems[0])
	}
	wantProblem(t, Validate(s, opts()), "tables", CodeLimit)
}

func TestOversizedTableStopsCrossTableChecks(t *testing.T) {
	var fields strings.Builder
	for i := range 300 {
		fields.WriteString("f" + big.NewInt(int64(i)).String() + ": {type: string}, ")
	}
	s := decode(t, relationalDoc("  t: {count: 1, fields: {"+fields.String()+"}}\n"))
	wantProblem(t, Validate(s, opts()), "tables.t.fields", CodeLimit)
}

func TestDeepNestingIsRefused(t *testing.T) {
	doc := strings.Repeat("[", maxTreeDepth+5) + strings.Repeat("]", maxTreeDepth+5)
	if _, problems := Decode([]byte(doc), FormatJSON); !has(problems, "", CodeSyntax) {
		t.Fatalf("deep nesting got through: %v", []Problem(problems))
	}
}

func TestJSONToleratesTabsAndRejectsTrailingData(t *testing.T) {
	decode(t, "{\n\t\"kind\": \"random\"\n}")
	if _, problems := Decode([]byte(`{"kind":"random"} {"x":1}`), FormatJSON); !has(problems, "", CodeSyntax) {
		t.Fatalf("trailing data accepted: %v", []Problem(problems))
	}
}

func TestFieldValidation(t *testing.T) {
	cases := []struct {
		name, field, path, code string
	}{
		{"unknown type", "{type: nope}", "type", CodeUnknownType},
		{"min above max", "{type: integer, min: 5, max: 1}", "", CodeRange},
		{"date range reversed", `{type: date, min: "2026-01-02", max: "2026-01-01"}`, "", CodeRange},
		{"min on string", "{type: string, min: 1}", "min", CodeConstraint},
		{"int out of 32 bits", "{type: integer, max: 3000000000}", "max", CodeLiteral},
		{"long out of 64 bits", "{type: long, max: 9223372036854775808}", "max", CodeLiteral},
		{"fractional integer", "{type: integer, min: 1.5}", "min", CodeLiteral},
		{"bad date", `{type: date, min: "soon"}`, "min", CodeLiteral},
		{"number for date", "{type: date, min: 5}", "min", CodeLiteral},
		{"length on integer", "{type: integer, max_length: 3}", "", CodeConstraint},
		{"length reversed", "{type: string, min_length: 5, max_length: 1}", "", CodeRange},
		{"negative length", "{type: string, min_length: -1}", "min_length", CodeRange},
		{"length over limit", "{type: string, max_length: 99999999}", "max_length", CodeLimit},
		{"pattern on integer", `{type: integer, pattern: "a"}`, "pattern", CodeConstraint},
		{"bad pattern", `{type: string, pattern: "("}`, "pattern", CodePattern},
		{"lookaround", `{type: string, pattern: "(?=a)"}`, "pattern", CodePattern},
		{"word boundary", `{type: string, pattern: "\\bword"}`, "pattern", CodePattern},
		{"pattern too long", `{type: string, pattern: "[a-z]{10}", max_length: 5}`, "pattern", CodePattern},
		{"pattern too short", `{type: string, pattern: "[a-z]{2}", min_length: 5}`, "pattern", CodePattern},
		{"enum without values", "{type: enum}", "values", CodeEmptyEnum},
		{"empty values", "{type: enum, values: []}", "values", CodeEmptyEnum},
		{"repeated value", "{type: enum, values: [a, a]}", "values", CodeLiteral},
		{"value of wrong type", "{type: integer, values: [1, x]}", "values", CodeLiteral},
		{"values on object", "{type: object, values: [1], fields: {a: {type: string}}}", "values", CodeConstraint},
		{"weights count", "{type: enum, values: [a, b], weights: [1]}", "weights", CodeWeights},
		{"negative weight", "{type: enum, values: [a, b], weights: [1, -1]}", "weights", CodeWeights},
		{"zero weights", "{type: enum, values: [a, b], weights: [0, 0]}", "weights", CodeWeights},
		{"weights without values", "{type: string, weights: [1]}", "weights", CodeWeights},
		{"distribution on string", "{type: string, distribution: uniform}", "distribution", CodeConstraint},
		{"unknown distribution", "{type: integer, distribution: wavy}", "distribution", CodeDistribution},
		{"normal without params", "{type: integer, distribution: normal}", "distribution", CodeDistribution},
		{"normal bad stddev", "{type: integer, distribution: {type: normal, mean: 1, stddev: 0}}", "distribution.stddev", CodeDistribution},
		{"stray parameter", "{type: integer, distribution: {type: uniform, mean: 1}}", "distribution.mean", CodeDistribution},
		{"zipf s", "{type: integer, distribution: {type: zipf, s: 1}}", "distribution.s", CodeDistribution},
		{"zipf on float", "{type: float, distribution: zipf}", "distribution", CodeDistribution},
		{"sequence on float", "{type: float, distribution: sequence}", "distribution", CodeDistribution},
		{"sequence step", "{type: long, distribution: {type: sequence, step: 0}}", "distribution.step", CodeDistribution},
		{"null probability without nullable", "{type: string, null_probability: 0.5}", "null_probability", CodeConstraint},
		{"probability above one", "{type: string, nullable: true, null_probability: 1.5}", "null_probability", CodeProbability},
		{"presence above one", "{type: string, presence: {optional: 2}}", "presence.optional", CodeProbability},
		{"default wrong type", "{type: integer, default: x}", "default", CodeLiteral},
		{"default null not nullable", "{type: string, default: null}", "default", CodeNullable},
		{"default on object", "{type: object, default: 1, fields: {a: {type: string}}}", "default", CodeConstraint},
		{"default probability alone", "{type: string, default_probability: 0.5}", "default_probability", CodeConstraint},
		{"precision on integer", "{type: integer, precision: 3}", "precision", CodeConstraint},
		{"scale above precision", "{type: decimal, precision: 2, scale: 3}", "scale", CodeRange},
		{"precision above max", "{type: decimal, precision: 39}", "precision", CodeRange},
		{"bound beyond precision", "{type: decimal, precision: 4, scale: 2, max: 100}", "max", CodeRange},
		{"format on date", "{type: date, format: unix}", "format", CodeConstraint},
		{"bad format", "{type: timestamp, format: julian}", "format", CodeBadValue},
		{"zone on date", "{type: date, zone: UTC}", "zone", CodeConstraint},
		{"ranges on string", "{type: string, ranges: any}", "ranges", CodeConstraint},
		{"bad ranges", "{type: ipv4, ranges: all}", "ranges", CodeBadValue},
		{"object without fields", "{type: object}", "fields", CodeRequiredFields},
		{"fields on string", "{type: string, fields: {a: {type: string}}}", "fields", CodeConstraint},
		{"array without items", "{type: array}", "items", CodeMissingKey},
		{"items on string", "{type: string, items: {type: string}}", "items", CodeConstraint},
		{"map without value", "{type: map}", "value", CodeMissingKey},
		{"integer map key", "{type: map, key: {type: integer}, value: {type: string}}", "key", CodeConstraint},
		{"unknown item type", "{type: array, items: {type: nope}}", "items.type", CodeUnknownType},
		{"nested schema without name", "{type: nested_schema}", "schema", CodeMissingKey},
		{"nested schema unknown", "{type: nested_schema, schema: nope}", "schema", CodeUnknownSchema},
		{"reference without ref", "{type: reference}", "ref", CodeConstraint},
		{"unique object", "{type: object, unique: true, fields: {a: {type: string}}}", "unique", CodeUniqueUnsupported},
		{"unique nested", "{type: object, fields: {a: {type: uuid, unique: true}}}", "fields.a.unique", CodeUniqueUnsupported},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := decode(t, "kind: hybrid\nfields:\n  f: "+c.field+"\n")
			wantProblem(t, Validate(s, opts()), join("fields.f", c.path), c.code)
		})
	}
}

func TestValidFieldCombinations(t *testing.T) {
	for _, field := range []string{
		"{type: string, min_length: 2, max_length: 5, pattern: \"[a-z]+\"}",
		"{type: enum, values: [1, a, true], weights: [1, 0, 2]}",
		"{type: long, distribution: {type: sequence, start: 5, step: 2}}",
		"{type: integer, min: 1e3, max: 2.0e3}",
		"{type: float, min: -1.5, max: .5, distribution: {type: exponential, rate: 2}}",
		"{type: timestamp, format: unix_ms, min: \"2026-01-01T00:00:00+04:00\"}",
		"{type: datetime, zone: Asia/Tbilisi}",
		"{type: time, min: \"08:00:00\", max: \"17:00:00\"}",
		"{type: null}",
		"{type: string, nullable: true, default: null}",
		"{type: uuid, values: [\"00000000-0000-0000-0000-000000000001\"]}",
		"{type: binary, max_length: 16}",
		"{type: array, items: {type: array, items: {type: object, fields: {a: {type: string}}}}}",
		"{type: credit_card_test_value}",
	} {
		s := decode(t, "kind: hybrid\nfields:\n  f: "+field+"\n")
		if problems := Validate(s, opts()); len(problems) > 0 {
			t.Errorf("%s: %v", field, []Problem(problems))
		}
	}
}

func TestRandomValidation(t *testing.T) {
	cases := []struct {
		name, doc, path, code string
	}{
		{"fields reversed", "fields: {min: 5, max: 1}", "fields", CodeRange},
		{"fields over cap", "fields: {max: 100000}", "fields.max", CodeLimit},
		{"negative depth", "depth: {min: -1, max: 1}", "depth.min", CodeRange},
		{"depth over cap", "depth: {max: 17}", "depth.max", CodeLimit},
		{"array over cap", "array_length: {max: 100000}", "array_length.max", CodeLimit},
		{"string over cap", "string_length: {max: 100000000}", "string_length.max", CodeLimit},
		{"integer reversed", "integer: {min: 5, max: 1}", "integer", CodeRange},
		{"float reversed", "float: {min: 5, max: 1}", "float", CodeRange},
		{"float precision", "float: {precision: 20}", "float.precision", CodeRange},
		{"type not drawable", "types: {email: 1}", "types.email", CodeUnknownType},
		{"negative type weight", "types: {string: -1}", "types.string", CodeWeights},
		{"zero total weight", "types: {string: 0}", "types", CodeWeights},
		{"probability range", "probabilities: {null: 2}", "probabilities.null", CodeProbability},
		{"probabilities total", "probabilities: {null: 0.6, object: 0.6}", "probabilities", CodeProbability},
		{"name style", "field_names: {style: fancy}", "field_names.style", CodeBadValue},
		{"name length", "field_names: {min_len: 5, max_len: 2}", "field_names", CodeRange},
		{"record size", "max_record_bytes: 0", "max_record_bytes", CodeRange},
		{"record size over limit", "max_record_bytes: 99999999", "max_record_bytes", CodeLimit},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := decode(t, "kind: random\n"+c.doc+"\n")
			wantProblem(t, Validate(s, opts()), c.path, c.code)
		})
	}
}

func TestDefinitionValidation(t *testing.T) {
	s := decode(t, `kind: hybrid
definitions:
  a: {fields: {x: {type: nested_schema, schema: b}, y: {type: nested_schema, schema: b}}}
  b: {fields: {x: {type: array, items: {type: nested_schema, schema: a}}}}
fields: {f: {type: nested_schema, schema: a}}
`)
	problems := Validate(s, Options{Limits: DefaultLimits(), Output: Output{Format: "csv"}})
	wantProblem(t, problems, "definitions.a", CodeSchemaCycle)
}

func TestSharedDefinitionsStayFastToValidate(t *testing.T) {
	// Each level references the next twice: naive expansion would take 2^60 steps.
	var b strings.Builder
	b.WriteString("kind: hybrid\ndefinitions:\n  d60: {fields: {leaf: {type: string}}}\n")
	for i := 59; i >= 0; i-- {
		n := big.NewInt(int64(i)).String()
		next := big.NewInt(int64(i + 1)).String()
		b.WriteString("  d" + n + ": {fields: {x: {type: nested_schema, schema: d" + next + "}, y: {type: nested_schema, schema: d" + next + "}}}\n")
	}
	b.WriteString("fields: {f: {type: nested_schema, schema: d0}}\n")
	s := decode(t, b.String())

	problems := Validate(s, Options{Limits: DefaultLimits(), Output: Output{Format: "csv"}})
	wantProblem(t, problems, "fields.f.schema", CodeDepth)
	wantProblem(t, problems, "fields", CodeRecordTooLarge)
	wantProblem(t, problems, "fields", CodeLimit)
}

func TestHybridRules(t *testing.T) {
	s := decode(t, "kind: hybrid\nfields: {}\n")
	wantProblem(t, Validate(s, opts()), "fields", CodeRequiredFields)

	s = decode(t, "kind: hybrid\nfields: {a: {type: string}}\nextra_random: {probability: 2}\n")
	wantProblem(t, Validate(s, opts()), "extra_random.probability", CodeProbability)

	s = decode(t, "kind: hybrid\nfields: {a: {type: string}}\nextra_random: {probability: 0.5, spec: {depth: {max: 99}}}\n")
	wantProblem(t, Validate(s, opts()), "extra_random.spec.depth.max", CodeLimit)

	s = decode(t, "kind: hybrid\nfields: {a: {type: string}}\n")
	wantProblem(t, Validate(s, Options{Limits: DefaultLimits(), Output: Output{Nested: true}}), "", CodeFormatIncompat)
}

func TestRecordSizeLimit(t *testing.T) {
	s := decode(t, "kind: hybrid\nfields: {a: {type: array, max_length: 1000, items: {type: string, max_length: 60000}}}\n")
	wantProblem(t, Validate(s, opts()), "fields", CodeRecordTooLarge)

	s = decode(t, "kind: relational\ntables: {t: {count: 1, fields: {a: {type: string, max_length: 60000}}}}\n")
	wantProblem(t, Validate(s, opts()), "tables.t.fields", CodeRecordTooLarge)
}

func TestSizeBoundCoversDeclaredValues(t *testing.T) {
	s := decode(t, `kind: hybrid
fields:
  a: {type: enum, values: [short, "`+strings.Repeat("x", 500)+`"]}
  b: {type: string, default: "`+strings.Repeat("y", 400)+`", max_length: 1}
`)
	got := fieldsMaxBytes(s.Hybrid.Fields, nil)
	if got < 900 {
		t.Fatalf("bound %d is below the declared values", got)
	}
}

func TestColumnsAndCollisions(t *testing.T) {
	s := decode(t, `kind: hybrid
definitions:
  addr: {fields: {city: {type: city}}}
fields:
  id: {type: uuid}
  profile: {type: object, fields: {name: {type: full_name}, home: {type: nested_schema, schema: addr}}}
  tags: {type: array, items: {type: string}}
  attrs: {type: map, value: {type: string}}
`)
	defs := map[string]*Definition{"addr": &s.Definitions[0]}

	got := Columns(s.Hybrid.Fields, defs, "", 0)
	want := []string{"id", "profile.name", "profile.home.city", "tags", "attrs"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns = %v", got)
	}

	got = Columns(s.Hybrid.Fields, defs, "__", 2)
	want = []string{"id", "profile__name", "profile__home__city", "tags__0", "tags__1", "attrs"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns = %v", got)
	}

	collide := decode(t, `kind: hybrid
fields:
  "a.b": {type: string}
  a: {type: object, fields: {b: {type: string}}}
`)
	wantProblem(t, Validate(collide, Options{Limits: DefaultLimits(), Output: Output{Format: "csv"}}), "fields", CodeColumnCollision)
	if problems := Validate(collide, opts()); len(problems) > 0 {
		t.Fatalf("no CSV, no collision: %v", []Problem(problems))
	}
	if problems := Validate(collide, Options{Limits: DefaultLimits(), Output: Output{Format: "csv", Separator: "__"}}); len(problems) > 0 {
		t.Fatalf("separator should resolve it: %v", []Problem(problems))
	}
}

func relationalDoc(tables string) string { return "kind: relational\ntables:\n" + tables }

func TestRelationalValidation(t *testing.T) {
	root := "  p: {count: 10, primary_key: id, fields: {id: {type: uuid}}}\n"
	cases := []struct {
		name, tables, path, code string
	}{
		{"no tables", "  {}\n", "tables", CodeNoTables},
		{"bad table name", "  Bad-Name: {count: 1, fields: {a: {type: string}}}\n", "tables.Bad-Name", CodeTableName},
		{"no fields", "  t: {count: 1, fields: {}}\n", "tables.t.fields", CodeRequiredFields},
		{"no count", "  t: {fields: {a: {type: string}}}\n", "tables.t.count", CodeCountRequired},
		{"zero count", "  t: {count: 0, fields: {a: {type: string}}}\n", "tables.t.count", CodeRange},
		{"count and owner", root + "  t: {count: 5, owner: {ref: p, per_parent: {max: 1}}, fields: {f: {type: reference, ref: p.id, owner: true}}}\n", "tables.t.count", CodeCountForbidden},
		{"pk missing", "  t: {count: 1, primary_key: nope, fields: {a: {type: string}}}\n", "tables.t.primary_key", CodePrimaryKey},
		{"pk nullable", "  t: {count: 1, primary_key: a, fields: {a: {type: uuid, nullable: true}}}\n", "tables.t.primary_key", CodePrimaryKey},
		{"pk object", "  t: {count: 1, primary_key: a, fields: {a: {type: object, fields: {b: {type: string}}}}}\n", "tables.t.primary_key", CodePrimaryKey},
		{"pk float", "  t: {count: 1, primary_key: a, fields: {a: {type: float}}}\n", "tables.t.primary_key", CodePrimaryKey},
		{"pk reference", root + "  t: {count: 1, primary_key: a, fields: {a: {type: reference, ref: p.id}}}\n", "tables.t.primary_key", CodePrimaryKey},
		{"pk capacity", "  t: {count: 11, primary_key: a, fields: {a: {type: integer, min: 1, max: 10}}}\n", "tables.t.fields.a.unique", CodeUniqueInfeasible},
		{"fk table missing", "  t: {count: 1, fields: {a: {type: reference, ref: nope.id}}}\n", "tables.t.fields.a.ref", CodeFKTarget},
		{"fk field missing", root + "  t: {count: 1, fields: {a: {type: reference, ref: p.nope}}}\n", "tables.t.fields.a.ref", CodeFKTarget},
		{"fk not a key", "  p: {count: 1, fields: {x: {type: string}}}\n  t: {count: 1, fields: {a: {type: reference, ref: p.x}}}\n", "tables.t.fields.a.ref", CodeFKTargetKey},
		{"fk to nullable", "  p: {count: 1, fields: {x: {type: uuid, unique: true, nullable: true}}}\n  t: {count: 1, fields: {a: {type: reference, ref: p.x}}}\n", "tables.t.fields.a.ref", CodeFKTargetKey},
		{"ref without reference type", root + "  t: {count: 1, fields: {a: {type: uuid, ref: p.id}}}\n", "tables.t.fields.a.ref", CodeConstraint},
		{"owner unknown", "  t: {owner: {ref: nope, per_parent: {max: 1}}, fields: {a: {type: string}}}\n", "tables.t.owner.ref", CodeOwner},
		{"owner field missing", root + "  t: {owner: {ref: p, per_parent: {max: 1}}, fields: {a: {type: string}}}\n", "tables.t.owner", CodeOwner},
		{"owner flag without owner", root + "  t: {count: 1, fields: {a: {type: reference, ref: p.id, owner: true}}}\n", "tables.t.owner", CodeOwner},
		{"owner field to other table", root + "  q: {count: 1, primary_key: id, fields: {id: {type: uuid}}}\n  t: {owner: {ref: p, per_parent: {max: 1}}, fields: {a: {type: reference, ref: q.id, owner: true}}}\n", "tables.t.owner", CodeOwner},
		{"two owner fields", root + "  t: {owner: {ref: p, per_parent: {max: 1}}, fields: {a: {type: reference, ref: p.id, owner: true}, b: {type: reference, ref: p.id, owner: true}}}\n", "tables.t.owner", CodeOwner},
		{"nullable owner", root + "  t: {owner: {ref: p, per_parent: {max: 1}}, fields: {a: {type: reference, ref: p.id, owner: true, nullable: true}}}\n", "tables.t.fields.a", CodeOwner},
		{"owner with select", root + "  t: {owner: {ref: p, per_parent: {max: 1}}, fields: {a: {type: reference, ref: p.id, owner: true, select: {mode: earlier}}}}\n", "tables.t.fields.a.select", CodeSelect},
		{"per_parent reversed", root + "  t: {owner: {ref: p, per_parent: {min: 5, max: 1}}, fields: {a: {type: reference, ref: p.id, owner: true}}}\n", "tables.t.owner.per_parent", CodeRange},
		{"per_parent zero", root + "  t: {owner: {ref: p, per_parent: {max: 0, min: 0}}, fields: {a: {type: reference, ref: p.id, owner: true}}}\n", "tables.t.owner.per_parent", CodeRange},
		{"per_parent distribution", root + "  t: {owner: {ref: p, per_parent: {max: 2, distribution: sequence}}, fields: {a: {type: reference, ref: p.id, owner: true}}}\n", "tables.t.owner.per_parent.distribution", CodeDistribution},
		{"select distribution", root + "  t: {count: 1, fields: {a: {type: reference, ref: p.id, select: {distribution: exponential, rate: 1}}}}\n", "tables.t.fields.a.select.distribution", CodeDistribution},
		{"earlier on other table", root + "  t: {count: 1, fields: {a: {type: reference, ref: p.id, nullable: true, select: {mode: earlier}}}}\n", "tables.t.fields.a.select.mode", CodeSelect},
		{"earlier not nullable", "  t: {count: 1, primary_key: id, fields: {id: {type: uuid}, a: {type: reference, ref: t.id, select: {mode: earlier}}}}\n", "tables.t.fields.a.select.mode", CodeSelect},
		{"unknown mode", root + "  t: {count: 1, fields: {a: {type: reference, ref: p.id, select: {mode: odd}}}}\n", "tables.t.fields.a.select.mode", CodeSelect},
		{"distinct without owner", root + "  t: {count: 1, fields: {a: {type: reference, ref: p.id, select: {distinct: true}}}}\n", "tables.t.fields.a.select.distinct", CodeSelect},
		{"distinct exhausted", root + "  o: {count: 1, primary_key: id, fields: {id: {type: uuid}}}\n  t: {owner: {ref: o, per_parent: {max: 11}}, fields: {f: {type: reference, ref: o.id, owner: true}, a: {type: reference, ref: p.id, select: {distinct: true}}}}\n", "tables.t.fields.a.select.distinct", CodeSelect},
		{"fk to maybe-empty table", root + "  c: {primary_key: id, owner: {ref: p, per_parent: {min: 0, max: 2}}, fields: {id: {type: uuid}, f: {type: reference, ref: p.id, owner: true}}}\n  t: {count: 1, fields: {a: {type: reference, ref: c.id}}}\n", "tables.t.fields.a.ref", CodeFKMayBeEmpty},
		{"derive without owner", "  t: {count: 1, fields: {a: {type: date, derive: {from: owner, field: x}}}}\n", "tables.t.fields.a.derive", CodeDerive},
		{"derive from other", root + "  t: {count: 1, fields: {a: {type: date, derive: {from: parent, field: x}}}}\n", "tables.t.fields.a.derive.from", CodeDerive},
		{"derive field missing", root + "  t: {owner: {ref: p, per_parent: {max: 1}}, fields: {f: {type: reference, ref: p.id, owner: true}, a: {type: uuid, derive: {from: owner, field: nope}}}}\n", "tables.t.fields.a.derive.field", CodeDerive},
		{"derive type differs", root + "  t: {owner: {ref: p, per_parent: {max: 1}}, fields: {f: {type: reference, ref: p.id, owner: true}, a: {type: string, derive: {from: owner, field: id}}}}\n", "tables.t.fields.a.derive.field", CodeDerive},
		{"offset on uuid", root + "  t: {owner: {ref: p, per_parent: {max: 1}}, fields: {f: {type: reference, ref: p.id, owner: true}, a: {type: uuid, derive: {from: owner, field: id, offset: {max: 1h}}}}}\n", "tables.t.fields.a.derive.offset", CodeDerive},
		{"unique key one field", "  t: {count: 1, unique_keys: [{fields: [a]}], fields: {a: {type: string}}}\n", "tables.t.unique_keys[0].fields", CodeUniqueKey},
		{"unique key unknown field", "  t: {count: 1, unique_keys: [{fields: [a, nope]}], fields: {a: {type: string}}}\n", "tables.t.unique_keys[0].fields", CodeUniqueKey},
		{"unique key repeated", "  t: {count: 1, unique_keys: [{fields: [a, a]}], fields: {a: {type: string}}}\n", "tables.t.unique_keys[0].fields", CodeUniqueKey},
		{"unique key no driver", "  t: {count: 1, unique_keys: [{fields: [a, b]}], fields: {a: {type: string}, b: {type: string}}}\n", "tables.t.unique_keys[0].driver", CodeUniqueKey},
		{"unique key foreign driver", "  t: {count: 1, unique_keys: [{fields: [a, b], driver: c}], fields: {a: {type: string}, b: {type: string}, c: {type: string}}}\n", "tables.t.unique_keys[0].driver", CodeUniqueKey},
		{"unique key driver too small", "  t: {count: 100, unique_keys: [{fields: [a, b], driver: b}], fields: {a: {type: string}, b: {type: integer, min: 1, max: 5}}}\n", "tables.t.unique_keys[0].driver.unique", CodeUniqueInfeasible},
		{"index space overflow", "  p: {count: 9000000000000000000, primary_key: id, fields: {id: {type: uuid}}}\n  t: {owner: {ref: p, per_parent: {max: 10}}, fields: {f: {type: reference, ref: p.id, owner: true}}}\n", "tables.t", CodeIndexOverflow},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wantProblem(t, Validate(decode(t, relationalDoc(c.tables)), opts()), c.path, c.code)
		})
	}
}

func TestOwnershipCycles(t *testing.T) {
	s := decode(t, relationalDoc(`  a: {primary_key: id, owner: {ref: b, per_parent: {max: 1}}, fields: {id: {type: uuid}, f: {type: reference, ref: b.id, owner: true}}}
  b: {primary_key: id, owner: {ref: c, per_parent: {max: 1}}, fields: {id: {type: uuid}, f: {type: reference, ref: c.id, owner: true}}}
  c: {primary_key: id, owner: {ref: a, per_parent: {max: 1}}, fields: {id: {type: uuid}, f: {type: reference, ref: a.id, owner: true}}}
`))
	problems := Validate(s, opts())
	var cycles []Problem
	for _, p := range problems {
		if p.Code == CodeOwnershipCycle {
			cycles = append(cycles, p)
		}
	}
	if len(cycles) != 1 || !strings.Contains(cycles[0].Message, "->") {
		t.Fatalf("cycles = %v", cycles)
	}

	s = decode(t, relationalDoc("  a: {primary_key: id, owner: {ref: a, per_parent: {max: 1}}, fields: {id: {type: uuid}, f: {type: reference, ref: a.id, owner: true}}}\n"))
	wantProblem(t, Validate(s, opts()), "tables.a.owner.ref", CodeOwnershipCycle)
}

func TestSelectingCyclesAreAllowed(t *testing.T) {
	s := decode(t, relationalDoc(`  a: {count: 5, primary_key: id, fields: {id: {type: uuid}, b_id: {type: reference, ref: b.id}}}
  b: {count: 5, primary_key: id, fields: {id: {type: uuid}, a_id: {type: reference, ref: a.id}}}
  e: {count: 5, primary_key: id, fields: {id: {type: uuid}, boss: {type: reference, ref: e.id}}}
`))
	if problems := Validate(s, opts()); len(problems) > 0 {
		t.Fatalf("selecting cycles rejected: %v", []Problem(problems))
	}
}

func TestValidRelationalVariations(t *testing.T) {
	for name, tables := range map[string]string{
		"fk to unique field":         "  p: {count: 2, fields: {code: {type: uuid, unique: true}}}\n  t: {count: 1, fields: {a: {type: reference, ref: p.code}}}\n",
		"nullable fk to maybe-empty": "  p: {count: 1, primary_key: id, fields: {id: {type: uuid}}}\n  c: {primary_key: id, owner: {ref: p, per_parent: {min: 0, max: 2}}, fields: {id: {type: uuid}, f: {type: reference, ref: p.id, owner: true}}}\n  t: {count: 1, fields: {a: {type: reference, ref: c.id, nullable: true}}}\n",
		"guaranteed owned target":    "  p: {count: 1, primary_key: id, fields: {id: {type: uuid}}}\n  c: {primary_key: id, owner: {ref: p, per_parent: {min: 1, max: 2}}, fields: {id: {type: uuid}, f: {type: reference, ref: p.id, owner: true}}}\n  t: {count: 1, fields: {a: {type: reference, ref: c.id}}}\n",
		"distinct within bounds":     "  p: {count: 10, primary_key: id, fields: {id: {type: uuid}}}\n  o: {count: 1, primary_key: id, fields: {id: {type: uuid}}}\n  t: {owner: {ref: o, per_parent: {max: 10}}, fields: {f: {type: reference, ref: o.id, owner: true}, a: {type: reference, ref: p.id, select: {distinct: true}}}}\n",
		"driver with enough space":   "  t: {count: 5, unique_keys: [{fields: [a, b], driver: b}], fields: {a: {type: string}, b: {type: integer, min: 1, max: 5}}}\n",
		"no primary key":             "  t: {count: 1, fields: {a: {type: string}}}\n",
		"enum key at capacity":       "  t: {count: 3, primary_key: a, fields: {a: {type: enum, values: [x, y, z]}}}\n",
	} {
		s := decode(t, relationalDoc(tables))
		if problems := Validate(s, opts()); len(problems) > 0 {
			t.Errorf("%s: %v", name, []Problem(problems))
		}
	}
}

func TestUniqueFeasibility(t *testing.T) {
	cases := []struct {
		field     string
		count     int64
		ok        bool
		unsupport bool
	}{
		{"{type: integer, min: 1, max: 100}", 100, true, false},
		{"{type: integer, min: 1, max: 100}", 101, false, false},
		{"{type: integer}", 4294967296, true, false},
		{"{type: integer}", 4294967297, false, false},
		{"{type: long, distribution: {type: sequence}}", 1 << 62, true, false},
		{"{type: uuid}", 1 << 62, true, false},
		{"{type: email}", 1 << 62, true, false},
		{"{type: boolean}", 2, false, true},
		{"{type: float}", 2, false, true},
		{"{type: first_name}", 2, false, true},
		{"{type: first_name, values: [a, b]}", 2, true, false},
		{"{type: first_name, values: [a, b]}", 3, false, false},
		{"{type: first_name, generator: custom}", 1 << 62, true, false},
		{"{type: phone}", 1 << 62, true, false},
		{`{type: string, pattern: "ORD-[0-9]{3}"}`, 1000, true, false},
		{`{type: string, pattern: "ORD-[0-9]{3}"}`, 1001, false, false},
		{`{type: string, pattern: "[A-C]{2}[0-9]"}`, 90, true, false},
		{`{type: string, pattern: "[A-C]{2}[0-9]"}`, 91, false, false},
		{`{type: string, pattern: "a|b"}`, 2, true, false}, // simplifies to the class [ab]
		{`{type: string, pattern: "cat|horse"}`, 2, false, true},
		{`{type: string, pattern: "[0-9]+"}`, 2, false, true},
		{"{type: string, max_length: 2}", 1296, true, false},
		{"{type: string, max_length: 2}", 1297, false, false},
		{"{type: string, max_length: 13}", 1 << 62, true, false},
		{`{type: date, min: "2026-01-01", max: "2026-01-10"}`, 10, true, false},
		{`{type: date, min: "2026-01-01", max: "2026-01-10"}`, 11, false, false},
		{`{type: timestamp, min: "2026-01-01T00:00:00Z", max: "2026-01-01T00:00:59Z"}`, 60, true, false},
		{`{type: timestamp, min: "2026-01-01T00:00:00Z", max: "2026-01-01T00:00:59Z"}`, 61, false, false},
		{"{type: time}", 86400, true, false},
		{"{type: time}", 86401, false, false},
		{"{type: decimal, scale: 2, min: 0, max: 1}", 101, true, false},
		{"{type: decimal, scale: 2, min: 0, max: 1}", 102, false, false},
		{"{type: ipv4}", 768, true, false},
		{"{type: ipv4}", 769, false, false},
		{"{type: ipv4, ranges: any}", 1 << 32, true, false},
		{"{type: ipv6}", 1 << 62, true, false},
		{"{type: binary, max_length: 1}", 256, true, false},
		{"{type: binary, max_length: 1}", 257, false, false},
	}
	for _, c := range cases {
		s := decode(t, "kind: hybrid\nfields:\n  f: "+strings.Replace(c.field, "{type:", "{unique: true, type:", 1)+"\n")
		problems := Validate(s, Options{Limits: DefaultLimits(), MaxRecords: c.count})
		switch {
		case c.ok && has(problems, "fields.f.unique", CodeUniqueInfeasible), c.ok && has(problems, "fields.f.unique", CodeUniqueUnsupported):
			t.Errorf("%s with %d rows rejected: %v", c.field, c.count, []Problem(problems))
		case !c.ok && c.unsupport && !has(problems, "fields.f.unique", CodeUniqueUnsupported):
			t.Errorf("%s: want unsupported, got %v", c.field, []Problem(problems))
		case !c.ok && !c.unsupport && !has(problems, "fields.f.unique", CodeUniqueInfeasible):
			t.Errorf("%s with %d rows: want infeasible, got %v", c.field, c.count, []Problem(problems))
		}
	}
}

func TestOwnedIndexSpaceDrivesFeasibility(t *testing.T) {
	doc := func(max string) string {
		return relationalDoc("  p: {count: 10, primary_key: id, fields: {id: {type: uuid}}}\n" +
			"  c: {owner: {ref: p, per_parent: {max: " + max + "}}, fields: {f: {type: reference, ref: p.id, owner: true}, code: {type: integer, unique: true, min: 1, max: 100}}}\n")
	}
	if problems := Validate(decode(t, doc("10")), opts()); len(problems) > 0 {
		t.Fatalf("10 × 10 fits 100: %v", []Problem(problems))
	}
	wantProblem(t, Validate(decode(t, doc("11")), opts()), "tables.c.fields.code.unique", CodeUniqueInfeasible)
}

func TestNestedLayout(t *testing.T) {
	doc := relationalDoc(`  p: {count: 1, primary_key: id, fields: {id: {type: uuid}}}
  c: {primary_key: id, owner: {ref: p, per_parent: {max: 50}}, fields: {id: {type: uuid}, f: {type: reference, ref: p.id, owner: true}}}
  g: {owner: {ref: c, per_parent: {max: 50}}, fields: {f: {type: reference, ref: c.id, owner: true}, s: {type: string, max_length: 100}}}
`)
	s := decode(t, doc)
	limits := DefaultLimits()

	nested := func(format, root string) Options {
		return Options{Limits: limits, Output: Output{Format: format, Nested: true, Root: root}}
	}
	wantProblem(t, Validate(s, nested("csv", "p")), "output.relational.layout", CodeFormatIncompat)
	wantProblem(t, Validate(s, nested("json", "nope")), "output.relational.root", CodeFKTarget)
	wantProblem(t, Validate(s, nested("json", "c")), "output.relational.root", CodeFormatIncompat)
	// 50 × 50 × ~500 bytes is far above 64 KiB: the worst case decides, not the average.
	wantProblem(t, Validate(s, nested("json", "p")), "tables.p", CodeRecordTooLarge)

	limits.MaxRecordBytes = 16 << 20
	if problems := Validate(s, Options{Limits: limits, Output: Output{Format: "json", Nested: true, Root: "p"}}); len(problems) > 0 {
		t.Fatalf("large enough limit: %v", []Problem(problems))
	}
}

func TestAnalyzePattern(t *testing.T) {
	big1000 := big.NewInt(1000)
	cases := []struct {
		pattern  string
		minLen   int64
		maxLen   int64
		capacity *big.Int
	}{
		{"abc", 3, 3, big.NewInt(1)},
		{"[0-9]{3}", 3, 3, big1000},
		{"^[0-9]{3}$", 3, 3, big1000},
		{"ORD-[A-F]{2}", 6, 6, big.NewInt(36)},
		{`\d`, 1, 1, big.NewInt(10)},
		{"(a)[bc]", 2, 2, big.NewInt(2)},
		{"[a-z]{2,4}", 2, 4, nil},
		{"a?", 0, 1, nil},
		{"a*", 0, PatternMaxRepeat, nil},
		{"a+", 1, PatternMaxRepeat, nil},
		{"(ab)+", 2, 2 * PatternMaxRepeat, nil},
		{"cat|horse", 3, 5, nil},
		{"(?i)abc", 3, 3, nil},
		{".", 1, 1, nil},
	}
	for _, c := range cases {
		info, err := AnalyzePattern(c.pattern)
		if err != nil {
			t.Errorf("%q: %v", c.pattern, err)
			continue
		}
		if info.MinLen != c.minLen || info.MaxLen != c.maxLen {
			t.Errorf("%q: length %d..%d, want %d..%d", c.pattern, info.MinLen, info.MaxLen, c.minLen, c.maxLen)
		}
		switch {
		case c.capacity == nil && info.Capacity != nil:
			t.Errorf("%q: capacity %v, want none", c.pattern, info.Capacity)
		case c.capacity != nil && (info.Capacity == nil || info.Capacity.Cmp(c.capacity) != 0):
			t.Errorf("%q: capacity %v, want %v", c.pattern, info.Capacity, c.capacity)
		}
	}
	for _, bad := range []string{"(", "[", `\b`, `\B`, "a{2000}"} {
		if _, err := AnalyzePattern(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestProblemsReadable(t *testing.T) {
	ps := Problems{{Path: "a", Code: "X", Message: "m"}, {Code: "Y", Message: "n"}}
	if got := ps.Error(); !strings.Contains(got, "a: X: m") || !strings.Contains(got, "1 more") {
		t.Fatalf("Error() = %q", got)
	}
	if got := ps[1].String(); got != "Y: n" {
		t.Fatalf("String() = %q", got)
	}
}

func TestEveryTypeHasASizeBound(t *testing.T) {
	for typ := range typeInfo {
		f := FieldDef{Name: "f", Type: typ}
		z := &sizer{memo: map[string]int64{}}
		switch typ {
		case TypeEnum: // sized from its values
		case TypeObject, TypeNestedSchema: // sized from its fields
		case TypeArray, TypeMap:
		default:
			if z.value(&f) <= 0 {
				t.Errorf("type %s has no size bound", typ)
			}
		}
	}
}

func FuzzDecodeNeverPanics(f *testing.F) {
	f.Add([]byte("kind: random\n"))
	f.Add([]byte(`{"kind":"hybrid","fields":{"a":{"type":"string"}}}`))
	f.Add([]byte("kind: relational\ntables: {t: {count: 1, fields: {a: {type: string}}}}\n"))
	f.Add([]byte("a: &a [*a]\n"))
	f.Add([]byte("kind: hybrid\nfields: {f: {type: enum, values: [1, x, null], weights: [1, 2]}}\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		s, problems := Decode(data, FormatAuto)
		if len(problems) > 0 {
			return
		}
		Validate(s, Options{Limits: DefaultLimits(), Output: Output{Format: "csv"}})

		// Whatever decodes must re-encode to something that decodes to the same schema.
		again, problems := Decode(EncodeJSON(s), FormatJSON)
		if len(problems) > 0 {
			t.Fatalf("re-decode: %v\n%s", []Problem(problems), EncodeJSON(s))
		}
		if string(EncodeJSON(again)) != string(EncodeJSON(s)) {
			t.Fatalf("canonical form unstable:\n%s\n%s", EncodeJSON(s), EncodeJSON(again))
		}
	})
}

func FuzzAnalyzePattern(f *testing.F) {
	for _, s := range []string{"abc", "[a-z]{1,5}", "(a|b)*", `\d+`, "^x$", "(?i)q"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, pattern string) {
		info, err := AnalyzePattern(pattern)
		if err != nil {
			return
		}
		if info.MinLen < 0 || info.MinLen > info.MaxLen {
			t.Fatalf("%q: length %d..%d", pattern, info.MinLen, info.MaxLen)
		}
		if info.Capacity != nil && info.Capacity.Sign() <= 0 {
			t.Fatalf("%q: capacity %v", pattern, info.Capacity)
		}
	})
}
