package gen

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
)

var refTime = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

const testSeed = 42

func parse(t testing.TB, doc string) *schema.Schema {
	t.Helper()
	s, problems := schema.Decode([]byte(doc), schema.FormatYAML)
	if len(problems) > 0 {
		t.Fatalf("decode: %v", problems)
	}
	return s
}

func build(t testing.TB, in Input) *Engine {
	t.Helper()
	if problems := schema.Validate(in.Schema, schema.Options{Limits: schema.DefaultLimits()}); len(problems) > 0 {
		t.Fatalf("validate: %v", problems)
	}
	if in.ReferenceTime.IsZero() {
		in.ReferenceTime = refTime
	}
	e, err := Build(in)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return e
}

func engine(t testing.TB, doc string) *Engine {
	t.Helper()
	return build(t, Input{Schema: parse(t, doc), Seed: testSeed})
}

// render turns a value into text with field order intact: this is what is hashed and
// compared, standing in for a serializer.
func render(b *strings.Builder, v *record.Value) {
	switch v.Kind {
	case record.KindNull:
		b.WriteString("null")
	case record.KindBool:
		b.WriteString(strconv.FormatBool(v.Bool()))
	case record.KindInt:
		b.WriteString(strconv.FormatInt(v.Int(), 10))
	case record.KindFloat:
		b.WriteString(strconv.FormatFloat(v.Float(), 'g', -1, 64))
	case record.KindDecimal:
		b.WriteString(v.Str)
	case record.KindString:
		enc, _ := json.Marshal(v.Str)
		b.Write(enc)
	case record.KindArray:
		b.WriteByte('[')
		for i := range v.List {
			if i > 0 {
				b.WriteByte(',')
			}
			render(b, &v.List[i])
		}
		b.WriteByte(']')
	case record.KindObject:
		renderObject(b, v.Obj)
	}
}

func renderObject(b *strings.Builder, o *record.Object) {
	b.WriteByte('{')
	for i := range o.Fields {
		if i > 0 {
			b.WriteByte(',')
		}
		enc, _ := json.Marshal(o.Fields[i].Name)
		b.Write(enc)
		b.WriteByte(':')
		render(b, &o.Fields[i].Val)
	}
	b.WriteByte('}')
}

func text(o *record.Object) string {
	var b strings.Builder
	renderObject(&b, o)
	return b.String()
}

// row is a generated record that outlives its batch.
type row struct {
	index uint64
	text  string
	data  map[string]any
}

// collect generates [from, to) of a stream in batches.
func collect(t testing.TB, e *Engine, id StreamID, from, to uint64, batch int) []row {
	t.Helper()
	g, err := e.NewStreamGenerator(id)
	if err != nil {
		t.Fatal(err)
	}
	var out []row
	var b record.Batch
	for next := from; next < to; {
		b.Reset()
		n, err := g.Fill(&b, next, to, batch)
		if err != nil {
			t.Fatal(err)
		}
		if n <= next {
			t.Fatalf("Fill made no progress at %d", next)
		}
		if b.Len() > batch {
			t.Fatalf("batch holds %d, limit %d", b.Len(), batch)
		}
		for i := range b.Records {
			s := text(&b.Records[i])
			out = append(out, row{index: b.Indices[i], text: s})
		}
		next = n
	}
	return out
}

func decodeRows(t testing.TB, rows []row) {
	t.Helper()
	for i := range rows {
		dec := json.NewDecoder(strings.NewReader(rows[i].text))
		dec.UseNumber()
		if err := dec.Decode(&rows[i].data); err != nil {
			t.Fatalf("row %q: %v", rows[i].text, err)
		}
	}
}

func joined(rows []row) string {
	var b strings.Builder
	for _, r := range rows {
		fmt.Fprintf(&b, "%d %s\n", r.index, r.text)
	}
	return b.String()
}

func readFile(t testing.TB, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

const randomDoc = `kind: random
fields: {min: 2, max: 6}
depth: {min: 0, max: 3}
array_length: {min: 0, max: 4}
`

func ecommerce(t testing.TB) string { return readFile(t, "../schema/testdata/ecommerce.yaml") }
func customer(t testing.TB) string  { return readFile(t, "../schema/testdata/customer.yaml") }

func smallShop(customers int) string {
	return fmt.Sprintf(`kind: relational
tables:
  customers:
    count: %d
    primary_key: id
    fields:
      id:    { type: uuid }
      email: { type: email, unique: true }
      seen:  { type: timestamp, min: "2024-01-01T00:00:00Z", max: "2025-01-01T00:00:00Z" }
  orders:
    primary_key: id
    owner: { ref: customers, per_parent: { min: 0, max: 4 } }
    fields:
      id:     { type: uuid }
      buyer:  { type: reference, ref: customers.id, owner: true }
      number: { type: string, pattern: "ORD-[0-9]{9}", unique: true }
      placed: { type: timestamp, derive: { from: owner, field: seen, offset: { min: 1h, max: 48h } } }
      copied: { type: timestamp, derive: { from: owner, field: seen } }
  products:
    count: 50
    primary_key: sku
    fields:
      sku:  { type: long, generator: { name: sequence, params: { start: 1000 } } }
      name: { type: string, max_length: 12 }
  items:
    primary_key: id
    owner: { ref: orders, per_parent: { min: 1, max: 3 } }
    unique_keys:
      - fields: [order_id, line]
    fields:
      id:       { type: uuid }
      order_id: { type: reference, ref: orders.id, owner: true }
      line:     { type: integer, generator: ordinal }
      product:  { type: reference, ref: products.sku, select: { distribution: zipf } }
      other:    { type: reference, ref: orders.id, nullable: true, null_probability: 0.2 }
      pick:     { type: reference, ref: products.sku, select: { distinct: true } }
  staff:
    count: 40
    primary_key: id
    fields:
      id:   { type: integer, distribution: { type: sequence, start: 1 } }
      boss: { type: reference, ref: staff.id, nullable: true, null_probability: 0, select: { mode: earlier } }
`, customers)
}

func allStreams(t testing.TB, e *Engine) map[StreamID][]row {
	t.Helper()
	out := map[StreamID][]row{}
	for _, s := range e.Streams() {
		rows := collect(t, e, s.ID, 0, max(s.Space, 300), 64)
		decodeRows(t, rows)
		out[s.ID] = rows
	}
	return out
}

func TestSameSeedSameBytesDifferentSeedDifferent(t *testing.T) {
	for name, doc := range map[string]string{"random": randomDoc, "hybrid": customer(t), "relational": smallShop(30)} {
		t.Run(name, func(t *testing.T) {
			a, b := allStreams(t, engine(t, doc)), allStreams(t, engine(t, doc))
			other := allStreams(t, build(t, Input{Schema: parse(t, doc), Seed: testSeed + 1}))
			differs := false
			for id := range a {
				if joined(a[id]) != joined(b[id]) {
					t.Fatalf("stream %s differs across runs", id)
				}
				differs = differs || joined(a[id]) != joined(other[id])
			}
			if !differs {
				t.Fatal("a different seed gave the same data")
			}
		})
	}
}

// The heart of the distributed design: bytes do not depend on how work is divided.
func TestOutputDoesNotDependOnPartitioning(t *testing.T) {
	for name, doc := range map[string]string{"random": randomDoc, "hybrid": customer(t), "relational": smallShop(40)} {
		t.Run(name, func(t *testing.T) {
			e := engine(t, doc)
			for _, s := range e.Streams() {
				total := max(s.Space, 1000)
				whole := joined(collect(t, e, s.ID, 0, total, 1000))

				// Seven uneven partitions on fresh generators, each with its own batch size.
				var parts strings.Builder
				cuts := []uint64{0, 1, total / 7, total / 3, total / 2, total - 3, total - 1, total}
				for i := 0; i+1 < len(cuts); i++ {
					parts.WriteString(joined(collect(t, engine(t, doc), s.ID, cuts[i], cuts[i+1], 1+i*3)))
				}
				if parts.String() != whole {
					t.Fatalf("stream %s differs when partitioned", s.ID)
				}
			}
		})
	}
}

func TestResumingMidBatchIsExact(t *testing.T) {
	e := engine(t, customer(t))
	whole := collect(t, e, StreamMain, 0, 100, 100)
	for _, cut := range []uint64{1, 37, 99} {
		tail := collect(t, e, StreamMain, cut, 100, 7)
		if joined(tail) != joined(whole[cut:]) {
			t.Fatalf("resume at %d differs", cut)
		}
	}
}

func TestReusedBatchesDoNotLeakBetweenRecords(t *testing.T) {
	e := engine(t, randomDoc)
	fresh := joined(collect(t, e, StreamMain, 0, 500, 1000))

	g, err := e.NewStreamGenerator(StreamMain)
	if err != nil {
		t.Fatal(err)
	}
	pool := record.NewPool(16)
	var reused []row
	for next := uint64(0); next < 500; {
		b := pool.Get()
		n, err := g.Fill(b, next, 500, 16)
		if err != nil {
			t.Fatal(err)
		}
		for i := range b.Records {
			reused = append(reused, row{index: b.Indices[i], text: text(&b.Records[i])})
		}
		pool.Put(b)
		next = n
	}
	if joined(reused) != fresh {
		t.Fatal("reusing batches changed the output")
	}
}

// Consecutive indices must not produce correlated first draws.
func TestNeighbouringIndicesAreUncorrelated(t *testing.T) {
	e := engine(t, "kind: hybrid\nfields: {x: {type: integer, min: 0, max: 15}}\n")
	rows := collect(t, e, StreamMain, 0, 16000, 1000)
	decodeRows(t, rows)
	counts := make([]float64, 16)
	repeats := 0
	var prev string
	for _, r := range rows {
		x := r.data["x"].(json.Number).String()
		v, _ := strconv.Atoi(x)
		counts[v]++
		if x == prev {
			repeats++
		}
		prev = x
	}
	var chi float64
	for _, c := range counts {
		chi += (c - 1000) * (c - 1000) / 1000
	}
	if chi > 50 { // the 99.99th percentile of chi-squared with 15 degrees of freedom is near 44
		t.Fatalf("not uniform, chi-squared = %v: %v", chi, counts)
	}
	if math.Abs(float64(repeats)-1000) > 200 { // one in sixteen of 16000
		t.Fatalf("%d repeats of the previous value, want about 1000", repeats)
	}
}

func TestHybridRules(t *testing.T) {
	e := engine(t, customer(t))
	rows := collect(t, e, StreamMain, 0, 5000, 500)
	decodeRows(t, rows)

	required := []string{"id", "firstName", "age", "active", "tier", "balance", "joined", "tags", "attrs", "ip"}
	present := map[string]int{}
	extras := 0
	declared := map[string]bool{"lastName": true, "email": true, "home": true, "work": true}
	for _, f := range required {
		declared[f] = true
	}
	seenIDs := map[string]bool{}
	seenEmails := map[string]bool{}
	for _, r := range rows {
		for _, f := range required {
			if _, ok := r.data[f]; !ok {
				t.Fatalf("required field %s missing: %s", f, r.text)
			}
		}
		hasExtra := false
		for k := range r.data {
			present[k]++
			if !declared[k] {
				hasExtra = true
			}
		}
		if hasExtra {
			extras++
		}
		age, _ := r.data["age"].(json.Number).Int64()
		if age < 18 || age > 90 {
			t.Fatalf("age %d", age)
		}
		id := r.data["id"].(string)
		if seenIDs[id] {
			t.Fatalf("unique id repeated: %s", id)
		}
		seenIDs[id] = true
		if email, ok := r.data["email"].(string); ok {
			if seenEmails[email] {
				t.Fatalf("unique email repeated: %s", email)
			}
			seenEmails[email] = true
		}
	}

	const n = 5000.0
	for field, p := range map[string]float64{"lastName": 0.8, "email": 0.6, "home": 0.5, "work": 0.5} {
		want := p * n
		if math.Abs(float64(present[field])-want) > 4*math.Sqrt(n*p*(1-p)) {
			t.Errorf("%s present %d times, want about %.0f", field, present[field], want)
		}
	}
	// Some extras draw no fields at all (min 1 here, so few), around 30% have some.
	if extras < 1200 || extras > 1800 {
		t.Errorf("%d records with extra fields, want about 1500", extras)
	}
}

func TestDeclaredFieldsNeverCollideWithExtras(t *testing.T) {
	e := engine(t, `kind: hybrid
fields: {value: {type: boolean}, item: {type: boolean}, count: {type: boolean}}
extra_random: {probability: 1, spec: {fields: {min: 20, max: 20}, depth: {max: 0}}}
`)
	rows := collect(t, e, StreamMain, 0, 300, 100)
	for _, r := range rows {
		seen := map[string]bool{}
		dec := json.NewDecoder(strings.NewReader(r.text))
		if _, err := dec.Token(); err != nil {
			t.Fatal(err)
		}
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				t.Fatal(err)
			}
			if seen[key.(string)] {
				t.Fatalf("duplicate key %v: %s", key, r.text)
			}
			seen[key.(string)] = true
			var skip any
			if err := dec.Decode(&skip); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func depthOf(v any) int {
	switch x := v.(type) {
	case map[string]any:
		deepest := 0
		for _, c := range x {
			deepest = max(deepest, depthOf(c))
		}
		return deepest + 1
	case []any:
		deepest := 0
		for _, c := range x {
			deepest = max(deepest, depthOf(c))
		}
		return deepest + 1
	}
	return 0
}

func TestRandomRespectsItsConfiguration(t *testing.T) {
	e := engine(t, `kind: random
fields: {min: 2, max: 5}
depth: {min: 1, max: 3}
array_length: {min: 1, max: 3}
string_length: {min: 4, max: 6}
integer: {min: 10, max: 20}
float: {min: 1, max: 2, precision: 2}
`)
	rows := collect(t, e, StreamMain, 0, 3000, 500)
	decodeRows(t, rows)

	var check func(v any, level int)
	types := map[string]int{}
	check = func(v any, level int) {
		switch x := v.(type) {
		case map[string]any:
			types["object"]++
			if len(x) < 2 || len(x) > 5 {
				t.Fatalf("object with %d fields", len(x))
			}
			for _, c := range x {
				check(c, level+1)
			}
		case []any:
			types["array"]++
			if len(x) < 1 || len(x) > 3 {
				t.Fatalf("array of %d", len(x))
			}
			for _, c := range x {
				check(c, level+1)
			}
		case string:
			types["string"]++
			if len(x) < 4 || len(x) > 6 {
				t.Fatalf("string %q", x)
			}
		case json.Number:
			f, _ := x.Float64()
			if strings.Contains(x.String(), ".") {
				types["float"]++
				if f < 1 || f > 2 {
					t.Fatalf("float %v", f)
				}
			} else {
				types["integer"]++
				if f < 1 || f > 20 {
					t.Fatalf("number %v", f)
				}
			}
		case bool:
			types["boolean"]++
		case nil:
			types["null"]++
		}
	}
	minDepth, maxDepth := 99, 0
	for _, r := range rows {
		check(r.data, 0)
		d := depthOf(r.data)
		minDepth, maxDepth = min(minDepth, d), max(maxDepth, d)
	}
	// depth counts nested levels below the top object.
	if minDepth < 2 {
		t.Errorf("shallowest record has depth %d, want at least 2", minDepth)
	}
	if maxDepth > 4 {
		t.Errorf("deepest record has depth %d, limit is 4", maxDepth)
	}
	for _, name := range []string{"object", "array", "string", "integer", "float", "boolean", "null"} {
		if types[name] == 0 {
			t.Errorf("type %s never produced: %v", name, types)
		}
	}
}

func TestRandomTypeSelection(t *testing.T) {
	onlyStrings := engine(t, "kind: random\ntypes: {string: 1}\ndepth: {max: 2}\n")
	for _, r := range collect(t, onlyStrings, StreamMain, 0, 300, 100) {
		var data map[string]any
		if err := json.Unmarshal([]byte(r.text), &data); err != nil {
			t.Fatal(err)
		}
		for _, v := range data {
			if _, ok := v.(string); !ok {
				t.Fatalf("non-string: %s", r.text)
			}
		}
	}

	// Only nested types with depth 0 must not spin or fail.
	onlyNested := engine(t, "kind: random\ntypes: {object: 1, array: 1}\ndepth: {max: 0}\n")
	collect(t, onlyNested, StreamMain, 0, 100, 100)

	probs := engine(t, "kind: random\nprobabilities: {null: 1, object: 0, array: 0}\n")
	for _, r := range collect(t, probs, StreamMain, 0, 100, 100) {
		var data map[string]any
		if err := json.Unmarshal([]byte(r.text), &data); err != nil {
			t.Fatal(err)
		}
		for _, v := range data {
			if v != nil {
				t.Fatalf("non-null: %s", r.text)
			}
		}
	}

	for name, doc := range map[string]string{
		"long":      "kind: random\ntypes: {long: 1}\n",
		"decimal":   "kind: random\ntypes: {decimal: 1}\n",
		"uuid":      "kind: random\ntypes: {uuid: 1}\n",
		"date":      "kind: random\ntypes: {date: 1}\n",
		"datetime":  "kind: random\ntypes: {datetime: 1}\n",
		"timestamp": "kind: random\ntypes: {timestamp: 1}\n",
		"boolean":   "kind: random\ntypes: {boolean: 1}\n",
		"null":      "kind: random\ntypes: {null: 1}\n",
	} {
		t.Run(name, func(t *testing.T) { collect(t, engine(t, doc), StreamMain, 0, 50, 50) })
	}
}

func TestRandomFieldNames(t *testing.T) {
	for style, re := range map[string]*regexp.Regexp{
		"words":  regexp.MustCompile(`^[a-z_0-9]+$`),
		"camel":  regexp.MustCompile(`^[a-z]+[A-Z][a-z]*(_[0-9]+)?$`),
		"snake":  regexp.MustCompile(`^[a-z]+_[a-z]+(_[0-9]+)?$`),
		"random": regexp.MustCompile(`^[a-z]{3,6}(_[0-9]+)?$`),
	} {
		doc := fmt.Sprintf("kind: random\nfield_names: {style: %s, min_len: 3, max_len: 40}\ndepth: {max: 0}\n", style)
		if style == "random" {
			doc = "kind: random\nfield_names: {style: random, min_len: 3, max_len: 6}\ndepth: {max: 0}\n"
		}
		rows := collect(t, engine(t, doc), StreamMain, 0, 200, 100)
		decodeRows(t, rows)
		for _, r := range rows {
			for k := range r.data {
				if !re.MatchString(k) {
					t.Fatalf("style %s: name %q", style, k)
				}
			}
		}
	}

	// Narrow names with many fields still stay distinct, through suffixes.
	narrow := engine(t, "kind: random\nfields: {min: 50, max: 50}\ndepth: {max: 0}\nfield_names: {style: words, min_len: 1, max_len: 1}\n")
	for _, r := range collect(t, narrow, StreamMain, 0, 20, 20) {
		var data map[string]any
		if err := json.Unmarshal([]byte(r.text), &data); err != nil {
			t.Fatal(err)
		}
		if len(data) != 50 {
			t.Fatalf("%d distinct names of 50", len(data))
		}
	}
}

func TestRandomStaysWithinItsByteBudget(t *testing.T) {
	const limit = 2000
	e := engine(t, fmt.Sprintf(`kind: random
fields: {min: 200, max: 200}
depth: {min: 0, max: 10}
array_length: {min: 100, max: 100}
string_length: {min: 500, max: 500}
max_record_bytes: %d
`, limit))
	rows := collect(t, e, StreamMain, 0, 500, 100)
	for _, r := range rows {
		if len(r.text) > limit {
			t.Fatalf("record of %d bytes, limit %d: %s", len(r.text), limit, r.text[:200])
		}
		if !json.Valid([]byte(r.text)) {
			t.Fatalf("invalid JSON: %s", r.text)
		}
	}
}

func TestDeclaredSizeBoundHolds(t *testing.T) {
	e := build(t, Input{Schema: parse(t, `kind: hybrid
definitions:
  address:
    fields:
      city: { type: city, presence: required }
      zip:  { type: postal_code, presence: { optional: 0.9 } }
fields:
  id:        { type: uuid, unique: true }
  firstName: { type: first_name }
  age:       { type: integer, min: 18, max: 90 }
  tier:      { type: enum, values: [gold, silver, bronze] }
  balance:   { type: money, min: 0, max: 100000 }
  note:      { type: string }
  email:     { type: email, unique: true }
  url:       { type: url }
  home:      { type: nested_schema, schema: address }
  tags:      { type: array, items: { type: string } }
  attrs:     { type: map, value: { type: integer } }
  ip:        { type: ip_address }
  blob:      { type: binary }
  stamp:     { type: timestamp }
`), Seed: testSeed})
	spec := e.Streams()[0]
	for _, r := range collect(t, e, StreamMain, 0, 3000, 500) {
		if int64(len(r.text)) > spec.MaxRecordBytes {
			t.Fatalf("record of %d bytes exceeds the bound %d: %s", len(r.text), spec.MaxRecordBytes, r.text)
		}
	}
}

func TestReferenceTimeIsTheOnlyClock(t *testing.T) {
	doc := "kind: hybrid\nfields: {a: {type: timestamp}, b: {type: date}}\n"
	first := joined(collect(t, build(t, Input{Schema: parse(t, doc), Seed: 1, ReferenceTime: refTime}), StreamMain, 0, 50, 50))
	second := joined(collect(t, build(t, Input{Schema: parse(t, doc), Seed: 1, ReferenceTime: refTime}), StreamMain, 0, 50, 50))
	moved := joined(collect(t, build(t, Input{Schema: parse(t, doc), Seed: 1, ReferenceTime: refTime.AddDate(1, 0, 0)}), StreamMain, 0, 50, 50))
	if first != second {
		t.Fatal("output changed between runs with the same reference time")
	}
	if first == moved {
		t.Fatal("reference time had no effect")
	}
}

func str(r row, field string) string {
	v, ok := r.data[field]
	if !ok || v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	}
	return fmt.Sprint(v)
}

func TestRelationalIntegrity(t *testing.T) {
	const customers = 200
	e := engine(t, smallShop(customers))
	rows := allStreams(t, e)
	cust, orders, products, items, staff := rows[TableStream("customers")], rows[TableStream("orders")],
		rows[TableStream("products")], rows[TableStream("items")], rows[TableStream("staff")]

	if len(cust) != customers || len(products) != 50 || len(staff) != 40 {
		t.Fatalf("counts: %d customers, %d products, %d staff", len(cust), len(products), len(staff))
	}
	if len(orders) == 0 || len(items) == 0 {
		t.Fatal("no owned rows")
	}

	unique := func(rows []row, field string) map[string]row {
		t.Helper()
		seen := make(map[string]row, len(rows))
		for _, r := range rows {
			v := str(r, field)
			if _, dup := seen[v]; dup {
				t.Fatalf("%s %q repeated", field, v)
			}
			seen[v] = r
		}
		return seen
	}
	custByID := unique(cust, "id")
	unique(cust, "email")
	orderByID := unique(orders, "id")
	unique(orders, "number")
	prodBySKU := unique(products, "sku")
	unique(items, "id")
	staffByID := unique(staff, "id")

	// Ownership: each order's buyer exists, and the count matches the declared range.
	perCustomer := map[string]int{}
	for _, o := range orders {
		buyer := str(o, "buyer")
		c, ok := custByID[buyer]
		if !ok {
			t.Fatalf("order refers to a missing customer: %s", o.text)
		}
		perCustomer[buyer]++

		// A derived copy equals the parent's value, whatever else happened in that row.
		if str(o, "copied") != str(c, "seen") {
			t.Fatalf("derived field %q differs from its source %q", str(o, "copied"), str(c, "seen"))
		}
		placed, err := time.Parse(time.RFC3339, str(o, "placed"))
		if err != nil {
			t.Fatal(err)
		}
		seen, _ := time.Parse(time.RFC3339, str(c, "seen"))
		if d := placed.Sub(seen); d < time.Hour || d > 48*time.Hour {
			t.Fatalf("offset %v outside 1h..48h", d)
		}
	}
	for buyer, n := range perCustomer {
		if n > 4 {
			t.Fatalf("customer %s owns %d orders", buyer, n)
		}
	}
	// Uniform over 0..4 leaves about a fifth of customers with none.
	if empty := customers - len(perCustomer); empty < 15 || empty > 70 {
		t.Errorf("%d customers without orders, want about 40", empty)
	}

	type key struct {
		order string
		line  string
	}
	seenLines := map[key]bool{}
	perOrder := map[string]int{}
	nullOther := 0
	skew := map[string]int{}
	picks := map[string]map[string]bool{}
	for _, it := range items {
		order := str(it, "order_id")
		if _, ok := orderByID[order]; !ok {
			t.Fatalf("item refers to a missing order: %s", it.text)
		}
		perOrder[order]++

		k := key{order, str(it, "line")}
		if seenLines[k] {
			t.Fatalf("composite key repeated: %+v", k)
		}
		seenLines[k] = true

		sku := str(it, "product")
		if _, ok := prodBySKU[sku]; !ok {
			t.Fatalf("item refers to a missing product: %s", it.text)
		}
		skew[sku]++

		// A selecting reference into an owned table must hit a row that really exists.
		if other := str(it, "other"); other == "" {
			nullOther++
		} else if _, ok := orderByID[other]; !ok {
			t.Fatalf("reference to a nonexistent order: %s", it.text)
		}

		pick := str(it, "pick")
		if _, ok := prodBySKU[pick]; !ok {
			t.Fatalf("distinct pick is not a product: %s", it.text)
		}
		if picks[order] == nil {
			picks[order] = map[string]bool{}
		}
		if picks[order][pick] {
			t.Fatalf("order %s repeats pick %s", order, pick)
		}
		picks[order][pick] = true
	}
	for order, n := range perOrder {
		if n < 1 || n > 3 {
			t.Fatalf("order %s has %d items", order, n)
		}
	}
	if len(perOrder) != len(orders) {
		t.Fatalf("per_parent.min is 1, but %d of %d orders have items", len(perOrder), len(orders))
	}
	if p := float64(nullOther) / float64(len(items)); p < 0.1 || p > 0.3 {
		t.Errorf("null share %.2f, want about 0.2", p)
	}
	// Zipf concentrates on few products.
	counts := make([]int, 0, len(skew))
	for _, n := range skew {
		counts = append(counts, n)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(counts)))
	if counts[0] < len(items)/10 {
		t.Errorf("zipf looks uniform: top product has %d of %d items", counts[0], len(items))
	}

	// Self references point strictly backwards, and the first row has none.
	for i, s := range staff {
		boss := str(s, "boss")
		if i == 0 {
			if boss != "" {
				t.Fatalf("first row has boss %q", boss)
			}
			continue
		}
		b, ok := staffByID[boss]
		if !ok {
			t.Fatalf("missing boss: %s", s.text)
		}
		if b.index >= s.index {
			t.Fatalf("boss %d is not earlier than %d", b.index, s.index)
		}
	}
}

func TestStreamsListParentsFirst(t *testing.T) {
	e := engine(t, smallShop(10))
	position := map[StreamID]int{}
	for i, s := range e.Streams() {
		position[s.ID] = i
	}
	if position[TableStream("customers")] > position[TableStream("orders")] ||
		position[TableStream("orders")] > position[TableStream("items")] {
		t.Fatalf("order: %v", position)
	}
	if got := len(e.Streams()); got != 5 {
		t.Fatalf("%d streams", got)
	}
}

func TestSparseIndexSpace(t *testing.T) {
	e := engine(t, smallShop(100))
	var orders StreamSpec
	for _, s := range e.Streams() {
		if s.ID == TableStream("orders") {
			orders = s
		}
	}
	if orders.Space != 400 || orders.PerParent != 4 {
		t.Fatalf("space %d, per parent %d", orders.Space, orders.PerParent)
	}
	rows := collect(t, e, orders.ID, 0, orders.Space, 50)
	if float64(len(rows)) < orders.ExpectedRows/2 || float64(len(rows)) > orders.ExpectedRows*2 {
		t.Fatalf("%d rows, expected about %v", len(rows), orders.ExpectedRows)
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].index <= rows[i-1].index {
			t.Fatalf("indices not ascending: %d then %d", rows[i-1].index, rows[i].index)
		}
	}
}

func TestLongEmptyStretchesReturnEarly(t *testing.T) {
	e := engine(t, `kind: relational
tables:
  p: { count: 9000000, primary_key: id, fields: { id: { type: uuid } } }
  c:
    primary_key: id
    owner: { ref: p, per_parent: { min: 0, max: 1 } }
    fields:
      id: { type: uuid }
      f: { type: reference, ref: p.id, owner: true }
`)
	g, err := e.NewStreamGenerator(TableStream("c"))
	if err != nil {
		t.Fatal(err)
	}
	var b record.Batch
	next, err := g.Fill(&b, 0, 9000000, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if next >= 9000000 || next == 0 {
		t.Fatalf("Fill scanned to %d in one call", next)
	}
}

func TestNestedLayoutMatchesNormalized(t *testing.T) {
	doc := smallShop(25)
	nested := build(t, Input{Schema: parse(t, doc), Seed: testSeed, NestedRoot: "customers"})
	normal := engine(t, doc)

	if streams := nested.Streams(); len(streams) != 1 || streams[0].ID != TableStream("customers") {
		t.Fatalf("streams: %+v", streams)
	}
	rows := collect(t, nested, TableStream("customers"), 0, 25, 10)
	decodeRows(t, rows)

	flat := allStreams(t, normal)
	var nestedOrders, nestedItems []string
	for i, r := range rows {
		if str(r, "id") != str(flat[TableStream("customers")][i], "id") {
			t.Fatalf("root %d differs", i)
		}
		orders, ok := r.data["orders"].([]any)
		if !ok {
			t.Fatalf("no orders array: %s", r.text)
		}
		for _, o := range orders {
			order := o.(map[string]any)
			nestedOrders = append(nestedOrders, order["id"].(string))
			for _, it := range order["items"].([]any) {
				nestedItems = append(nestedItems, it.(map[string]any)["id"].(string))
			}
		}
	}

	ids := func(rows []row) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = str(r, "id")
		}
		return out
	}
	if !slices.Equal(nestedOrders, ids(flat[TableStream("orders")])) {
		t.Fatal("nested orders differ from the normalized table")
	}
	if !slices.Equal(nestedItems, ids(flat[TableStream("items")])) {
		t.Fatal("nested items differ from the normalized table")
	}
}

func TestBuildErrors(t *testing.T) {
	cases := map[string]Input{
		"no schema":       {},
		"no content":      {Schema: &schema.Schema{}},
		"bad generator":   {Schema: parse(t, "kind: hybrid\nfields: {a: {type: string, generator: nope}}\n")},
		"too many rows":   {Schema: parse(t, "kind: hybrid\nfields: {a: {type: integer, min: 1, max: 3, unique: true}}\n"), MaxRows: 4},
		"ordinal on root": {Schema: parse(t, "kind: relational\ntables: {t: {count: 1, fields: {a: {type: integer, generator: ordinal}}}}\n")},
		"nested non-root": {Schema: parse(t, smallShop(5)), NestedRoot: "orders"},
		"nested unknown":  {Schema: parse(t, smallShop(5)), NestedRoot: "nope"},
		"phone too few":   {Schema: parse(t, "kind: relational\ntables: {t: {count: 80001, fields: {a: {type: phone, unique: true}}}}\n")},
	}
	for name, in := range cases {
		if _, err := Build(in); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	e := engine(t, randomDoc)
	if _, err := e.NewStreamGenerator("nope"); err == nil {
		t.Error("unknown stream accepted")
	}
	r := engine(t, smallShop(5))
	if _, err := r.NewStreamGenerator(StreamMain); err == nil {
		t.Error("unknown relational stream accepted")
	}
}

func TestUniqueExhaustionFailsWithAClearError(t *testing.T) {
	e := build(t, Input{Schema: parse(t, "kind: hybrid\nfields: {a: {type: integer, min: 1, max: 3, unique: true}}\n"), Seed: 1})
	g, err := e.NewStreamGenerator(StreamMain)
	if err != nil {
		t.Fatal(err)
	}
	var b record.Batch
	next, err := g.Fill(&b, 0, 10, 10)
	if err == nil || !strings.Contains(err.Error(), "exhausted") {
		t.Fatalf("err = %v", err)
	}
	if next != 3 || b.Len() != 3 {
		t.Fatalf("stopped at %d with %d records", next, b.Len())
	}
}

func TestPlanCoversEachIndexOnce(t *testing.T) {
	e := engine(t, smallShop(1000))
	cfg := PlanConfig{UnitRecords: 100, UnitMaxBytes: 1 << 40}
	units, err := PlanBounded(e.Streams(), 0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range e.Streams() {
		var pos, seq uint64
		for _, u := range units {
			if u.Stream != s.ID {
				continue
			}
			if u.Start != pos || u.End <= u.Start || u.Seq != seq {
				t.Fatalf("stream %s: unit %+v at position %d", s.ID, u, pos)
			}
			if s.PerParent > 1 && u.End != s.Space && u.End%s.PerParent != 0 {
				t.Fatalf("stream %s: unit %+v splits a parent", s.ID, u)
			}
			pos = u.End
			seq++
		}
		if pos != s.Space {
			t.Fatalf("stream %s: covered %d of %d", s.ID, pos, s.Space)
		}

		// Units of sparse streams hold about the requested rows, not a tenth of them.
		if s.ID == TableStream("orders") {
			var rows int
			first := units[slices.IndexFunc(units, func(u UnitSpec) bool { return u.Stream == s.ID })]
			rows = len(collect(t, e, s.ID, first.Start, first.End, 1000))
			if rows < 50 || rows > 200 {
				t.Errorf("first orders unit has %d rows, want about 100", rows)
			}
		}
	}

	// Whatever the units, generating them all equals generating everything at once.
	for _, s := range e.Streams() {
		var parts strings.Builder
		for _, u := range units {
			if u.Stream == s.ID {
				parts.WriteString(joined(collect(t, e, s.ID, u.Start, u.End, 37)))
			}
		}
		if parts.String() != joined(collect(t, e, s.ID, 0, s.Space, 1000)) {
			t.Fatalf("stream %s differs when split into planned units", s.ID)
		}
	}
}

func TestPlanCounts(t *testing.T) {
	flat := StreamSpec{ID: StreamMain, PerParent: 1}
	cfg := PlanConfig{UnitRecords: 10}
	if _, err := PlanBounded([]StreamSpec{flat}, 0, cfg); err == nil {
		t.Fatal("no count accepted")
	}
	units, err := PlanBounded([]StreamSpec{flat}, 25, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 3 || units[2].Start != 20 || units[2].End != 25 {
		t.Fatalf("units: %+v", units)
	}

	// The byte budget shrinks units of large records.
	big := StreamSpec{ID: StreamMain, PerParent: 1, MaxRecordBytes: 4000}
	units, err = PlanBounded([]StreamSpec{big}, 1000, PlanConfig{UnitRecords: 500, UnitMaxBytes: 10000})
	if err != nil {
		t.Fatal(err)
	}
	if units[0].End != 10 {
		t.Fatalf("first unit: %+v", units[0])
	}

	// Near the top of the index space, a unit must not wrap around.
	huge := StreamSpec{ID: StreamMain, PerParent: 1}
	cur := Cursor{Next: ^uint64(0) - 5}
	if _, _, err := NextUnit(huge, cfg, cur, 0); err == nil {
		t.Fatal("wrapped past the end of the index space")
	}
}

func TestNextUnit(t *testing.T) {
	cfg := PlanConfig{UnitRecords: 10}
	open := StreamSpec{ID: StreamMain, PerParent: 1}

	var cur Cursor
	u, cur, err := NextUnit(open, cfg, cur, 0)
	if err != nil || u.Start != 0 || u.End != 10 || u.Seq != 0 {
		t.Fatalf("first: %+v %v", u, err)
	}
	u, cur, err = NextUnit(open, cfg, cur, 0)
	if err != nil || u.Start != 10 || u.End != 20 || u.Seq != 1 {
		t.Fatalf("second: %+v %v", u, err)
	}

	// A record limit ends the stream exactly on the limit.
	u, cur, err = NextUnit(open, cfg, cur, 25)
	if err != nil || u.End != 25 {
		t.Fatalf("limited: %+v %v", u, err)
	}
	if _, _, err = NextUnit(open, cfg, cur, 25); err != ErrStreamEnd {
		t.Fatalf("after the limit: %v", err)
	}

	// A table's own size also ends it.
	table := StreamSpec{ID: "table:t", Space: 15, PerParent: 1}
	u, cur, err = NextUnit(table, cfg, Cursor{Next: 10}, 0)
	if err != nil || u.End != 15 {
		t.Fatalf("table: %+v %v", u, err)
	}
	if _, _, err = NextUnit(table, cfg, cur, 0); err != ErrStreamEnd {
		t.Fatalf("after the table: %v", err)
	}
}

// goldenSchemas pin the engine's output. If this fails, generated bytes changed: bump
// gen.Version, then run with UPDATE_GOLDEN=1.
func goldenSchemas(t *testing.T) map[string]Input {
	return map[string]Input{
		"random":     {Schema: parse(t, randomDoc), Seed: 7},
		"hybrid":     {Schema: parse(t, customer(t)), Seed: 7},
		"relational": {Schema: parse(t, ecommerce(t)), Seed: 7},
		"nested":     {Schema: parse(t, smallShop(10)), Seed: 7, NestedRoot: "customers"},
	}
}

const goldenFile = "testdata/golden.txt"

func TestGoldenOutput(t *testing.T) {
	var lines []string
	for name, in := range goldenSchemas(t) {
		e := build(t, in)
		for _, s := range e.Streams() {
			h := sha256.New()
			for _, r := range collect(t, e, s.ID, 0, min(max(s.Space, 1000), 20000), 256) {
				fmt.Fprintf(h, "%d %s\n", r.index, r.text)
			}
			lines = append(lines, fmt.Sprintf("v%d %s %s %s", Version, name, s.ID, hex.EncodeToString(h.Sum(nil))))
		}
	}
	slices.Sort(lines)
	got := strings.Join(lines, "\n") + "\n"

	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.MkdirAll("testdata", 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenFile, []byte(got), 0o640); err != nil {
			t.Fatal(err)
		}
		return
	}
	want := strings.ReplaceAll(readFile(t, goldenFile), "\r\n", "\n")
	if got != want {
		t.Fatalf("generated output changed.\nIf that is intended, bump gen.Version and run with UPDATE_GOLDEN=1.\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func FuzzGenerateNeverPanics(f *testing.F) {
	for _, doc := range []string{
		randomDoc,
		"kind: hybrid\nfields: {a: {type: enum, values: [1, x]}, b: {type: array, items: {type: uuid}}}\n",
		"kind: hybrid\nfields: {a: {type: decimal, scale: 2, min: 0.001, max: 0.002}}\n",
		"kind: relational\ntables: {t: {count: 3, primary_key: i, fields: {i: {type: uuid}}}}\n",
		"kind: relational\ntables: {p: {count: 2, primary_key: i, fields: {i: {type: uuid}}}, c: {owner: {ref: p, per_parent: {min: 0, max: 2}}, fields: {r: {type: reference, ref: p.i, owner: true}}}}\n",
		"kind: hybrid\nfields: {a: {type: string, pattern: \"[a-z]+\", min_length: 2, max_length: 3}}\n",
	} {
		f.Add([]byte(doc), uint64(1))
	}
	f.Fuzz(func(t *testing.T, data []byte, seed uint64) {
		s, problems := schema.Decode(data, schema.FormatAuto)
		if len(problems) > 0 {
			return
		}
		limits := schema.Limits{MaxDepth: 4, MaxFields: 16, MaxArrayLength: 8, MaxStringLength: 64, MaxRecordBytes: 64 << 10}
		if problems := schema.Validate(s, schema.Options{Limits: limits}); len(problems) > 0 {
			return
		}
		e, err := Build(Input{Schema: s, Seed: seed, ReferenceTime: refTime})
		if err != nil {
			return // a clear refusal is fine; a panic is not
		}
		for _, spec := range e.Streams() {
			g, err := e.NewStreamGenerator(spec.ID)
			if err != nil {
				t.Fatal(err)
			}
			var b record.Batch
			// A generation error can be legitimate; output that is produced must be valid.
			_, _ = g.Fill(&b, 0, min(max(spec.Space, 20), 20), 20)
			for i := range b.Records {
				if rendered := text(&b.Records[i]); !strings.HasPrefix(rendered, "{") {
					t.Fatalf("bad record: %q", rendered)
				}
			}
		}
	})
}

func benchFill(b *testing.B, in Input, id StreamID) {
	b.Helper()
	if in.ReferenceTime.IsZero() {
		in.ReferenceTime = refTime
	}
	e, err := Build(in)
	if err != nil {
		b.Fatal(err)
	}
	g, err := e.NewStreamGenerator(id)
	if err != nil {
		b.Fatal(err)
	}
	var batch record.Batch
	var next uint64
	b.ReportAllocs()
	for b.Loop() {
		batch.Reset()
		n, err := g.Fill(&batch, next, next+1000, 1000)
		if err != nil {
			b.Fatal(err)
		}
		next = n
		b.ReportMetric(1000, "records/op")
	}
}

func BenchmarkRandomRecords(b *testing.B) {
	benchFill(b, Input{Schema: parse(b, randomDoc), Seed: 1}, StreamMain)
}

func BenchmarkHybridRecords(b *testing.B) {
	benchFill(b, Input{Schema: parse(b, customer(b)), Seed: 1}, StreamMain)
}

func BenchmarkRelationalRows(b *testing.B) {
	benchFill(b, Input{Schema: parse(b, smallShop(100000000)), Seed: 1}, TableStream("customers"))
}
