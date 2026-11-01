package value

import (
	"encoding/base64"
	"math"
	"math/big"
	"math/rand/v2"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/genadigeno/data-feed/internal/gen/unique"
	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
)

var refTime = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

const draws = 3000

type fixture struct {
	gen Generator
	ctx *Ctx
}

func field(t *testing.T, doc string) *schema.FieldDef {
	t.Helper()
	s, problems := schema.Decode([]byte("kind: hybrid\nfields:\n  f: "+doc+"\n"), schema.FormatYAML)
	if len(problems) > 0 {
		t.Fatalf("decode %s: %v", doc, problems)
	}
	if problems := schema.Validate(s, schema.Options{Limits: schema.DefaultLimits()}); len(problems) > 0 {
		t.Fatalf("validate %s: %v", doc, problems)
	}
	return &s.Hybrid.Fields[0]
}

func bind(t *testing.T, f *schema.FieldDef, rows uint64) *fixture {
	t.Helper()
	r := rand.New(rand.NewPCG(1, 2))
	env := &Env{Rand: r, Key: unique.SeedKey(1), RefTime: refTime, Rows: rows}
	g, err := NewRegistry().Bind(f, env)
	if err != nil {
		t.Fatalf("bind %+v: %v", f, err)
	}
	return &fixture{gen: g, ctx: &Ctx{Rand: r, RefTime: refTime, Arena: &record.Arena{}, Ordinal: 3}}
}

func (x *fixture) next(t *testing.T, i uint64) record.Value {
	t.Helper()
	x.ctx.Index = i
	var v record.Value
	if err := x.gen.Generate(x.ctx, &v); err != nil {
		t.Fatalf("generate %d: %v", i, err)
	}
	return v
}

// each draws values and checks each one.
func each(t *testing.T, doc string, check func(v *record.Value) string) {
	t.Helper()
	x := bind(t, field(t, doc), 0)
	for i := range uint64(draws) {
		v := x.next(t, i)
		if msg := check(&v); msg != "" {
			t.Fatalf("%s: draw %d = %+v: %s", doc, i, v, msg)
		}
	}
}

func isString(v *record.Value) string {
	if v.Kind != record.KindString {
		return "not a string"
	}
	return ""
}

func TestTypesProduceValidValues(t *testing.T) {
	match := func(pattern string) func(*record.Value) string {
		re := regexp.MustCompile(pattern)
		return func(v *record.Value) string {
			if v.Kind != record.KindString || !re.MatchString(v.Str) {
				return "does not match " + pattern
			}
			return ""
		}
	}
	inList := func(list []string) func(*record.Value) string {
		set := map[string]bool{}
		for _, s := range list {
			set[s] = true
		}
		return func(v *record.Value) string {
			if !set[v.Str] {
				return "not in the list"
			}
			return ""
		}
	}
	cases := map[string]func(*record.Value) string{
		"{type: boolean}": func(v *record.Value) string {
			if v.Kind != record.KindBool {
				return "not a bool"
			}
			return ""
		},
		"{type: null}": func(v *record.Value) string {
			if v.Kind != record.KindNull {
				return "not null"
			}
			return ""
		},
		"{type: uuid}":                         match(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`),
		"{type: email}":                        match(`^[a-z]+\.[a-z]+[0-9]+@(example\.(com|net|org)|mail\.example|shop\.test)$`),
		"{type: url}":                          match(`^https://(example\.(com|net|org)|mail\.example|shop\.test)/[a-z]+/[a-z]+$`),
		"{type: phone}":                        match(`^\+1-[2-9][0-9]{2}-555-01[0-9]{2}$`),
		"{type: postal_code}":                  match(`^[0-9]{5}$`),
		"{type: address}":                      match(`^[0-9]+ [A-Za-z]+ [A-Za-z]+$`),
		"{type: full_name}":                    match(`^[A-Za-z]+ [A-Za-z]+$`),
		"{type: company_name}":                 match(`^[A-Za-z]+ [A-Za-z]+ [A-Za-z]+$`),
		"{type: currency}":                     match(`^[A-Z]{3}$`),
		"{type: country}":                      inList(countries),
		"{type: city}":                         inList(cities),
		"{type: first_name}":                   inList(firstNames),
		"{type: last_name}":                    inList(lastNames),
		"{type: credit_card_test_value}":       inList(testCardNumbers),
		"{type: payment_status}":               inList(paymentStatuses),
		"{type: order_status}":                 inList(orderStatuses),
		"{type: date}":                         match(`^\d{4}-\d{2}-\d{2}$`),
		"{type: time}":                         match(`^\d{2}:\d{2}:\d{2}$`),
		"{type: datetime}":                     match(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`),
		"{type: timestamp}":                    match(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`),
		"{type: datetime, zone: Asia/Tbilisi}": match(`\+04:00$`),
		"{type: binary, min_length: 4, max_length: 9}": func(v *record.Value) string {
			b, err := base64.StdEncoding.DecodeString(v.Str)
			if err != nil || len(b) < 4 || len(b) > 9 {
				return "bad binary"
			}
			return ""
		},
	}
	for doc, check := range cases {
		t.Run(doc, func(t *testing.T) { each(t, doc, check) })
	}
}

func TestCardValuesAreLuhnValid(t *testing.T) {
	for _, n := range testCardNumbers {
		sum, double := 0, false
		for i := len(n) - 1; i >= 0; i-- {
			d := int(n[i] - '0')
			if double {
				if d *= 2; d > 9 {
					d -= 9
				}
			}
			sum += d
			double = !double
		}
		if sum%10 != 0 {
			t.Errorf("%s fails the Luhn check", n)
		}
	}
}

func TestIPsStayInDocumentationRanges(t *testing.T) {
	v4 := []netip.Prefix{
		netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
	}
	v6 := netip.MustParsePrefix("2001:db8::/32")
	inRange := func(v *record.Value) string {
		a, err := netip.ParseAddr(v.Str)
		if err != nil {
			return err.Error()
		}
		if v6.Contains(a) {
			return ""
		}
		for _, p := range v4 {
			if p.Contains(a) {
				return ""
			}
		}
		return "outside the documentation ranges"
	}
	for _, doc := range []string{"{type: ipv4}", "{type: ipv6}", "{type: ip_address}"} {
		each(t, doc, inRange)
	}
	each(t, "{type: ipv4, ranges: any}", func(v *record.Value) string {
		if a, err := netip.ParseAddr(v.Str); err != nil || !a.Is4() {
			return "not IPv4"
		}
		return ""
	})
}

func TestNumericRanges(t *testing.T) {
	ints := map[string][2]int64{
		"{type: integer, min: 5, max: 9}":                {5, 9},
		"{type: integer, min: -3, max: -3}":              {-3, -3},
		"{type: integer}":                                {math.MinInt32, math.MaxInt32},
		"{type: long, min: 5000000000, max: 5000000005}": {5000000000, 5000000005},
		"{type: long}":                                   {math.MinInt64, math.MaxInt64},
		"{type: integer, min: 0, max: 100, distribution: {type: normal, mean: 50, stddev: 500}}": {0, 100},
		"{type: integer, min: 10, max: 100, distribution: {type: exponential, rate: 0.01}}":      {10, 100},
		"{type: integer, min: 10, max: 20, distribution: zipf}":                                  {10, 20},
		"{type: integer, min: 10, max: 20, distribution: uniform}":                               {10, 20},
	}
	for doc, r := range ints {
		seen := map[int64]bool{}
		each(t, doc, func(v *record.Value) string {
			seen[v.Int()] = true
			if v.Kind != record.KindInt || v.Int() < r[0] || v.Int() > r[1] {
				return "out of range"
			}
			return ""
		})
		if r[1]-r[0] == 4 && len(seen) != 5 {
			t.Errorf("%s: only saw %v", doc, seen)
		}
	}

	each(t, "{type: float, min: -1.5, max: 2.5}", func(v *record.Value) string {
		if v.Kind != record.KindFloat || v.Float() < -1.5 || v.Float() > 2.5 {
			return "out of range"
		}
		return ""
	})
	each(t, "{type: float, min: 0, max: 1, distribution: {type: normal, mean: 0.5, stddev: 9}}", func(v *record.Value) string {
		if v.Float() < 0 || v.Float() > 1 {
			return "normal not clamped"
		}
		return ""
	})
}

func TestZipfFavoursLowValues(t *testing.T) {
	x := bind(t, field(t, "{type: integer, min: 0, max: 99, distribution: {type: zipf, s: 1.5}}"), 0)
	low := 0
	for i := range uint64(draws) {
		v := x.next(t, i)
		if v.Int() < 10 {
			low++
		}
	}
	if low < draws/2 {
		t.Fatalf("only %d of %d draws were low", low, draws)
	}
}

func TestSequenceFollowsIndex(t *testing.T) {
	for _, doc := range []string{"{type: long, distribution: {type: sequence, start: 100, step: 5}}", "{type: long, generator: {name: sequence, params: {start: 100, step: 5}}}"} {
		x := bind(t, field(t, doc), 0)
		for _, i := range []uint64{0, 1, 7, 1000} {
			v := x.next(t, i)
			if v.Int() != 100+int64(i)*5 {
				t.Errorf("%s: index %d gave %d", doc, i, v.Int())
			}
		}
	}
}

func TestDecimalsAreExact(t *testing.T) {
	check := func(doc, lo, hi string, scale int) {
		t.Helper()
		lower, _ := new(big.Rat).SetString(lo)
		upper, _ := new(big.Rat).SetString(hi)
		each(t, doc, func(v *record.Value) string {
			if v.Kind != record.KindDecimal {
				return "not a decimal"
			}
			r, ok := new(big.Rat).SetString(v.Str)
			if !ok {
				return "unparseable"
			}
			if r.Cmp(lower) < 0 || r.Cmp(upper) > 0 {
				return "out of range"
			}
			if scale > 0 {
				dot := strings.IndexByte(v.Str, '.')
				if dot < 0 || len(v.Str)-dot-1 != scale {
					return "wrong number of decimal places"
				}
			} else if strings.Contains(v.Str, ".") {
				return "unexpected decimal point"
			}
			return ""
		})
	}
	check("{type: decimal, scale: 2, min: 0.5, max: 999}", "0.5", "999", 2)
	check("{type: decimal, scale: 0, min: -5, max: 5}", "-5", "5", 0)
	check("{type: decimal, scale: 3, min: -0.002, max: 0.002}", "-0.002", "0.002", 3)
	check("{type: money, min: 1, max: 2}", "1", "2", 2)
	check("{type: decimal, precision: 38, scale: 10, min: -1234567890123456789012345678, max: 1234567890123456789012345678}",
		"-1234567890123456789012345678", "1234567890123456789012345678", 10)
	check("{type: decimal, scale: 2, min: 0, max: 100, distribution: {type: normal, mean: 50, stddev: 5000}}", "0", "100", 2)
	check("{type: decimal, precision: 4, scale: 2}", "0", "99.99", 2)
}

func TestTemporalRanges(t *testing.T) {
	inRange := func(doc, layout, lo, hi string) {
		t.Helper()
		low, _ := time.Parse(layout, lo)
		high, _ := time.Parse(layout, hi)
		each(t, doc, func(v *record.Value) string {
			got, err := time.Parse(layout, v.Str)
			if err != nil {
				return err.Error()
			}
			if got.Before(low) || got.After(high) {
				return "out of range"
			}
			return ""
		})
	}
	inRange(`{type: date, min: "2026-01-01", max: "2026-01-05"}`, "2006-01-02", "2026-01-01", "2026-01-05")
	inRange(`{type: time, min: "08:00:00", max: "09:00:00"}`, "15:04:05", "08:00:00", "09:00:00")
	inRange(`{type: timestamp, min: "2026-01-01T00:00:00Z", max: "2026-01-02T00:00:00Z"}`, time.RFC3339, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
	inRange(`{type: datetime, min: "2026-01-01T00:00:00Z", max: "2026-01-02T00:00:00Z"}`, time.RFC3339, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
	// No bounds: the window ends at the reference time, never the wall clock.
	inRange("{type: timestamp}", time.RFC3339, "2016-11-01T00:00:00Z", "2026-11-01T12:00:00Z")

	unixLo, unixHi := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix(), time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC).Unix()
	each(t, `{type: timestamp, format: unix, min: "2026-01-01T00:00:00Z", max: "2026-01-02T00:00:00Z"}`, func(v *record.Value) string {
		if v.Kind != record.KindInt || v.Int() < unixLo || v.Int() > unixHi {
			return "out of range"
		}
		return ""
	})
	each(t, `{type: timestamp, format: unix_ms, min: "2026-01-01T00:00:00Z", max: "2026-01-02T00:00:00Z"}`, func(v *record.Value) string {
		if v.Kind != record.KindInt || v.Int() < unixLo*1000 || v.Int() >= (unixHi+1)*1000 {
			return "out of range"
		}
		return ""
	})
}

func TestStringLengths(t *testing.T) {
	cases := map[string][2]int{
		"{type: string}": {defaultStringLow, defaultStringHigh},
		"{type: string, min_length: 3, max_length: 5}": {3, 5},
		"{type: string, min_length: 0, max_length: 0}": {0, 0},
		"{type: string, max_length: 2}":                {0, 2},
		"{type: string, min_length: 40}":               {40, 40},
	}
	for doc, r := range cases {
		each(t, doc, func(v *record.Value) string {
			if len(v.Str) < r[0] || len(v.Str) > r[1] {
				return "length out of range"
			}
			return isString(v)
		})
	}
}

func TestPatternsMatch(t *testing.T) {
	for _, p := range []string{
		"ORD-[0-9]{10}", "[a-z]+", "[A-Z]{2,4}-[0-9]?", "(cat|horse)s?", `\d{3}-\d{4}`, "^start.*end$", "x|y|z", "[^a-c]{3}",
		"(?i)hello", `\w+@\w+\.test`, "[а-я]{3}", "a*b+",
	} {
		re := regexp.MustCompile("^(?:" + p + ")$")
		doc := strconv.Quote(p)
		each(t, "{type: string, pattern: "+doc+"}", func(v *record.Value) string {
			if !re.MatchString(v.Str) {
				return "does not match " + p
			}
			return ""
		})
	}
}

func TestPatternRespectsLengthLimits(t *testing.T) {
	each(t, `{type: string, pattern: "[a-z]+", min_length: 5, max_length: 7}`, func(v *record.Value) string {
		if len(v.Str) < 5 || len(v.Str) > 7 {
			return "length out of range"
		}
		return ""
	})
}

func TestValuesAndWeights(t *testing.T) {
	x := bind(t, field(t, "{type: enum, values: [a, b, c], weights: [1, 0, 3]}"), 0)
	counts := map[string]int{}
	for i := range uint64(draws) {
		v := x.next(t, i)
		counts[v.Str]++
	}
	if counts["b"] != 0 {
		t.Fatalf("zero weight drawn: %v", counts)
	}
	if ratio := float64(counts["c"]) / float64(counts["a"]); ratio < 2.4 || ratio > 3.6 {
		t.Fatalf("weights off: %v", counts)
	}

	x = bind(t, field(t, "{type: enum, values: [1, x, 2.5, true]}"), 0)
	kinds := map[record.Kind]bool{}
	for i := range uint64(200) {
		v := x.next(t, i)
		kinds[v.Kind] = true
	}
	for _, k := range []record.Kind{record.KindInt, record.KindString, record.KindDecimal, record.KindBool} {
		if !kinds[k] {
			t.Errorf("kind %d never drawn: %v", k, kinds)
		}
	}
}

func TestAliasMatchesWeights(t *testing.T) {
	weights := []float64{5, 0, 1, 10, 4}
	a := NewAlias(weights)
	r := rand.New(rand.NewPCG(3, 4))
	counts := make([]int, len(weights))
	const n = 200000
	for range n {
		counts[a.Sample(r)]++
	}
	for i, w := range weights {
		want := w / 20 * n
		if math.Abs(float64(counts[i])-want) > 4*math.Sqrt(want+1)+1 {
			t.Errorf("weight %v drew %d, want about %v", w, counts[i], want)
		}
	}
}

func TestNullableAndDefaults(t *testing.T) {
	x := bind(t, field(t, "{type: string, nullable: true, null_probability: 0.25}"), 0)
	nulls := 0
	for i := range uint64(draws) {
		v := x.next(t, i)
		if v.Kind == record.KindNull {
			nulls++
		}
	}
	if nulls < draws*20/100 || nulls > draws*30/100 {
		t.Fatalf("%d nulls of %d", nulls, draws)
	}

	x = bind(t, field(t, "{type: integer, min: 1, max: 5, default: 99, default_probability: 0.5}"), 0)
	defaults := 0
	for i := range uint64(draws) {
		v := x.next(t, i)
		if v.Int() == 99 {
			defaults++
		}
	}
	if defaults < draws*40/100 || defaults > draws*60/100 {
		t.Fatalf("%d defaults of %d", defaults, draws)
	}

	each(t, "{type: string, default: fixed}", func(v *record.Value) string {
		if v.Str != "fixed" {
			return "default not always used"
		}
		return ""
	})
}

func TestCompositeTypes(t *testing.T) {
	each(t, "{type: array, min_length: 2, max_length: 4, items: {type: integer, min: 1, max: 3}}", func(v *record.Value) string {
		if v.Kind != record.KindArray || len(v.List) < 2 || len(v.List) > 4 {
			return "bad array"
		}
		for i := range v.List {
			if v.List[i].Int() < 1 || v.List[i].Int() > 3 {
				return "bad item"
			}
		}
		return ""
	})
	each(t, "{type: object, fields: {a: {type: boolean}, b: {type: string}}}", func(v *record.Value) string {
		if v.Kind != record.KindObject || v.Obj.Fields[0].Name != "a" || v.Obj.Fields[1].Name != "b" {
			return "bad object"
		}
		return ""
	})
	each(t, "{type: map, min_length: 3, max_length: 3, key: {type: enum, values: [x, y]}, value: {type: boolean}}", func(v *record.Value) string {
		if v.Kind != record.KindObject || len(v.Obj.Fields) != 3 {
			return "bad map"
		}
		seen := map[string]bool{}
		for _, f := range v.Obj.Fields {
			if seen[f.Name] {
				return "duplicate map key"
			}
			seen[f.Name] = true
		}
		return ""
	})
}

func TestPresenceProbability(t *testing.T) {
	s, problems := schema.Decode([]byte("kind: hybrid\nfields:\n  req: {type: boolean}\n  opt: {type: boolean, presence: {optional: 0.3}}\n"), schema.FormatYAML)
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	r := rand.New(rand.NewPCG(5, 6))
	fields, err := NewRegistry().BindFields(s.Hybrid.Fields, &Env{Rand: r, Key: unique.SeedKey(1), RefTime: refTime})
	if err != nil {
		t.Fatal(err)
	}
	g := &ObjectGen{Fields: fields}
	c := &Ctx{Rand: r, Arena: &record.Arena{}}
	present := 0
	const n = 20000
	for range n {
		var o record.Object
		if err := g.Fill(c, &o); err != nil {
			t.Fatal(err)
		}
		if o.Get("req") == nil {
			t.Fatal("required field missing")
		}
		if o.Get("opt") != nil {
			present++
		}
	}
	// 4 standard deviations of a binomial with p = 0.3.
	if math.Abs(float64(present)-0.3*n) > 4*math.Sqrt(n*0.3*0.7) {
		t.Fatalf("present %d of %d", present, n)
	}
}

// Every type schema.UniqueCapacity supports must deliver exactly its capacity.
func TestUniqueValuesNeverRepeat(t *testing.T) {
	cases := []struct {
		doc  string
		rows uint64
	}{
		{"{type: integer, min: 1, max: 2000}", 2000},
		{"{type: long, distribution: {type: sequence}}", 5000},
		{"{type: long, generator: sequence}", 5000},
		{"{type: uuid}", 20000},
		{"{type: email}", 20000},
		{"{type: url}", 20000},
		{"{type: phone}", 20000},
		{"{type: ipv4}", 768},
		{"{type: ipv4, ranges: any}", 20000},
		{"{type: ipv6}", 20000},
		{"{type: ip_address}", 20000},
		{"{type: string}", 20000},
		{"{type: string, min_length: 20, max_length: 30}", 20000},
		{"{type: string, max_length: 2}", 1296},
		{"{type: binary, max_length: 1}", 256},
		{"{type: binary}", 20000},
		{`{type: string, pattern: "ORD-[0-9]{3}"}`, 1000},
		{`{type: string, pattern: "[A-C]{2}-[x-z]"}`, 27},
		{`{type: string, pattern: "[^a]{4}"}`, 20000},
		{"{type: enum, values: [a, b, c]}", 3},
		{"{type: decimal, scale: 2, min: 0, max: 10}", 1001},
		{"{type: decimal, scale: 2, min: 0.125, max: 0.375}", 25},
		{"{type: money, min: 0}", 20000},
		{`{type: date, min: "2026-01-01", max: "2026-03-01"}`, 60},
		{"{type: date}", 3653},
		{"{type: time}", 20000},
		{`{type: timestamp, min: "2026-01-01T00:00:00Z", max: "2026-01-01T00:59:59Z"}`, 3600},
		{"{type: timestamp}", 20000},
		{"{type: timestamp, format: unix}", 20000},
		{"{type: datetime}", 20000},
	}
	for _, c := range cases {
		t.Run(c.doc, func(t *testing.T) {
			f := field(t, c.doc)
			f.Unique = true
			x := bind(t, f, c.rows)
			seen := make(map[string]bool, c.rows)
			for i := range c.rows {
				v := x.next(t, i)
				key := v.Str
				if key == "" {
					key = strconv.FormatInt(v.Int(), 10)
				}
				if seen[key] {
					t.Fatalf("value %q repeated at row %d", key, i)
				}
				seen[key] = true
			}

			// A capacity the validator accepts must never exceed what is delivered.
			if capacity := schema.UniqueCapacity(f); capacity.Known && capacity.N != nil &&
				capacity.N.Cmp(new(big.Int).SetUint64(c.rows)) < 0 {
				t.Fatalf("delivered %d rows but the schema reports capacity %v", c.rows, capacity.N)
			}
		})
	}
}

func TestUniqueValuesAreStableAcrossBinds(t *testing.T) {
	f := field(t, "{type: email, unique: true}")
	a, b := bind(t, f, 0), bind(t, f, 0)
	// Draw from one row generator so that the row's RNG differs; key values must not care.
	a.ctx.Rand.Uint64()
	for _, i := range []uint64{0, 5, 1000} {
		x, y := a.next(t, i), b.next(t, i)
		if x.Str != y.Str {
			t.Fatalf("row %d: %q vs %q", i, x.Str, y.Str)
		}
	}
}

func TestUniqueRefusesTooManyRows(t *testing.T) {
	f := field(t, "{type: integer, min: 1, max: 10, unique: true}")
	r := rand.New(rand.NewPCG(1, 2))
	if _, err := NewRegistry().Bind(f, &Env{Rand: r, Key: unique.SeedKey(1), Rows: 11}); err == nil {
		t.Fatal("bound with more rows than values")
	}

	x := bind(t, f, 0)
	var v record.Value
	x.ctx.Index = 10
	if err := x.gen.Generate(x.ctx, &v); err == nil || !strings.Contains(err.Error(), "exhausted") {
		t.Fatalf("row beyond capacity: %v", err)
	}
}

func TestUniqueRefusedForTypesWithoutEncoding(t *testing.T) {
	for _, doc := range []string{"{type: boolean}", "{type: float}", "{type: first_name}", `{type: string, pattern: "[0-9]+"}`} {
		f := field(t, doc)
		f.Unique = true
		r := rand.New(rand.NewPCG(1, 2))
		g, err := NewRegistry().Bind(f, &Env{Rand: r, Key: unique.SeedKey(1)})
		if err != nil {
			continue
		}
		c := &Ctx{Rand: r, Arena: &record.Arena{}}
		var v record.Value
		if err := g.Generate(c, &v); err == nil {
			t.Errorf("%s: unique generation succeeded", doc)
		}
	}
}

type fakeVocab map[string][]string

func (f fakeVocab) Group(name string) (Sampler, bool) {
	values, ok := f[name]
	return fakeSampler(values), ok
}

type fakeSampler []string

func (s fakeSampler) Sample(r *rand.Rand) string { return s[r.IntN(len(s))] }
func (s fakeSampler) Len() int                   { return len(s) }
func (s fakeSampler) At(i int) string            { return s[i] }

func TestVocabulary(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	vocab := fakeVocab{"product_name": {"Lamp", "Desk"}, "order_status": {"NEW"}}
	env := &Env{Rand: r, Key: unique.SeedKey(1), RefTime: refTime, Vocabulary: vocab}
	c := &Ctx{Rand: r, Arena: &record.Arena{}}
	reg := NewRegistry()

	g, err := reg.Bind(field(t, "{type: string, generator: {name: vocab, params: {group: product_name}}}"), env)
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		var v record.Value
		if err := g.Generate(c, &v); err != nil {
			t.Fatal(err)
		}
		if v.Str != "Lamp" && v.Str != "Desk" {
			t.Fatalf("value %q", v.Str)
		}
	}

	g, err = reg.Bind(field(t, "{type: order_status}"), env)
	if err != nil {
		t.Fatal(err)
	}
	var v record.Value
	if err := g.Generate(c, &v); err != nil || v.Str != "NEW" {
		t.Fatalf("catalog group did not replace the built-in list: %q %v", v.Str, err)
	}

	if _, err := reg.Bind(field(t, "{type: string, generator: {name: vocab, params: {group: nope}}}"), env); err == nil {
		t.Fatal("unknown group accepted")
	}
	if _, err := reg.Bind(field(t, "{type: string, generator: {name: vocab}}"), env); err == nil {
		t.Fatal("missing group accepted")
	}

	// With no catalog at all, generation still works.
	env.Vocabulary = nil
	if _, err := reg.Bind(field(t, "{type: string, generator: {name: vocab, params: {group: product_name}}}"), env); err != nil {
		t.Fatalf("no catalog: %v", err)
	}
}

func TestGeneratorErrorsAndRegistration(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	env := &Env{Rand: r, Key: unique.SeedKey(1)}
	reg := NewRegistry()
	for _, doc := range []string{
		"{type: string, generator: nope}",
		"{type: long, generator: {name: sequence, params: {step: 0}}}",
		"{type: long, generator: {name: sequence, params: {start: x}}}",
		"{type: long, generator: {name: sequence, params: {other: 1}}}",
		"{type: datetime, zone: Not/AZone}",
	} {
		if _, err := reg.Bind(field(t, doc), env); err == nil {
			t.Errorf("%s accepted", doc)
		}
	}
	if err := reg.Register("sequence", newSequenceFactory); err == nil {
		t.Error("duplicate generator name accepted")
	}
	if err := reg.Register("mine", newSequenceFactory); err != nil {
		t.Errorf("new generator rejected: %v", err)
	}
}

func TestFieldErrorNamesThePath(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	s, _ := schema.Decode([]byte("kind: hybrid\nfields: {a: {type: object, fields: {b: {type: string, generator: nope}}}}\n"), schema.FormatYAML)
	_, err := NewRegistry().BindFields(s.Hybrid.Fields, &Env{Rand: r, Key: unique.SeedKey(1)})
	if err == nil || !strings.HasPrefix(err.Error(), "a.b: ") {
		t.Fatalf("err = %v", err)
	}
}

func BenchmarkPrimitives(b *testing.B) {
	for _, doc := range []string{
		"{type: integer}", "{type: string}", "{type: uuid}", "{type: email}", "{type: timestamp}", "{type: decimal, scale: 2, min: 0, max: 1000}",
	} {
		b.Run(doc, func(b *testing.B) {
			s, problems := schema.Decode([]byte("kind: hybrid\nfields:\n  f: "+doc+"\n"), schema.FormatYAML)
			if len(problems) > 0 {
				b.Fatal(problems)
			}
			r := rand.New(rand.NewPCG(1, 2))
			g, err := NewRegistry().Bind(&s.Hybrid.Fields[0], &Env{Rand: r, Key: unique.SeedKey(1), RefTime: refTime})
			if err != nil {
				b.Fatal(err)
			}
			c := &Ctx{Rand: r, RefTime: refTime, Arena: &record.Arena{}, Scratch: make([]byte, 0, 256)}
			var v record.Value
			b.ReportAllocs()
			for b.Loop() {
				if err := g.Generate(c, &v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
