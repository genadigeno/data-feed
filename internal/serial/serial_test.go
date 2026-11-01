package serial

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
)

// sample builds a record exercising every kind.
func sample(a *record.Arena) *record.Object {
	inner := a.Object(2)
	inner.Add("city").SetString("Tbilisi")
	inner.Add("zip").SetNull()

	list := a.Values(3)
	list[0].SetInt(1)
	list[1].SetString("two")
	list[2].SetBool(false)

	o := a.Object(8)
	o.Add("id").SetInt(42)
	o.Add("name").SetString(`He said "hi", <ok> & left`)
	o.Add("price").SetDecimal("12.50")
	o.Add("ratio").SetFloat(0.25)
	o.Add("active").SetBool(true)
	o.Add("note").SetNull()
	o.Add("home").SetObject(inner)
	o.Add("tags").SetArray(list)
	return o
}

func document(t testing.TB, e Encoder, records ...*record.Object) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	e.StartPart()
	w.Write(e.Header())
	for i, r := range records {
		if err := e.Encode(w, r, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	w.Write(e.Footer())
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func mustNew(t testing.TB, format string, o Options, s Stream) Encoder {
	t.Helper()
	e, err := New(format, o, s)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func fields(t testing.TB, doc string) Stream {
	t.Helper()
	s, problems := schema.Decode([]byte(doc), schema.FormatYAML)
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	return Stream{Fields: s.Hybrid.Fields, Definitions: s.Definitions}
}

func TestJSONDocumentParsesBack(t *testing.T) {
	var a record.Arena
	for _, pretty := range []bool{false, true} {
		doc := document(t, mustNew(t, FormatJSON, Options{Pretty: pretty}, Stream{}), sample(&a), sample(&a))
		var got []map[string]any
		if err := json.Unmarshal(doc, &got); err != nil {
			t.Fatalf("pretty=%v: %v\n%s", pretty, err, doc)
		}
		if len(got) != 2 {
			t.Fatalf("%d records", len(got))
		}
		r := got[0]
		if r["id"] != float64(42) || r["name"] != `He said "hi", <ok> & left` || r["price"] != 12.5 ||
			r["ratio"] != 0.25 || r["active"] != true || r["note"] != nil {
			t.Fatalf("scalars: %v", r)
		}
		if home := r["home"].(map[string]any); home["city"] != "Tbilisi" || home["zip"] != nil {
			t.Fatalf("object: %v", home)
		}
		if tags := r["tags"].([]any); len(tags) != 3 || tags[1] != "two" {
			t.Fatalf("array: %v", tags)
		}
	}
}

func TestJSONKeepsOrderAndHTMLCharacters(t *testing.T) {
	var a record.Arena
	line := document(t, mustNew(t, FormatNDJSON, Options{}, Stream{}), sample(&a))
	s := string(line)
	if strings.Index(s, `"id"`) > strings.Index(s, `"name"`) || strings.Index(s, `"name"`) > strings.Index(s, `"tags"`) {
		t.Fatalf("order lost: %s", s)
	}
	if !strings.Contains(s, "<ok> & left") {
		t.Fatalf("HTML characters were escaped: %s", s)
	}
}

func TestEmptyDocumentsAreValid(t *testing.T) {
	var a []any
	if err := json.Unmarshal(EmptyDocument(mustNew(t, FormatJSON, Options{}, Stream{})), &a); err != nil || len(a) != 0 {
		t.Fatalf("JSON: %v %v", a, err)
	}
	var root struct{ XMLName xml.Name }
	if err := xml.Unmarshal(EmptyDocument(mustNew(t, FormatXML, Options{}, Stream{})), &root); err != nil || root.XMLName.Local != "records" {
		t.Fatalf("XML: %+v %v", root, err)
	}
	if got := EmptyDocument(mustNew(t, FormatNDJSON, Options{}, Stream{})); len(got) != 0 {
		t.Fatalf("NDJSON: %q", got)
	}
}

func TestJSONEmptyContainersAndSpecialNumbers(t *testing.T) {
	var a record.Arena
	o := a.Object(5)
	o.Add("list").SetArray(nil)
	o.Add("obj").SetObject(a.Object(0))
	o.Add("big").SetFloat(1e300)
	o.Add("tiny").SetFloat(1e-300)
	o.Add("nan").SetFloat(math.NaN())
	got := string(appendJSONObject(nil, o, compactIndent))
	if !json.Valid([]byte(got)) {
		t.Fatalf("invalid: %s", got)
	}
	if !strings.Contains(got, `"list":[]`) || !strings.Contains(got, `"obj":{}`) || !strings.Contains(got, `"nan":null`) {
		t.Fatalf("got %s", got)
	}
}

func TestFloatsMatchEncodingJSON(t *testing.T) {
	for _, f := range []float64{0, 1, -1, 0.1, 1e20, 1e21, 1e-6, 1e-7, 123456.789, math.MaxFloat64, math.SmallestNonzeroFloat64} {
		want, _ := json.Marshal(f)
		// encoding/json rewrites exponents, e-07 to e-7; both are the same number.
		got := appendJSONFloat(nil, f)
		var a, b float64
		if err := json.Unmarshal(got, &a); err != nil {
			t.Fatalf("%v: %s: %v", f, got, err)
		}
		if err := json.Unmarshal(want, &b); err != nil || a != b {
			t.Fatalf("%v: got %s, encoding/json %s", f, got, want)
		}
	}
}

// xmlNode is a generic parse of an element.
type xmlNode struct {
	XMLName  xml.Name
	Attrs    []xml.Attr `xml:",any,attr"`
	Text     string     `xml:",chardata"`
	Children []xmlNode  `xml:",any"`
}

func (n *xmlNode) child(name string) *xmlNode {
	for i := range n.Children {
		if n.Children[i].XMLName.Local == name {
			return &n.Children[i]
		}
	}
	return nil
}

func TestXMLParsesBack(t *testing.T) {
	var a record.Arena
	doc := document(t, mustNew(t, FormatXML, Options{XMLRoot: "people", XMLRecord: "person"}, Stream{}), sample(&a), sample(&a))

	var root xmlNode
	if err := xml.Unmarshal(doc, &root); err != nil {
		t.Fatalf("%v\n%s", err, doc)
	}
	if root.XMLName.Local != "people" || len(root.Children) != 2 || root.Children[0].XMLName.Local != "person" {
		t.Fatalf("root: %+v", root)
	}
	r := &root.Children[0]
	if r.child("id").Text != "42" || r.child("name").Text != `He said "hi", <ok> & left` || r.child("price").Text != "12.50" {
		t.Fatalf("scalars: %+v", r)
	}
	if r.child("home").child("city").Text != "Tbilisi" {
		t.Fatalf("object: %+v", r.child("home"))
	}
	tags := r.child("tags")
	if len(tags.Children) != 3 || tags.Children[0].XMLName.Local != "item" || tags.Children[1].Text != "two" {
		t.Fatalf("array: %+v", tags)
	}
}

func TestXMLNullHandling(t *testing.T) {
	var a record.Arena
	parse := func(null string) *xmlNode {
		doc := document(t, mustNew(t, FormatXML, Options{XMLNull: null}, Stream{}), sample(&a))
		var root xmlNode
		if err := xml.Unmarshal(doc, &root); err != nil {
			t.Fatalf("%s: %v\n%s", null, err, doc)
		}
		return &root.Children[0]
	}
	if parse("omit").child("note") != nil {
		t.Error("omit kept the element")
	}
	if n := parse("empty").child("note"); n == nil || n.Text != "" {
		t.Errorf("empty: %+v", n)
	}
	n := parse("xsi_nil").child("note")
	if n == nil || len(n.Attrs) != 1 || n.Attrs[0].Name.Local != "nil" || n.Attrs[0].Value != "true" {
		t.Errorf("xsi_nil: %+v", n)
	}
}

func TestXMLNames(t *testing.T) {
	cases := map[string]string{
		"name":       "name",
		"with space": "with_space",
		"1st":        "_1st",
		"":           "_",
		"a:b":        "a_b",
		"xmlThing":   "_xmlThing",
		"XML":        "_XML",
		"héllo":      "h_llo",
		"a-b.c_d":    "a-b.c_d",
		"<tag>":      "_tag_",
		".dot":       "_.dot",
	}
	for in, want := range cases {
		got := XMLName(in)
		if got != want {
			t.Errorf("XMLName(%q) = %q, want %q", in, got, want)
		}
		if err := xml.Unmarshal([]byte("<"+got+"/>"), new(xmlNode)); err != nil {
			t.Errorf("%q is not usable: %v", got, err)
		}
	}
}

func TestXMLOptionsValidated(t *testing.T) {
	for _, o := range []Options{{XMLRoot: "bad name"}, {XMLRecord: "1x"}, {XMLNull: "maybe"}} {
		if _, err := New(FormatXML, o, Stream{}); err == nil {
			t.Errorf("%+v accepted", o)
		}
	}
}

func TestXMLNameCacheIsBounded(t *testing.T) {
	e := mustNew(t, FormatXML, Options{}, Stream{}).(*xmlEncoder)
	for i := range xmlNameCacheLimit * 3 {
		e.name("f" + strconv.Itoa(i))
	}
	if len(e.names) > xmlNameCacheLimit {
		t.Fatalf("cache holds %d names", len(e.names))
	}
}

func readCSV(t testing.TB, doc []byte, delimiter rune) [][]string {
	t.Helper()
	r := csv.NewReader(bytes.NewReader(doc))
	if delimiter != 0 {
		r.Comma = delimiter
	}
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("%v\n%s", err, doc)
	}
	return rows
}

const csvSchema = `kind: hybrid
definitions:
  addr: {fields: {city: {type: city}, zip: {type: postal_code}}}
fields:
  id: {type: integer}
  name: {type: string}
  price: {type: decimal}
  ratio: {type: float}
  active: {type: boolean}
  note: {type: string, nullable: true}
  home: {type: nested_schema, schema: addr}
  tags: {type: array, items: {type: string}}
  missing: {type: string, presence: {optional: 0.5}}
  deep: {type: object, fields: {a: {type: object, fields: {b: {type: string}}}}}
`

func TestCSVFlattensFromTheSchema(t *testing.T) {
	var a record.Arena
	rows := readCSV(t, document(t, mustNew(t, FormatCSV, Options{}, fields(t, csvSchema)), sample(&a)), 0)

	want := []string{"id", "name", "price", "ratio", "active", "note", "home.city", "home.zip", "tags", "missing", "deep.a.b"}
	if strings.Join(rows[0], "|") != strings.Join(want, "|") {
		t.Fatalf("header: %v", rows[0])
	}
	r := rows[1]
	if r[0] != "42" || r[1] != `He said "hi", <ok> & left` || r[2] != "12.50" || r[3] != "0.25" || r[4] != "true" ||
		r[5] != "" || r[6] != "Tbilisi" || r[7] != "" || r[9] != "" || r[10] != "" {
		t.Fatalf("row: %q", r)
	}
	var tags []any
	if err := json.Unmarshal([]byte(r[8]), &tags); err != nil || len(tags) != 3 {
		t.Fatalf("array column %q: %v", r[8], err)
	}
}

// Columns must match what validation checks for collisions.
func TestCSVColumnsMatchSchemaColumns(t *testing.T) {
	stream := fields(t, csvSchema)
	defs := map[string]*schema.Definition{}
	for i := range stream.Definitions {
		defs[stream.Definitions[i].Name] = &stream.Definitions[i]
	}
	for _, cols := range []int{0, 2} {
		e := mustNew(t, FormatCSV, Options{CSVArrayColumns: cols, CSVSeparator: "__"}, stream).(*csvColumnEncoder)
		want := schema.Columns(stream.Fields, defs, "__", cols)
		if strings.Join(e.Columns(), ",") != strings.Join(want, ",") {
			t.Fatalf("columns %v, schema says %v", e.Columns(), want)
		}
	}
}

func TestCSVIndexedArraysAndOptions(t *testing.T) {
	var a record.Arena
	e := mustNew(t, FormatCSV, Options{CSVArrayColumns: 2, CSVDelimiter: ';', CSVNull: "NULL", CSVSeparator: "_"}, fields(t, csvSchema))
	rows := readCSV(t, document(t, e, sample(&a)), ';')
	header := strings.Join(rows[0], "|")
	if !strings.Contains(header, "tags_0|tags_1") || !strings.Contains(header, "home_city") {
		t.Fatalf("header: %s", header)
	}
	r := map[string]string{}
	for i, name := range rows[0] {
		r[name] = rows[1][i]
	}
	if r["tags_0"] != "1" || r["tags_1"] != "two" || r["note"] != "NULL" || r["missing"] != "NULL" {
		t.Fatalf("row: %v", r)
	}

	noHeader := mustNew(t, FormatCSV, Options{CSVNoHeader: true}, fields(t, csvSchema))
	if rows := readCSV(t, document(t, noHeader, sample(&a)), 0); len(rows) != 1 {
		t.Fatalf("%d rows without a header", len(rows))
	}
}

func TestCSVQuoting(t *testing.T) {
	var a record.Arena
	stream := fields(t, "kind: hybrid\nfields: {v: {type: string}}\n")
	values := []string{"plain", "a,b", `quo"te`, "line\nbreak", " leading", "\ttab", "", "semi;colon", `""`, ",\",\n"}
	var records []*record.Object
	for _, v := range values {
		o := a.Object(1)
		o.Add("v").SetString(v)
		records = append(records, o)
	}
	doc := document(t, mustNew(t, FormatCSV, Options{}, stream), records...)
	rows := readCSV(t, doc, 0)
	for i, want := range values {
		if rows[i+1][0] != want {
			t.Errorf("value %d: got %q, want %q", i, rows[i+1][0], want)
		}
	}
	if strings.Contains(string(doc), `"plain"`) {
		t.Error("quoted a value that does not need it")
	}
}

func TestCSVDelimiterValidated(t *testing.T) {
	for _, d := range []byte{'"', '\n', '\r', 0xC3} {
		if _, err := New(FormatCSV, Options{CSVDelimiter: d}, Stream{}); err == nil {
			t.Errorf("delimiter %q accepted", d)
		}
	}
	if _, err := New(FormatCSV, Options{CSVLayout: "wide"}, Stream{}); err == nil {
		t.Error("unknown layout accepted")
	}
	if _, err := New("yaml", Options{}, Stream{}); err == nil {
		t.Error("unknown format accepted")
	}
}

func TestCSVLongLayoutIsLossless(t *testing.T) {
	var a record.Arena
	o := sample(&a)
	o.Add("emptyList").SetArray(nil)
	o.Add("emptyObj").SetObject(a.Object(0))
	rows := readCSV(t, document(t, mustNew(t, FormatCSV, Options{}, Stream{}), o), 0)

	if strings.Join(rows[0], ",") != "record_index,path,type,value" {
		t.Fatalf("header: %v", rows[0])
	}
	got := map[string][2]string{}
	for _, r := range rows[1:] {
		if r[0] != "0" {
			t.Fatalf("index: %v", r)
		}
		got[r[1]] = [2]string{r[2], r[3]}
	}
	want := map[string][2]string{
		"id": {"integer", "42"}, "price": {"decimal", "12.50"}, "ratio": {"float", "0.25"}, "active": {"boolean", "true"},
		"note": {"null", ""}, "home.city": {"string", "Tbilisi"}, "home.zip": {"null", ""},
		"tags.0": {"integer", "1"}, "tags.1": {"string", "two"}, "tags.2": {"boolean", "false"},
		"emptyList": {"array", ""}, "emptyObj": {"object", ""},
	}
	for path, w := range want {
		if got[path] != w {
			t.Errorf("%s: got %v, want %v", path, got[path], w)
		}
	}
}

func TestCSVJSONColumnLayout(t *testing.T) {
	var a record.Arena
	rows := readCSV(t, document(t, mustNew(t, FormatCSV, Options{CSVLayout: CSVLayoutJSONColumn}, Stream{}), sample(&a)), 0)
	if strings.Join(rows[0], ",") != "record_index,record" {
		t.Fatalf("header: %v", rows[0])
	}
	var r map[string]any
	if err := json.Unmarshal([]byte(rows[1][1]), &r); err != nil || r["id"] != float64(42) {
		t.Fatalf("record %q: %v", rows[1][1], err)
	}
}

func TestContentTypesAndExtensions(t *testing.T) {
	for format, want := range map[string][2]string{
		FormatJSON: {"application/json", "json"}, FormatNDJSON: {"application/x-ndjson", "ndjson"},
		FormatXML: {"application/xml", "xml"}, FormatCSV: {"text/csv", "csv"},
	} {
		e := mustNew(t, format, Options{}, Stream{})
		if e.ContentType() != want[0] || e.Extension() != want[1] {
			t.Errorf("%s: %s %s", format, e.ContentType(), e.Extension())
		}
	}
}

func FuzzStringsSurviveEveryFormat(f *testing.F) {
	for _, s := range []string{"", "plain", `q"uote`, "new\nline", "\r\n", "<&>", "\x00\x01", "é世界", "\xff\xfe", "\xed\xa0\x80", ",;", " x", "]]>", "￾"} {
		f.Add(s, s)
	}
	f.Fuzz(func(t *testing.T, name, value string) {
		var a record.Arena
		o := a.Object(1)
		o.Add(name).SetString(value)

		// JSON: valid output, and the value intact wherever it was valid UTF-8.
		out := appendJSONObject(nil, o, compactIndent)
		var parsed map[string]string
		if err := json.Unmarshal(out, &parsed); err != nil {
			t.Fatalf("JSON %q: %v", out, err)
		}
		if got, ok := parsed[jsonClean(name)]; !ok || got != jsonClean(value) {
			t.Fatalf("JSON value: got %q, want %q", got, jsonClean(value))
		}

		// XML: always well-formed, whatever the name and value hold.
		e := mustNew(t, FormatXML, Options{}, Stream{})
		doc := document(t, e, o)
		var root xmlNode
		if err := xml.Unmarshal(doc, &root); err != nil {
			t.Fatalf("XML %q: %v", doc, err)
		}
		if len(root.Children) != 1 || len(root.Children[0].Children) != 1 {
			t.Fatalf("XML shape: %q", doc)
		}
		if got, want := root.Children[0].Children[0].Text, xmlClean(value); got != want {
			t.Fatalf("XML value: got %q, want %q", got, want)
		}

		// CSV: always parses to one header and one row, with the value intact.
		stream := Stream{Fields: []schema.FieldDef{{Name: "v", Type: schema.TypeString}}}
		ro := a.Object(1)
		ro.Add("v").SetString(value)
		rows := readCSV(t, document(t, mustNew(t, FormatCSV, Options{}, stream), ro), 0)
		if !utf8.ValidString(value) {
			return // encoding/csv rewrites invalid UTF-8
		}
		if len(rows) != 2 {
			t.Fatalf("CSV: %d rows for %q", len(rows), value)
		}
		// encoding/csv normalizes \r\n inside quotes to \n.
		if want := strings.ReplaceAll(value, "\r\n", "\n"); rows[1][0] != want {
			t.Fatalf("CSV value: got %q, want %q", rows[1][0], want)
		}
	})
}

// jsonClean replaces each invalid byte with U+FFFD, as encoding/json does.
func jsonClean(s string) string { return string([]rune(s)) }

// xmlClean is what appendXMLText should preserve: every rune XML allows.
func xmlClean(s string) string {
	var b strings.Builder
	for _, r := range s {
		if !isXMLChar(r) || r == utf8.RuneError {
			r = utf8.RuneError
		}
		b.WriteRune(r)
	}
	return b.String()
}

func benchEncode(b *testing.B, format string, s Stream) {
	var a record.Arena
	r := sample(&a)
	e := mustNew(b, format, Options{}, s)
	w := bufio.NewWriter(io.Discard)
	e.StartPart()
	b.ReportAllocs()
	for b.Loop() {
		if err := e.Encode(w, r, 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON(b *testing.B)   { benchEncode(b, FormatJSON, Stream{}) }
func BenchmarkNDJSON(b *testing.B) { benchEncode(b, FormatNDJSON, Stream{}) }
func BenchmarkXML(b *testing.B)    { benchEncode(b, FormatXML, Stream{}) }
func BenchmarkCSV(b *testing.B)    { benchEncode(b, FormatCSV, fields(b, csvSchema)) }
