package serial

import (
	"bufio"
	"fmt"
	"strconv"

	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
)

// Layouts for schemaless streams, whose columns cannot be known in advance.
const (
	CSVLayoutLong       = "long"        // one row per scalar: record_index, path, type, value
	CSVLayoutJSONColumn = "json_column" // one row per record: record_index, record
)

func newCSVEncoder(o Options, s Stream) (Encoder, error) {
	base := csvBase{delimiter: o.CSVDelimiter, null: o.CSVNull, noHeader: o.CSVNoHeader, separator: o.CSVSeparator}
	if base.delimiter == 0 {
		base.delimiter = ','
	}
	if base.separator == "" {
		base.separator = "."
	}
	if base.delimiter == '"' || base.delimiter == '\r' || base.delimiter == '\n' || base.delimiter >= 0x80 {
		return nil, fmt.Errorf("%q cannot be a CSV delimiter", base.delimiter)
	}

	if s.Fields != nil {
		return newCSVColumnEncoder(base, s, o.CSVArrayColumns), nil
	}
	switch o.CSVLayout {
	case "", CSVLayoutLong:
		return &csvLongEncoder{csvBase: base}, nil
	case CSVLayoutJSONColumn:
		return &csvJSONEncoder{csvBase: base}, nil
	}
	return nil, fmt.Errorf("CSV layout %q: want long or json_column", o.CSVLayout)
}

type csvBase struct {
	delimiter byte
	null      string
	noHeader  bool
	separator string
}

func (e *csvBase) Footer() []byte      { return nil }
func (e *csvBase) Separator() []byte   { return nil }
func (e *csvBase) StartPart()          {}
func (e *csvBase) ContentType() string { return "text/csv" }
func (e *csvBase) Extension() string   { return "csv" }

func (e *csvBase) header(names []string) []byte {
	if e.noHeader {
		return nil
	}
	var buf []byte
	for i, name := range names {
		if i > 0 {
			buf = append(buf, e.delimiter)
		}
		buf = e.appendField(buf, name)
	}
	return append(buf, '\n')
}

// appendField writes one field, quoted only when RFC 4180 requires it. A leading space
// is quoted too, since some readers would otherwise trim it.
func (e *csvBase) appendField(dst []byte, s string) []byte {
	if !e.needsQuotes(s) {
		return append(dst, s...)
	}
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			dst = append(dst, '"')
		}
		dst = append(dst, s[i])
	}
	return append(dst, '"')
}

func (e *csvBase) needsQuotes(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == ' ' || s[0] == '\t' {
		return true
	}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case e.delimiter, '"', '\n', '\r':
			return true
		}
	}
	return false
}

// appendValue writes a value as one field. Nested values become JSON text.
func (e *csvBase) appendValue(dst []byte, v *record.Value, scratch *[]byte) []byte {
	switch v.Kind {
	case record.KindNull:
		return e.appendField(dst, e.null)
	case record.KindBool:
		return strconv.AppendBool(dst, v.Bool())
	case record.KindInt:
		return strconv.AppendInt(dst, v.Int(), 10)
	case record.KindFloat:
		return appendJSONFloat(dst, v.Float())
	case record.KindDecimal:
		return append(dst, v.Str...)
	case record.KindString:
		return e.appendField(dst, v.Str)
	}
	*scratch = appendJSONValue((*scratch)[:0], v, compactIndent)
	return e.appendField(dst, string(*scratch))
}

// csvNode is a position in the tree of flattened columns.
type csvNode struct {
	column   int // -1 for an inner node
	children map[string]*csvNode
	// indexed expands an array's elements onto its child nodes.
	indexed bool
}

// csvColumnEncoder writes fixed columns from a known schema.
type csvColumnEncoder struct {
	csvBase
	names   []string
	root    *csvNode
	cells   []*record.Value
	scratch []byte
	json    []byte
}

func newCSVColumnEncoder(base csvBase, s Stream, arrayColumns int) *csvColumnEncoder {
	defs := make(map[string]*schema.Definition, len(s.Definitions))
	for i := range s.Definitions {
		defs[s.Definitions[i].Name] = &s.Definitions[i]
	}
	e := &csvColumnEncoder{csvBase: base}
	e.root = &csvNode{column: -1, children: map[string]*csvNode{}}
	e.addFields(e.root, "", s.Fields, defs, arrayColumns, 0)
	e.cells = make([]*record.Value, len(e.names))
	return e
}

func (e *csvColumnEncoder) addColumn(name string) int {
	e.names = append(e.names, name)
	return len(e.names) - 1
}

// addFields mirrors schema.Columns, which validation checks collisions against.
func (e *csvColumnEncoder) addFields(n *csvNode, prefix string, fields []schema.FieldDef, defs map[string]*schema.Definition, arrayColumns, depth int) {
	for i := range fields {
		if len(e.names) >= schema.MaxColumns {
			return
		}
		f := &fields[i]
		name := prefix + f.Name
		child := &csvNode{column: -1}
		n.children[f.Name] = child
		switch {
		case f.Type == schema.TypeObject && len(f.Fields) > 0 && depth < schema.MaxColumnDepth:
			child.children = map[string]*csvNode{}
			e.addFields(child, name+e.separator, f.Fields, defs, arrayColumns, depth+1)
		case f.Type == schema.TypeNestedSchema && defs[f.Schema] != nil && depth < schema.MaxColumnDepth:
			child.children = map[string]*csvNode{}
			e.addFields(child, name+e.separator, defs[f.Schema].Fields, defs, arrayColumns, depth+1)
		case f.Type == schema.TypeArray && arrayColumns > 0:
			child.children = make(map[string]*csvNode, arrayColumns)
			child.indexed = true
			for j := range arrayColumns {
				child.children[strconv.Itoa(j)] = &csvNode{column: e.addColumn(name + e.separator + strconv.Itoa(j))}
			}
		default:
			child.column = e.addColumn(name)
		}
	}
}

// Columns returns the column names in order.
func (e *csvColumnEncoder) Columns() []string { return e.names }

func (e *csvColumnEncoder) Header() []byte { return e.header(e.names) }

func (e *csvColumnEncoder) Encode(w *bufio.Writer, r *record.Object, _ uint64) error {
	clear(e.cells)
	e.place(e.root, r)
	buf := e.scratch[:0]
	for i, cell := range e.cells {
		if i > 0 {
			buf = append(buf, e.delimiter)
		}
		if cell == nil {
			// An absent optional field reads as null.
			buf = e.appendField(buf, e.null)
			continue
		}
		buf = e.appendValue(buf, cell, &e.json)
	}
	if len(buf) == 0 && len(e.cells) == 1 {
		// A blank line is no row at all to CSV readers: an empty lone field must be quoted.
		buf = append(buf, '"', '"')
	}
	buf = append(buf, '\n')
	e.scratch = buf
	_, err := w.Write(buf)
	return err
}

func (e *csvColumnEncoder) place(n *csvNode, o *record.Object) {
	for i := range o.Fields {
		child, ok := n.children[o.Fields[i].Name]
		if !ok {
			continue
		}
		v := &o.Fields[i].Val
		switch {
		case child.column >= 0:
			e.cells[child.column] = v
		case child.indexed && v.Kind == record.KindArray:
			// Elements beyond the configured columns are cut off, as documented.
			for j := range min(len(v.List), len(child.children)) {
				e.cells[child.children[strconv.Itoa(j)].column] = &v.List[j]
			}
		case v.Kind == record.KindObject:
			e.place(child, v.Obj)
		}
	}
}

// csvLongEncoder writes one row per scalar, so any shape fits fixed columns losslessly.
type csvLongEncoder struct {
	csvBase
	scratch []byte
	json    []byte
	path    []byte
}

var csvLongColumns = []string{"record_index", "path", "type", "value"}

func (e *csvLongEncoder) Header() []byte { return e.header(csvLongColumns) }

func (e *csvLongEncoder) Encode(w *bufio.Writer, r *record.Object, index uint64) error {
	buf := e.scratch[:0]
	e.path = e.path[:0]
	buf = e.appendObject(buf, r, index)
	e.scratch = buf
	_, err := w.Write(buf)
	return err
}

func (e *csvLongEncoder) appendObject(dst []byte, o *record.Object, index uint64) []byte {
	mark := len(e.path)
	for i := range o.Fields {
		if mark > 0 {
			e.path = append(e.path, e.separator...)
		}
		e.path = append(e.path, o.Fields[i].Name...)
		dst = e.appendAt(dst, &o.Fields[i].Val, index)
		e.path = e.path[:mark]
	}
	return dst
}

func (e *csvLongEncoder) appendAt(dst []byte, v *record.Value, index uint64) []byte {
	switch {
	case v.Kind == record.KindObject && len(v.Obj.Fields) > 0:
		return e.appendObject(dst, v.Obj, index)
	case v.Kind == record.KindArray && len(v.List) > 0:
		mark := len(e.path)
		for i := range v.List {
			e.path = append(e.path, e.separator...)
			e.path = strconv.AppendInt(e.path, int64(i), 10)
			dst = e.appendAt(dst, &v.List[i], index)
			e.path = e.path[:mark]
		}
		return dst
	}

	dst = strconv.AppendUint(dst, index, 10)
	dst = append(dst, e.delimiter)
	dst = e.appendField(dst, string(e.path))
	dst = append(dst, e.delimiter)
	dst = append(dst, longTypeName(v)...)
	dst = append(dst, e.delimiter)
	if v.Kind == record.KindObject || v.Kind == record.KindArray {
		// An empty container has no scalars: its row keeps it from vanishing.
		return append(dst, '\n')
	}
	dst = e.appendValue(dst, v, &e.json)
	return append(dst, '\n')
}

func longTypeName(v *record.Value) string {
	switch v.Kind {
	case record.KindNull:
		return "null"
	case record.KindBool:
		return "boolean"
	case record.KindInt:
		return "integer"
	case record.KindFloat:
		return "float"
	case record.KindDecimal:
		return "decimal"
	case record.KindString:
		return "string"
	case record.KindArray:
		return "array"
	}
	return "object"
}

// csvJSONEncoder writes one row per record, with the record as JSON text.
type csvJSONEncoder struct {
	csvBase
	scratch []byte
	json    []byte
}

var csvJSONColumns = []string{"record_index", "record"}

func (e *csvJSONEncoder) Header() []byte { return e.header(csvJSONColumns) }

func (e *csvJSONEncoder) Encode(w *bufio.Writer, r *record.Object, index uint64) error {
	e.json = appendJSONObject(e.json[:0], r, compactIndent)
	buf := strconv.AppendUint(e.scratch[:0], index, 10)
	buf = append(buf, e.delimiter)
	buf = e.appendField(buf, string(e.json))
	buf = append(buf, '\n')
	e.scratch = buf
	_, err := w.Write(buf)
	return err
}
