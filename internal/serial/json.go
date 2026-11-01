package serial

import (
	"bufio"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/genadigeno/data-feed/internal/record"
)

const hexDigits = "0123456789abcdef"

// appendJSONString writes s as a JSON string. Invalid UTF-8 becomes U+FFFD, so the
// output is always valid. HTML characters stay as they are: this is data, not a page.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch c {
			case '"', '\\':
				dst = append(dst, '\\', c)
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, `�`...)
			i += size
			start = i
			continue
		}
		i += size
	}
	return append(append(dst, s[start:]...), '"')
}

// appendJSONFloat follows encoding/json's choice of notation. A non-finite value cannot
// be written in JSON, so it becomes null; generators never produce one.
func appendJSONFloat(dst []byte, f float64) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return append(dst, "null"...)
	}
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	return strconv.AppendFloat(dst, f, format, -1, 64)
}

func appendJSONValue(dst []byte, v *record.Value, indent int) []byte {
	switch v.Kind {
	case record.KindNull:
		return append(dst, "null"...)
	case record.KindBool:
		return strconv.AppendBool(dst, v.Bool())
	case record.KindInt:
		return strconv.AppendInt(dst, v.Int(), 10)
	case record.KindFloat:
		return appendJSONFloat(dst, v.Float())
	case record.KindDecimal:
		return append(dst, v.Str...)
	case record.KindString:
		return appendJSONString(dst, v.Str)
	case record.KindArray:
		if len(v.List) == 0 {
			return append(dst, '[', ']')
		}
		dst = append(dst, '[')
		for i := range v.List {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendIndent(dst, indent+1)
			dst = appendJSONValue(dst, &v.List[i], indent+1)
		}
		dst = appendIndent(dst, indent)
		return append(dst, ']')
	case record.KindObject:
		return appendJSONObject(dst, v.Obj, indent)
	}
	return append(dst, "null"...)
}

func appendJSONObject(dst []byte, o *record.Object, indent int) []byte {
	if len(o.Fields) == 0 {
		return append(dst, '{', '}')
	}
	dst = append(dst, '{')
	for i := range o.Fields {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendIndent(dst, indent+1)
		dst = appendJSONString(dst, o.Fields[i].Name)
		dst = append(dst, ':')
		if indent >= 0 {
			dst = append(dst, ' ')
		}
		dst = appendJSONValue(dst, &o.Fields[i].Val, indent+1)
	}
	dst = appendIndent(dst, indent)
	return append(dst, '}')
}

// compactIndent turns indentation off.
const compactIndent = -1 << 30

// appendIndent breaks the line for pretty output; a negative level means compact.
func appendIndent(dst []byte, level int) []byte {
	if level < 0 {
		return dst
	}
	dst = append(dst, '\n')
	for range level {
		dst = append(dst, ' ', ' ')
	}
	return dst
}

// jsonEncoder writes one array per part, one record per line.
type jsonEncoder struct {
	pretty  bool
	first   bool
	scratch []byte
}

func (e *jsonEncoder) Header() []byte      { return []byte("[\n") }
func (e *jsonEncoder) Footer() []byte      { return []byte("\n]\n") }
func (e *jsonEncoder) Separator() []byte   { return []byte(",\n") }
func (e *jsonEncoder) StartPart()          { e.first = true }
func (e *jsonEncoder) ContentType() string { return "application/json" }
func (e *jsonEncoder) Extension() string   { return "json" }

func (e *jsonEncoder) Encode(w *bufio.Writer, r *record.Object, _ uint64) error {
	buf := e.scratch[:0]
	if !e.first {
		buf = append(buf, e.Separator()...)
	}
	e.first = false
	indent := compactIndent
	if e.pretty {
		indent = 0
	}
	buf = appendJSONObject(buf, r, indent)
	e.scratch = buf
	_, err := w.Write(buf)
	return err
}

// ndjsonEncoder writes one record per line, with no envelope.
type ndjsonEncoder struct{ scratch []byte }

func (e *ndjsonEncoder) Header() []byte      { return nil }
func (e *ndjsonEncoder) Footer() []byte      { return nil }
func (e *ndjsonEncoder) Separator() []byte   { return nil }
func (e *ndjsonEncoder) StartPart()          {}
func (e *ndjsonEncoder) ContentType() string { return "application/x-ndjson" }
func (e *ndjsonEncoder) Extension() string   { return "ndjson" }

func (e *ndjsonEncoder) Encode(w *bufio.Writer, r *record.Object, _ uint64) error {
	buf := appendJSONObject(e.scratch[:0], r, compactIndent)
	buf = append(buf, '\n')
	e.scratch = buf
	_, err := w.Write(buf)
	return err
}
