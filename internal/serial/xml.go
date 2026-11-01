package serial

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/genadigeno/data-feed/internal/record"
)

const (
	xmlDeclaration = `<?xml version="1.0" encoding="UTF-8"?>` + "\n"
	xmlItem        = "item"
	xmlNullOmit    = "omit"
	xmlNullEmpty   = "empty"
	xmlNullXsiNil  = "xsi_nil"
	// xmlNameCacheLimit bounds the name cache: random schemas invent names without end.
	xmlNameCacheLimit = 4096
)

// XMLName turns any field name into a valid XML element name.
func XMLName(name string) string {
	var b strings.Builder
	for i, r := range name {
		switch {
		case r == utf8.RuneError:
			b.WriteByte('_')
		case i == 0 && !isXMLNameStart(r):
			b.WriteByte('_')
			if isXMLNameChar(r) {
				b.WriteRune(r)
			}
		case !isXMLNameChar(r):
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	s := b.String()
	// Names beginning with "xml" are reserved by the specification.
	if s == "" || (len(s) >= 3 && strings.EqualFold(s[:3], "xml")) {
		return "_" + s
	}
	return s
}

// Names stay within ASCII. XML editions disagree about which other letters are allowed,
// and parsers follow different ones, so this is what is well-formed everywhere. The colon
// is left out too, because it would declare a namespace prefix.
func isXMLNameStart(r rune) bool {
	return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func isXMLNameChar(r rune) bool {
	return isXMLNameStart(r) || r == '-' || r == '.' || (r >= '0' && r <= '9')
}

// isXMLChar reports whether XML 1.0 allows a character at all.
func isXMLChar(r rune) bool {
	switch {
	case r == 0x9 || r == 0xA || r == 0xD:
		return true
	case r >= 0x20 && r <= 0xD7FF:
		return true
	case r >= 0xE000 && r <= 0xFFFD:
		return true
	case r >= 0x10000 && r <= 0x10FFFF:
		return true
	}
	return false
}

// appendXMLText escapes text. What XML cannot hold at all becomes U+FFFD. A carriage
// return is written as a reference, as parsers otherwise normalize it away.
func appendXMLText(dst []byte, s string) []byte {
	for _, r := range s {
		switch r {
		case '<':
			dst = append(dst, "&lt;"...)
		case '>':
			dst = append(dst, "&gt;"...)
		case '&':
			dst = append(dst, "&amp;"...)
		case '\r':
			dst = append(dst, "&#xD;"...)
		default:
			if !isXMLChar(r) || r == utf8.RuneError {
				r = utf8.RuneError
			}
			dst = utf8.AppendRune(dst, r)
		}
	}
	return dst
}

type xmlEncoder struct {
	root, rec string
	null      string
	names     map[string]string
	scratch   []byte
}

func newXMLEncoder(o Options) (Encoder, error) {
	e := &xmlEncoder{root: o.XMLRoot, rec: o.XMLRecord, null: o.XMLNull, names: map[string]string{}}
	if e.root == "" {
		e.root = "records"
	}
	if e.rec == "" {
		e.rec = "record"
	}
	if e.null == "" {
		e.null = xmlNullOmit
	}
	for _, name := range []string{e.root, e.rec} {
		if XMLName(name) != name {
			return nil, fmt.Errorf("%q is not a valid XML element name", name)
		}
	}
	switch e.null {
	case xmlNullOmit, xmlNullEmpty, xmlNullXsiNil:
	default:
		return nil, fmt.Errorf("XML null handling %q: want omit, empty or xsi_nil", e.null)
	}
	return e, nil
}

func (e *xmlEncoder) Header() []byte {
	if e.null == xmlNullXsiNil {
		return []byte(xmlDeclaration + "<" + e.root + ` xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">` + "\n")
	}
	return []byte(xmlDeclaration + "<" + e.root + ">\n")
}

func (e *xmlEncoder) Footer() []byte      { return []byte("</" + e.root + ">\n") }
func (e *xmlEncoder) Separator() []byte   { return nil }
func (e *xmlEncoder) StartPart()          {}
func (e *xmlEncoder) ContentType() string { return "application/xml" }
func (e *xmlEncoder) Extension() string   { return "xml" }

func (e *xmlEncoder) name(field string) string {
	if n, ok := e.names[field]; ok {
		return n
	}
	if len(e.names) >= xmlNameCacheLimit {
		clear(e.names)
	}
	n := XMLName(field)
	e.names[field] = n
	return n
}

func (e *xmlEncoder) Encode(w *bufio.Writer, r *record.Object, _ uint64) error {
	buf := append(e.scratch[:0], '<')
	buf = append(buf, e.rec...)
	buf = append(buf, '>')
	buf = e.appendFields(buf, r)
	buf = append(buf, '<', '/')
	buf = append(buf, e.rec...)
	buf = append(buf, '>', '\n')
	e.scratch = buf
	_, err := w.Write(buf)
	return err
}

func (e *xmlEncoder) appendFields(dst []byte, o *record.Object) []byte {
	for i := range o.Fields {
		dst = e.appendElement(dst, e.name(o.Fields[i].Name), &o.Fields[i].Val)
	}
	return dst
}

func (e *xmlEncoder) appendElement(dst []byte, name string, v *record.Value) []byte {
	if v.Kind == record.KindNull {
		switch e.null {
		case xmlNullOmit:
			return dst
		case xmlNullXsiNil:
			dst = append(dst, '<')
			dst = append(dst, name...)
			return append(dst, ` xsi:nil="true"/>`...)
		}
		dst = append(dst, '<')
		dst = append(dst, name...)
		return append(dst, '/', '>')
	}

	dst = append(dst, '<')
	dst = append(dst, name...)
	dst = append(dst, '>')
	switch v.Kind {
	case record.KindBool:
		dst = strconv.AppendBool(dst, v.Bool())
	case record.KindInt:
		dst = strconv.AppendInt(dst, v.Int(), 10)
	case record.KindFloat:
		dst = appendJSONFloat(dst, v.Float())
	case record.KindDecimal:
		dst = append(dst, v.Str...)
	case record.KindString:
		dst = appendXMLText(dst, v.Str)
	case record.KindArray:
		// Array elements sit under a wrapper, so an empty array and nested arrays survive.
		for i := range v.List {
			dst = e.appendElement(dst, xmlItem, &v.List[i])
		}
	case record.KindObject:
		dst = e.appendFields(dst, v.Obj)
	}
	dst = append(dst, '<', '/')
	dst = append(dst, name...)
	return append(dst, '>')
}
