// Package serial writes records as JSON, NDJSON, XML and CSV. It knows nothing about
// how records were generated, and adding a format means adding an encoder here.
package serial

import (
	"bufio"
	"fmt"

	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
)

// Format names.
const (
	FormatJSON   = "json"
	FormatNDJSON = "ndjson"
	FormatXML    = "xml"
	FormatCSV    = "csv"
)

// Encoder renders one stream, part by part. Each part is a complete document made of
// Header, the encoded records and Footer, so a part is useful on its own; parts can
// also be merged by keeping one header, joining bodies with Separator, and keeping one
// footer. An Encoder keeps scratch state and belongs to one goroutine.
type Encoder interface {
	Header() []byte
	Footer() []byte
	Separator() []byte
	// StartPart resets per-part state, before the first record of a part.
	StartPart()
	Encode(w *bufio.Writer, r *record.Object, index uint64) error
	ContentType() string
	Extension() string
}

// Options configures encoders. Unset fields take the documented defaults.
type Options struct {
	Pretty bool // JSON only: indent nested values

	XMLRoot   string // default "records"
	XMLRecord string // default "record"
	XMLNull   string // omit (default) | empty | xsi_nil

	CSVDelimiter    byte // default ','
	CSVNoHeader     bool
	CSVNull         string // how a null prints; default empty
	CSVSeparator    string // joins flattened names; default "."
	CSVArrayColumns int    // expand arrays to this many columns; 0 keeps one JSON column
	CSVLayout       string // schemaless streams: long (default) | json_column
}

// Stream describes what is being encoded. Fields is nil for schemaless streams.
type Stream struct {
	Fields      []schema.FieldDef
	Definitions []schema.Definition
}

// New returns an encoder for a format.
func New(format string, o Options, s Stream) (Encoder, error) {
	switch format {
	case FormatJSON:
		return &jsonEncoder{pretty: o.Pretty}, nil
	case FormatNDJSON:
		return &ndjsonEncoder{}, nil
	case FormatXML:
		return newXMLEncoder(o)
	case FormatCSV:
		return newCSVEncoder(o, s)
	}
	return nil, fmt.Errorf("unknown format %q: want json, ndjson, xml or csv", format)
}

// EmptyDocument renders a part with no records, for a stream that produced nothing:
// a merged download must still be a valid document.
func EmptyDocument(e Encoder) []byte {
	return append(append([]byte{}, e.Header()...), e.Footer()...)
}
