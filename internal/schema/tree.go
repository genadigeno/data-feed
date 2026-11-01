package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Limits protecting the parser from hostile documents. YAML aliases count every
// time they are expanded, which defuses alias bombs.
const (
	maxTreeDepth = 128
	maxTreeNodes = 2_000_000
)

type nodeKind uint8

const (
	nodeNull nodeKind = iota
	nodeBool
	nodeNumber
	nodeString
	nodeList
	nodeMap
)

func (k nodeKind) String() string {
	return [...]string{"null", "boolean", "number", "string", "list", "mapping"}[k]
}

// node is a format-independent document tree. Unlike a Go map it keeps key order,
// which is meaningful here: field order decides record layout and CSV columns.
type node struct {
	kind  nodeKind
	text  string
	keys  []string // nodeMap
	items []*node  // nodeList elements, or nodeMap values parallel to keys
	index map[string]int
}

// get finds a mapping's value. The index keeps huge hostile documents linear to read.
func (n *node) get(key string) *node {
	if i, ok := n.index[key]; ok {
		return n.items[i]
	}
	return nil
}

func (n *node) put(key string, value *node) {
	if n.index == nil {
		n.index = map[string]int{}
	}
	n.index[key] = len(n.keys)
	n.keys = append(n.keys, key)
	n.items = append(n.items, value)
}

// Format is a document syntax.
type Format uint8

const (
	FormatAuto Format = iota
	FormatYAML
	FormatJSON
)

func parseTree(data []byte, format Format) (*node, error) {
	switch format {
	case FormatJSON:
		return parseJSON(data)
	case FormatYAML:
		return parseYAML(data)
	}
	if bytes.HasPrefix(bytes.TrimSpace(data), []byte("{")) {
		// Probably JSON, but this may also be YAML flow style. JSON parsing also tolerates
		// the tab indentation that YAML forbids, hence not simply always using YAML.
		if n, err := parseJSON(data); err == nil {
			return n, nil
		}
	}
	return parseYAML(data)
}

// Limits on what parsing may cost. Aliases let a small YAML document expand into a large
// one, so these count what is produced, not what was received.
const (
	maxTreeTextBytes  = 16 << 20
	maxNumberText     = 64
	maxNumberExponent = 400 // beyond any float64; big.Rat's cost grows with this
)

type budget struct {
	nodes int
	text  int
}

// spendText charges keys and values.
func (b *budget) spendText(n int) error {
	b.text += n
	if b.text > maxTreeTextBytes {
		return fmt.Errorf("more than %d bytes of text once expanded", maxTreeTextBytes)
	}
	return nil
}

// checkNumber refuses numbers that are costly to work with and useless for generation.
func checkNumber(text string) error {
	if len(text) > maxNumberText {
		return fmt.Errorf("a number with more than %d characters", maxNumberText)
	}
	if i := strings.IndexAny(text, "eE"); i >= 0 {
		exp, err := strconv.Atoi(strings.TrimPrefix(text[i+1:], "+"))
		if err == nil && (exp > maxNumberExponent || exp < -maxNumberExponent) {
			return fmt.Errorf("a number with an exponent beyond %d", maxNumberExponent)
		}
	}
	return nil
}

func (b *budget) spend(depth int) error {
	if depth > maxTreeDepth {
		return fmt.Errorf("nesting deeper than %d levels", maxTreeDepth)
	}
	b.nodes++
	if b.nodes > maxTreeNodes {
		return fmt.Errorf("more than %d nodes", maxTreeNodes)
	}
	return nil
}

func parseYAML(data []byte) (*node, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var doc yaml.Node
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("document is empty")
		}
		return nil, err
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("expected a single YAML document")
	}
	return fromYAML(&doc, 0, &budget{})
}

func fromYAML(y *yaml.Node, depth int, b *budget) (*node, error) {
	if err := b.spend(depth); err != nil {
		return nil, err
	}
	switch y.Kind {
	case yaml.DocumentNode:
		return fromYAML(y.Content[0], depth, b)
	case yaml.AliasNode:
		return fromYAML(y.Alias, depth+1, b)
	case yaml.SequenceNode:
		n := &node{kind: nodeList, items: make([]*node, 0, len(y.Content))}
		for _, c := range y.Content {
			item, err := fromYAML(c, depth+1, b)
			if err != nil {
				return nil, err
			}
			n.items = append(n.items, item)
		}
		return n, nil
	case yaml.MappingNode:
		n := &node{kind: nodeMap}
		for i := 0; i+1 < len(y.Content); i += 2 {
			k := y.Content[i]
			if k.Kind != yaml.ScalarNode || k.ShortTag() == "!!merge" {
				return nil, fmt.Errorf("line %d: mapping keys must be plain scalars", k.Line)
			}
			if err := b.spendText(len(k.Value)); err != nil {
				return nil, err
			}
			if n.get(k.Value) != nil {
				return nil, fmt.Errorf("line %d: duplicate key %q", k.Line, k.Value)
			}
			value, err := fromYAML(y.Content[i+1], depth+1, b)
			if err != nil {
				return nil, err
			}
			n.put(k.Value, value)
		}
		return n, nil
	case yaml.ScalarNode:
		if err := b.spendText(len(y.Value)); err != nil {
			return nil, err
		}
		n, err := yamlScalar(y)
		if err == nil && n.kind == nodeNumber {
			err = checkNumber(n.text)
		}
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", y.Line, err)
		}
		return n, nil
	}
	return nil, fmt.Errorf("line %d: unsupported YAML node", y.Line)
}

func yamlScalar(y *yaml.Node) (*node, error) {
	switch y.ShortTag() {
	case "!!null":
		return &node{kind: nodeNull}, nil
	case "!!bool":
		var v bool
		if err := y.Decode(&v); err != nil {
			return nil, err
		}
		return &node{kind: nodeBool, text: strconv.FormatBool(v)}, nil
	case "!!int":
		var i int64
		if err := y.Decode(&i); err == nil {
			return &node{kind: nodeNumber, text: strconv.FormatInt(i, 10)}, nil
		}
		var u uint64
		if err := y.Decode(&u); err == nil {
			return &node{kind: nodeNumber, text: strconv.FormatUint(u, 10)}, nil
		}
		return nil, fmt.Errorf("line %d: integer %q out of range", y.Line, y.Value)
	case "!!float":
		text, err := normalizeFloat(y.Value)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", y.Line, err)
		}
		return &node{kind: nodeNumber, text: text}, nil
	default: // !!str, and !!timestamp, !!binary, which keep their source text
		return &node{kind: nodeString, text: y.Value}, nil
	}
}

var jsonNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// normalizeFloat keeps exact source text if it is already a valid JSON number, and
// otherwise rewrites YAML spellings such as .5 and +1.5. Infinity and NaN are refused.
func normalizeFloat(s string) (string, error) {
	if jsonNumber.MatchString(s) {
		return s, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f != f || f > 1.7976931348623157e308 || f < -1.7976931348623157e308 {
		return "", fmt.Errorf("%q is not a finite number", s)
	}
	return strconv.FormatFloat(f, 'f', -1, 64), nil
}

func parseJSON(data []byte) (*node, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	n, err := fromJSON(dec, 0, &budget{})
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("document is empty or truncated")
		}
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after the JSON document")
	}
	return n, nil
}

func fromJSON(dec *json.Decoder, depth int, b *budget) (*node, error) {
	if err := b.spend(depth); err != nil {
		return nil, err
	}
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch v := tok.(type) {
	case nil:
		return &node{kind: nodeNull}, nil
	case bool:
		return &node{kind: nodeBool, text: strconv.FormatBool(v)}, nil
	case json.Number:
		if err := checkNumber(v.String()); err != nil {
			return nil, err
		}
		return &node{kind: nodeNumber, text: v.String()}, nil
	case string:
		if err := b.spendText(len(v)); err != nil {
			return nil, err
		}
		return &node{kind: nodeString, text: v}, nil
	case json.Delim:
		if v == '[' {
			n := &node{kind: nodeList}
			for dec.More() {
				item, err := fromJSON(dec, depth+1, b)
				if err != nil {
					return nil, err
				}
				n.items = append(n.items, item)
			}
			_, err := dec.Token()
			return n, err
		}
		n := &node{kind: nodeMap}
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyTok.(string)
			if !ok {
				return nil, errors.New("object keys must be strings")
			}
			if n.get(key) != nil {
				return nil, fmt.Errorf("duplicate key %q", key)
			}
			value, err := fromJSON(dec, depth+1, b)
			if err != nil {
				return nil, err
			}
			n.put(key, value)
		}
		_, err := dec.Token()
		return n, err
	}
	return nil, fmt.Errorf("unexpected JSON token %v", tok)
}

// writeJSON renders canonical JSON: no extra whitespace, keys in tree order. The
// same schema always renders to the same bytes, which snapshot hashing relies on.
func writeJSON(w *strings.Builder, n *node) {
	switch n.kind {
	case nodeNull:
		w.WriteString("null")
	case nodeBool, nodeNumber:
		w.WriteString(n.text)
	case nodeString:
		writeJSONString(w, n.text)
	case nodeList:
		w.WriteByte('[')
		for i, item := range n.items {
			if i > 0 {
				w.WriteByte(',')
			}
			writeJSON(w, item)
		}
		w.WriteByte(']')
	case nodeMap:
		w.WriteByte('{')
		for i, key := range n.keys {
			if i > 0 {
				w.WriteByte(',')
			}
			writeJSONString(w, key)
			w.WriteByte(':')
			writeJSON(w, n.items[i])
		}
		w.WriteByte('}')
	}
}

func writeJSONString(w *strings.Builder, s string) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	// Encoding a string cannot fail.
	_ = enc.Encode(s)
	w.Write(bytes.TrimRight(b.Bytes(), "\n"))
}

func toYAML(n *node) *yaml.Node {
	switch n.kind {
	case nodeNull:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}
	case nodeBool:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: n.text}
	case nodeNumber:
		tag := "!!float"
		if !strings.ContainsAny(n.text, ".eE") {
			tag = "!!int"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: n.text}
	case nodeString:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: n.text}
	case nodeList:
		y := &yaml.Node{Kind: yaml.SequenceNode}
		for _, item := range n.items {
			y.Content = append(y.Content, toYAML(item))
		}
		return y
	default:
		y := &yaml.Node{Kind: yaml.MappingNode}
		for i, key := range n.keys {
			y.Content = append(y.Content,
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
				toYAML(n.items[i]))
		}
		return y
	}
}
