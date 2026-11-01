// Package snapshot is the immutable, content-addressed bundle a session executes.
// Everything generation depends on is inside, so a running session never changes when a
// schema or catalog is edited, and Workers need nothing else from the Coordinator.
package snapshot

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/gen/value"
	"github.com/genadigeno/data-feed/internal/schema"
	"github.com/genadigeno/data-feed/internal/serial"
)

// MaxDecompressedBytes bounds what a snapshot may expand to, whatever its compressed size.
const MaxDecompressedBytes = 1 << 30

// Snapshot is the bundle.
type Snapshot struct {
	EngineVersion  int             `json:"engine_version"`
	Schema         json.RawMessage `json:"schema"` // canonical schema JSON
	Seed           uint64          `json:"seed"`
	ReferenceTime  time.Time       `json:"reference_time"`
	Mode           string          `json:"mode"`
	Format         string          `json:"format"`
	NestedRoot     string          `json:"nested_root,omitempty"`
	MaxRows        uint64          `json:"max_rows,omitempty"`
	Compression    string          `json:"compression,omitempty"`
	Encoder        serial.Options  `json:"encoder"`
	BatchSize      int             `json:"batch_size"`
	QueueCapacity  int             `json:"queue_capacity"`
	UnitRecords    int64           `json:"unit_records"`
	UnitMaxBytes   int64           `json:"unit_max_bytes"`
	PartMaxBytes   int64           `json:"part_max_bytes"`
	PartMaxRecords int64           `json:"part_max_records"`
	PartMaxAgeMS   int64           `json:"part_max_age_ms"`
	// Catalog is the pinned vocabulary content, if any.
	Catalog json.RawMessage `json:"catalog,omitempty"`
}

// Encode renders the canonical form and its gzip compression, and returns the content
// hash. Equal snapshots hash equally.
func (s *Snapshot) Encode() (hash string, body []byte, err error) {
	canonical, err := json.Marshal(s)
	if err != nil {
		return "", nil, fmt.Errorf("encode snapshot: %w", err)
	}
	sum := sha256.Sum256(canonical)

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(canonical); err != nil {
		return "", nil, fmt.Errorf("compress snapshot: %w", err)
	}
	if err := zw.Close(); err != nil {
		return "", nil, fmt.Errorf("compress snapshot: %w", err)
	}
	return "sha256:" + hex.EncodeToString(sum[:]), buf.Bytes(), nil
}

// Decode reads a stored snapshot. If hash is not empty, the content must match it.
func Decode(body []byte, hash string) (*Snapshot, error) {
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	canonical, err := io.ReadAll(io.LimitReader(zr, MaxDecompressedBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	if len(canonical) > MaxDecompressedBytes {
		return nil, fmt.Errorf("snapshot expands beyond %d bytes", MaxDecompressedBytes)
	}
	if hash != "" {
		sum := sha256.Sum256(canonical)
		if got := "sha256:" + hex.EncodeToString(sum[:]); got != hash {
			return nil, fmt.Errorf("snapshot content does not match its hash %s", hash)
		}
	}
	var s Snapshot
	if err := json.Unmarshal(canonical, &s); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	return &s, nil
}

// VocabularyDecoder turns pinned catalog content into a vocabulary for the engine.
type VocabularyDecoder func(content json.RawMessage) (value.Vocabulary, error)

// Compiled is a snapshot ready to generate from.
type Compiled struct {
	Snapshot *Snapshot
	Schema   *schema.Schema
	Engine   *gen.Engine
}

// Compile builds the engine. It refuses a snapshot from another engine version: the bytes
// it produced are the point, and a different version would produce others.
func (s *Snapshot) Compile(vocab VocabularyDecoder) (*Compiled, error) {
	if s.EngineVersion != gen.Version {
		return nil, fmt.Errorf("snapshot needs engine version %d, this binary has %d", s.EngineVersion, gen.Version)
	}
	parsed, problems := schema.Decode(s.Schema, schema.FormatJSON)
	if len(problems) > 0 {
		return nil, fmt.Errorf("snapshot schema: %w", problems)
	}
	in := gen.Input{
		Schema: parsed, Seed: s.Seed, ReferenceTime: s.ReferenceTime, MaxRows: s.MaxRows, NestedRoot: s.NestedRoot,
	}
	if len(s.Catalog) > 0 {
		if vocab == nil {
			return nil, fmt.Errorf("snapshot has a catalog, but no vocabulary decoder is available")
		}
		v, err := vocab(s.Catalog)
		if err != nil {
			return nil, fmt.Errorf("snapshot catalog: %w", err)
		}
		in.Vocabulary = v
	}
	engine, err := gen.Build(in)
	if err != nil {
		return nil, err
	}
	return &Compiled{Snapshot: s, Schema: parsed, Engine: engine}, nil
}

// NewEncoder returns an encoder for one stream.
func (c *Compiled) NewEncoder(spec gen.StreamSpec) (serial.Encoder, error) {
	return serial.New(c.Snapshot.Format, c.Snapshot.Encoder, serial.Stream{Fields: spec.Fields, Definitions: c.Schema.Definitions})
}

// Stream returns a stream's description.
func (c *Compiled) Stream(id string) (gen.StreamSpec, bool) {
	for _, s := range c.Engine.Streams() {
		if string(s.ID) == id {
			return s, true
		}
	}
	return gen.StreamSpec{}, false
}
