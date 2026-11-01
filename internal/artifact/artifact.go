// Package artifact is the Coordinator's authoritative output storage: staged uploads,
// immutable part files, manifests, merged downloads and startup repair. Paths are only
// ever built from identifiers the Coordinator itself made.
// See docs/01-architecture/storage-design.md.
package artifact

import (
	"cmp"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

const (
	dirOutputs   = "outputs"
	dirStaging   = "staging"
	dirRetained  = "retained"
	manifestFile = "manifest.json"
)

var (
	safeID     = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	safeStream = regexp.MustCompile(`^(main|table-[a-z0-9_]{1,64})$`)
	safeExt    = regexp.MustCompile(`^[a-z0-9]{1,8}(\.gz)?$`)
)

// Part is a committed part as the database records it.
type Part struct {
	SessionID    string `json:"-"`
	Stream       string `json:"stream"`
	UnitID       string `json:"unit_id"`
	UnitSeq      int    `json:"unit_seq"`
	Seq          int    `json:"seq"`
	RelPath      string `json:"path"`
	FirstIndex   uint64 `json:"first_index"`
	EndIndex     uint64 `json:"end_index"`
	Records      uint64 `json:"records"`
	Bytes        int64  `json:"bytes"`
	SHA256       string `json:"sha256"`
	HeaderLen    int    `json:"header_len"`
	FooterLen    int    `json:"footer_len"`
	FencingToken uint64 `json:"-"`
}

// Manifest describes a session's output. The database is authoritative: this is an
// export for whoever reads the files.
type Manifest struct {
	Format      string `json:"format"`
	Compression string `json:"compression"`
	Records     uint64 `json:"records"`
	Bytes       int64  `json:"bytes"`
	Parts       []Part `json:"parts"`
}

// SortManifest orders parts by stream, then unit, then sequence.
func SortManifest(m *Manifest) {
	slices.SortStableFunc(m.Parts, func(a, b Part) int {
		return cmp.Or(cmp.Compare(a.Stream, b.Stream), cmp.Compare(a.UnitSeq, b.UnitSeq), cmp.Compare(a.Seq, b.Seq))
	})
}

// Store is the output directory tree.
type Store struct{ root string }

// New returns a store rooted at the data directory.
func New(root string) *Store { return &Store{root: root} }

func (s *Store) outputs() string { return filepath.Join(s.root, dirOutputs) }
func (s *Store) staging() string { return filepath.Join(s.root, dirStaging) }

// resolve turns a stored relative path into a real one, refusing anything outside the
// outputs directory, as defence in depth for paths read back from the database.
func (s *Store) resolve(rel string) (string, error) {
	local := filepath.FromSlash(rel)
	if !filepath.IsLocal(local) {
		return "", fmt.Errorf("unsafe artifact path %q", rel)
	}
	return filepath.Join(s.outputs(), local), nil
}

// StagingFile creates a file for an upload in progress, on the same filesystem as the
// final location, so promotion is an atomic rename.
func (s *Store) StagingFile() (*os.File, error) {
	if err := os.MkdirAll(s.staging(), 0o750); err != nil {
		return nil, fmt.Errorf("create staging directory: %w", err)
	}
	f, err := os.CreateTemp(s.staging(), "*.upload")
	if err != nil {
		return nil, fmt.Errorf("create staging file: %w", err)
	}
	return f, nil
}

// PartPath builds a part's relative path. The fencing token in the name keeps attempts
// apart, so a stale attempt can never overwrite a committed part.
func PartPath(session, stream string, unitSeq, partSeq int, token uint64, ext string) (string, error) {
	switch {
	case !safeID.MatchString(session):
		return "", fmt.Errorf("unsafe session id %q", session)
	case !safeStream.MatchString(stream):
		return "", fmt.Errorf("unsafe stream name %q", stream)
	case !safeExt.MatchString(ext):
		return "", fmt.Errorf("unsafe extension %q", ext)
	case unitSeq < 0 || partSeq < 0:
		return "", errors.New("negative sequence number")
	}
	return fmt.Sprintf("%s/%s/u%06d-p%04d.f%d.%s", session, stream, unitSeq, partSeq, token, ext), nil
}

// Promote moves a staged file to its final path, durably. It happens before the database
// commit: a crash in between leaves a file nothing refers to, which reconciliation removes.
func (s *Store) Promote(staged, rel string) error {
	final, err := s.resolve(rel)
	if err != nil {
		return err
	}
	dir := filepath.Dir(final)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create part directory: %w", err)
	}
	if err := os.Rename(staged, final); err != nil {
		return fmt.Errorf("promote part: %w", err)
	}
	return syncDir(dir)
}

// syncDir makes a rename durable. Windows cannot sync a directory and does not need to.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return nil //nolint:nilerr // best effort
	}
	defer d.Close()
	_ = d.Sync()
	return nil
}

// Open opens a committed part for reading.
func (s *Store) Open(rel string) (*os.File, error) {
	path, err := s.resolve(rel)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

// Remove deletes a part file. A missing file is not an error: it is the desired state.
func (s *Store) Remove(rel string) error {
	path, err := s.resolve(rel)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// WriteManifest atomically replaces a session's manifest.
func (s *Store) WriteManifest(session string, m Manifest) error {
	if !safeID.MatchString(session) {
		return fmt.Errorf("unsafe session id %q", session)
	}
	return WriteManifestFile(filepath.Join(s.outputs(), session, manifestFile), m)
}

// WriteManifestFile writes a manifest to a path, through a temporary file.
func WriteManifestFile(path string, m Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".manifest-*")
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	err = errors.Join(err, f.Close())
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		_ = os.Remove(f.Name())
	}
	return err
}

// FileInfo is a file found under outputs, by relative slash path.
type FileInfo struct {
	RelPath string
	Size    int64
}

// Scan lists part files under outputs, leaving out manifests and temporary files.
func (s *Store) Scan() ([]FileInfo, error) {
	root := s.outputs()
	var out []FileInfo
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() || d.Name() == manifestFile || strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out = append(out, FileInfo{RelPath: filepath.ToSlash(rel), Size: info.Size()})
		return nil
	})
	return out, err
}

// RetainSession moves a deleted session's output out of managed storage, so it stays on
// disk but reconciliation, which removes files no row refers to, leaves it alone.
func (s *Store) RetainSession(session string) error {
	if !safeID.MatchString(session) {
		return fmt.Errorf("unsafe session id %q", session)
	}
	from := filepath.Join(s.outputs(), session)
	if _, err := os.Stat(from); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	dir := filepath.Join(s.root, dirRetained)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	return os.Rename(from, filepath.Join(dir, session))
}

// PurgeSession deletes a session's output. Files still open, such as by a running download
// on Windows, make this fail; it is retried at the next opportunity.
func (s *Store) PurgeSession(session string) error {
	if !safeID.MatchString(session) {
		return fmt.Errorf("unsafe session id %q", session)
	}
	return os.RemoveAll(filepath.Join(s.outputs(), session))
}

// ActionKind says what a reconciliation step does.
type ActionKind uint8

const (
	// ActionDeleteFile removes a file no part refers to.
	ActionDeleteFile ActionKind = iota
	// ActionDropPart removes a part's row, as its file is missing or damaged.
	ActionDropPart
	// ActionRollbackUnit moves a unit's committed position back so it regenerates.
	ActionRollbackUnit
)

// Action is one step of reconciliation.
type Action struct {
	Kind    ActionKind
	RelPath string // DeleteFile
	UnitID  string // DropPart, RollbackUnit
	Seq     int    // DropPart
	To      uint64 // RollbackUnit: the new committed position
	NextSeq int    // RollbackUnit: the next part sequence number
}

// PlanReconcile compares files on disk with committed parts. It is pure, so every crash
// scenario can be tested as data. A file without a row comes from a crash between
// promotion and commit, or an interrupted purge. A row without an intact file breaks its
// unit from that part on, and the unit must regenerate from there: later parts are dropped
// too, because a unit's parts must join up. The file of a dropped later part is deleted.
func PlanReconcile(files []FileInfo, parts []Part) []Action {
	onDisk := make(map[string]int64, len(files))
	for _, f := range files {
		onDisk[f.RelPath] = f.Size
	}

	referenced := make(map[string]bool, len(parts))
	byUnit := map[string][]Part{}
	var units []string
	for _, p := range parts {
		referenced[p.RelPath] = true
		if _, ok := byUnit[p.UnitID]; !ok {
			units = append(units, p.UnitID)
		}
		byUnit[p.UnitID] = append(byUnit[p.UnitID], p)
	}

	var actions []Action
	for _, f := range files {
		if !referenced[f.RelPath] {
			actions = append(actions, Action{Kind: ActionDeleteFile, RelPath: f.RelPath})
		}
	}

	for _, unit := range units {
		list := byUnit[unit]
		sortParts(list)
		broken := -1
		for i, p := range list {
			size, ok := onDisk[p.RelPath]
			if !ok || size != p.Bytes {
				broken = i
				break
			}
		}
		if broken < 0 {
			continue
		}
		for _, p := range list[broken:] {
			actions = append(actions, Action{Kind: ActionDropPart, UnitID: unit, Seq: p.Seq})
			if _, ok := onDisk[p.RelPath]; ok {
				actions = append(actions, Action{Kind: ActionDeleteFile, RelPath: p.RelPath})
			}
		}
		first := list[broken]
		actions = append(actions, Action{Kind: ActionRollbackUnit, UnitID: unit, To: first.FirstIndex, NextSeq: first.Seq})
	}
	return actions
}

func sortParts(parts []Part) {
	slices.SortFunc(parts, func(a, b Part) int { return cmp.Compare(a.Seq, b.Seq) })
}

// ApplyFiles performs the file steps of a plan and clears the staging directory. The
// database steps are left to the caller, in one transaction.
func (s *Store) ApplyFiles(actions []Action) error {
	var errs []error
	for _, a := range actions {
		if a.Kind == ActionDeleteFile {
			if err := s.Remove(a.RelPath); err != nil {
				errs = append(errs, err)
			}
		}
	}
	errs = append(errs, s.clearStaging())
	return errors.Join(errs...)
}

func (s *Store) clearStaging() error {
	entries, err := os.ReadDir(s.staging())
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(s.staging(), e.Name())); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Layout is how a format's parts merge.
type Layout struct {
	Separator []byte
	Empty     []byte // the whole document when there are no parts
}

// segment is a piece of a merged document: a byte range of a part, or literal bytes.
type segment struct {
	rel      string
	off, len int64
	literal  []byte
}

// MergedReader presents parts as one document, by reading byte ranges from their files:
// constant memory, no temporary files, and it supports seeking for ranged downloads.
type MergedReader struct {
	s        *Store
	segments []segment
	size     int64
	pos      int64
	open     *os.File
	openRel  string
}

// Merged prepares a merged document: the first part's header, the bodies joined by the
// separator, and the last part's footer.
func (s *Store) Merged(parts []Part, l Layout) (*MergedReader, error) {
	m := &MergedReader{s: s}
	if len(parts) == 0 {
		m.segments = []segment{{literal: l.Empty, len: int64(len(l.Empty))}}
		m.size = int64(len(l.Empty))
		return m, nil
	}
	for i, p := range parts {
		if strings.HasSuffix(p.RelPath, ".gz") {
			return nil, errors.New("compressed parts cannot be merged: download them one by one")
		}
		body := p.Bytes - int64(p.HeaderLen) - int64(p.FooterLen)
		if body < 0 {
			return nil, fmt.Errorf("part %s has a body of %d bytes", p.RelPath, body)
		}
		if i == 0 && p.HeaderLen > 0 {
			m.add(segment{rel: p.RelPath, len: int64(p.HeaderLen)})
		}
		if i > 0 && len(l.Separator) > 0 {
			m.add(segment{literal: l.Separator, len: int64(len(l.Separator))})
		}
		m.add(segment{rel: p.RelPath, off: int64(p.HeaderLen), len: body})
		if i == len(parts)-1 && p.FooterLen > 0 {
			m.add(segment{rel: p.RelPath, off: p.Bytes - int64(p.FooterLen), len: int64(p.FooterLen)})
		}
	}
	return m, nil
}

func (m *MergedReader) add(s segment) {
	m.segments = append(m.segments, s)
	m.size += s.len
}

// Size returns the merged document's total length.
func (m *MergedReader) Size() int64 { return m.size }

// Read implements io.Reader.
func (m *MergedReader) Read(b []byte) (int, error) {
	if m.pos >= m.size {
		return 0, io.EOF
	}
	offset := m.pos
	for i := range m.segments {
		seg := &m.segments[i]
		if offset >= seg.len {
			offset -= seg.len
			continue
		}
		want := min(int64(len(b)), seg.len-offset)
		var n int
		var err error
		if seg.literal != nil {
			n = copy(b[:want], seg.literal[offset:])
		} else {
			if err = m.openPart(seg.rel); err != nil {
				return 0, err
			}
			n, err = m.open.ReadAt(b[:want], seg.off+offset)
			if errors.Is(err, io.EOF) && int64(n) == want {
				err = nil
			}
		}
		m.pos += int64(n)
		return n, err
	}
	return 0, io.EOF
}

func (m *MergedReader) openPart(rel string) error {
	if m.open != nil && m.openRel == rel {
		return nil
	}
	if m.open != nil {
		_ = m.open.Close()
		m.open = nil
	}
	f, err := m.s.Open(rel)
	if err != nil {
		return err
	}
	m.open, m.openRel = f, rel
	return nil
}

// Seek implements io.Seeker.
func (m *MergedReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = m.pos + offset
	case io.SeekEnd:
		abs = m.size + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("negative position")
	}
	m.pos = abs
	return abs, nil
}

// Close releases the open file.
func (m *MergedReader) Close() error {
	if m.open == nil {
		return nil
	}
	err := m.open.Close()
	m.open = nil
	return err
}

// EncodeSHA renders a part checksum for storage and manifests.
func EncodeSHA(sum [32]byte) string { return hex.EncodeToString(sum[:]) }
