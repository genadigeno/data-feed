package obs

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var safeLogID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

const sessionLogName = "session.log"

// SessionLogs keeps one JSONL log per session under <root>/<session>/, rotated by size.
// Attribute values pass through the same redaction as process logs.
type SessionLogs struct {
	root     string
	maxBytes int64
	files    int
	now      func() time.Time
	onError  func(error)

	mu sync.Mutex
}

// LogEntry is one session log line.
type LogEntry struct {
	Time    time.Time      `json:"time"`
	Level   string         `json:"level"`
	Message string         `json:"msg"`
	Attrs   map[string]any `json:"attrs,omitempty"`
}

// NewSessionLogs returns the log store. Logging failures go to onError: they must never
// fail the work being logged.
func NewSessionLogs(root string, maxBytes int64, files int, now func() time.Time, onError func(error)) *SessionLogs {
	return &SessionLogs{root: root, maxBytes: maxBytes, files: files, now: now, onError: onError}
}

func (l *SessionLogs) path(session string, generation int) (string, error) {
	if !safeLogID.MatchString(session) {
		return "", fmt.Errorf("unsafe session id %q", session)
	}
	name := sessionLogName
	if generation > 0 {
		name += "." + strconv.Itoa(generation)
	}
	return filepath.Join(l.root, session, name), nil
}

// Append adds a line. attrs are alternating keys and values, as in slog.
func (l *SessionLogs) Append(session string, level slog.Level, msg string, attrs ...any) {
	if err := l.append(session, level, msg, attrs); err != nil && l.onError != nil {
		l.onError(err)
	}
}

func (l *SessionLogs) append(session string, level slog.Level, msg string, attrs []any) error {
	entry := LogEntry{Time: l.now().UTC(), Level: level.String(), Message: msg}
	if len(attrs) > 0 {
		entry.Attrs = make(map[string]any, len(attrs)/2)
		for i := 0; i+1 < len(attrs); i += 2 {
			key, ok := attrs[i].(string)
			if !ok {
				continue
			}
			a := redact(slog.Any(key, attrs[i+1]))
			if err, isErr := attrs[i+1].(error); isErr {
				entry.Attrs[key] = err.Error()
				continue
			}
			entry.Attrs[key] = a.Value.Any()
		}
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	path, err := l.path(session, 0)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	if info, err := os.Stat(path); err == nil && info.Size()+int64(len(line)) > l.maxBytes {
		if err := l.rotate(session); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	_, err = f.Write(line)
	return errors.Join(err, f.Close())
}

// rotate shifts session.log to session.log.1 and so on, dropping the oldest.
func (l *SessionLogs) rotate(session string) error {
	for g := l.files - 1; g >= 0; g-- {
		from, err := l.path(session, g)
		if err != nil {
			return err
		}
		if g == l.files-1 {
			if err := os.Remove(from); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			continue
		}
		to, err := l.path(session, g+1)
		if err != nil {
			return err
		}
		if err := os.Rename(from, to); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// LogQuery selects lines.
type LogQuery struct {
	Since    time.Time
	MinLevel slog.Level
	Limit    int // the newest lines, up to this many
}

// Read returns matching lines, oldest first.
func (l *SessionLogs) Read(session string, q LogQuery) ([]LogEntry, error) {
	var keep []LogEntry
	for g := l.files - 1; g >= 0; g-- {
		path, err := l.path(session, g)
		if err != nil {
			return nil, err
		}
		f, err := os.Open(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		keep, err = readLogFile(f, keep, q)
		_ = f.Close()
		if err != nil {
			return nil, err
		}
	}
	return keep, nil
}

func readLogFile(r io.Reader, keep []LogEntry, q LogQuery) ([]LogEntry, error) {
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 1<<16), 1<<20)
	for scan.Scan() {
		var e LogEntry
		if json.Unmarshal(scan.Bytes(), &e) != nil {
			continue // a torn last line after a crash
		}
		var level slog.Level
		if level.UnmarshalText([]byte(e.Level)) == nil && level < q.MinLevel {
			continue
		}
		if !q.Since.IsZero() && e.Time.Before(q.Since) {
			continue
		}
		keep = append(keep, e)
		if q.Limit > 0 && len(keep) > 2*q.Limit {
			keep = append(keep[:0], keep[len(keep)-q.Limit:]...)
		}
	}
	if q.Limit > 0 && len(keep) > q.Limit {
		keep = append(keep[:0], keep[len(keep)-q.Limit:]...)
	}
	return keep, scan.Err()
}
