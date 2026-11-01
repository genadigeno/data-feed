package obs

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func testLogs(t *testing.T, maxBytes int64, files int) *SessionLogs {
	t.Helper()
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	return NewSessionLogs(t.TempDir(), maxBytes, files, func() time.Time {
		at = at.Add(time.Second)
		return at
	}, func(err error) { t.Errorf("log error: %v", err) })
}

func TestSessionLogReadsBackInOrder(t *testing.T) {
	l := testLogs(t, 1<<20, 3)
	l.Append("s1", slog.LevelInfo, "first", "n", 1)
	l.Append("s1", slog.LevelWarn, "second", "err", errors.New("boom"))
	l.Append("s2", slog.LevelInfo, "other")

	got, err := l.Read("s1", LogQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Message != "first" || got[1].Message != "second" {
		t.Fatalf("%+v", got)
	}
	if got[1].Attrs["err"] != "boom" {
		t.Fatalf("%+v", got[1].Attrs)
	}
}

func TestSessionLogRotatesAndBounds(t *testing.T) {
	l := testLogs(t, 300, 2)
	for range 50 {
		l.Append("s1", slog.LevelInfo, strings.Repeat("x", 50))
	}
	got, err := l.Read("s1", LogQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 || len(got) >= 50 {
		t.Fatalf("kept %d of 50", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Time.Before(got[i-1].Time) {
			t.Fatal("out of order across rotation")
		}
	}
}

func TestSessionLogFilters(t *testing.T) {
	l := testLogs(t, 1<<20, 2)
	for range 10 {
		l.Append("s1", slog.LevelInfo, "info")
	}
	l.Append("s1", slog.LevelError, "bad")
	got, err := l.Read("s1", LogQuery{MinLevel: slog.LevelWarn})
	if err != nil || len(got) != 1 || got[0].Message != "bad" {
		t.Fatalf("%+v %v", got, err)
	}
	got, err = l.Read("s1", LogQuery{Limit: 3})
	if err != nil || len(got) != 3 || got[2].Message != "bad" {
		t.Fatalf("%+v %v", got, err)
	}
	all, _ := l.Read("s1", LogQuery{})
	got, err = l.Read("s1", LogQuery{Since: all[9].Time})
	if err != nil || len(got) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestSessionLogRejectsUnsafeID(t *testing.T) {
	var reported error
	l := NewSessionLogs(t.TempDir(), 1<<20, 2, time.Now, func(err error) { reported = err })
	l.Append("../evil", slog.LevelInfo, "x")
	if reported == nil {
		t.Fatal("wrote outside its directory")
	}
	if _, err := l.Read("../evil", LogQuery{}); err == nil {
		t.Fatal("read outside its directory")
	}
}

func TestSessionLogMissingIsEmpty(t *testing.T) {
	got, err := testLogs(t, 100, 2).Read("nothing", LogQuery{})
	if err != nil || len(got) != 0 {
		t.Fatalf("%+v %v", got, err)
	}
}
