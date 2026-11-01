package sqlite

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestParseMigrationName(t *testing.T) {
	if v, err := parseMigrationName("0012_add_things.sql"); err != nil || v != 12 {
		t.Fatalf("got %d, %v", v, err)
	}
	for _, bad := range []string{"initial.sql", "0001.sql", "x_y.sql", "0000_zero.sql", "0001_a.txt"} {
		if _, err := parseMigrationName(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestPlanMigrationsAppliesOnlyWhatIsPending(t *testing.T) {
	all := []migration{{version: 1}, {version: 2}, {version: 3}}
	cases := map[int][]int{0: {1, 2, 3}, 1: {2, 3}, 3: {}}
	for applied, want := range cases {
		got, err := planMigrations(applied, all)
		if err != nil {
			t.Fatalf("applied %d: %v", applied, err)
		}
		if len(got) != len(want) {
			t.Fatalf("applied %d: got %d migrations, want %d", applied, len(got), len(want))
		}
		for i := range want {
			if got[i].version != want[i] {
				t.Errorf("applied %d: position %d is version %d", applied, i, got[i].version)
			}
		}
	}
}

func TestPlanMigrationsRefusesNewerDatabase(t *testing.T) {
	_, err := planMigrations(4, []migration{{version: 1}, {version: 2}})
	if err == nil || !strings.Contains(err.Error(), "schema version 4") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadMigrationsSortsAndChecksNumbering(t *testing.T) {
	good := fstest.MapFS{
		"m/0002_b.sql": {Data: []byte("B")},
		"m/0001_a.sql": {Data: []byte("A")},
	}
	ms, err := loadMigrations(good, "m")
	if err != nil {
		t.Fatal(err)
	}
	if ms[0].version != 1 || ms[0].sql != "A" || ms[1].version != 2 {
		t.Fatalf("wrong order: %+v", ms)
	}

	for name, bad := range map[string]fstest.MapFS{
		"gap":       {"m/0001_a.sql": {}, "m/0003_c.sql": {}},
		"duplicate": {"m/0001_a.sql": {}, "m/0001_b.sql": {}},
		"bad name":  {"m/notes.sql": {}},
	} {
		if _, err := loadMigrations(bad, "m"); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestEmbeddedMigrationsAreWellFormed(t *testing.T) {
	ms, err := loadMigrations(migrationFiles, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) == 0 {
		t.Fatal("no migrations embedded")
	}
	for _, m := range ms {
		if strings.TrimSpace(m.sql) == "" {
			t.Errorf("%s is empty", m.name)
		}
	}
}

func TestDSN(t *testing.T) {
	writer := dsn("/data/my db.sqlite", Options{BusyTimeoutMS: 5000, Durability: "strict"}, false)
	for _, want := range []string{"file:///data/my%20db.sqlite?", "_txlock=immediate", "journal_mode%28WAL%29", "busy_timeout%285000%29", "foreign_keys%281%29", "synchronous%28FULL%29"} {
		if !strings.Contains(writer, want) {
			t.Errorf("writer DSN %q lacks %q", writer, want)
		}
	}
	if strings.Contains(writer, "query_only") {
		t.Errorf("writer DSN is read-only: %q", writer)
	}

	reader := dsn(`C:\data\x.db`, Options{BusyTimeoutMS: 1000, Durability: "normal"}, true)
	for _, want := range []string{"file:///C:/data/x.db?", "query_only%281%29", "synchronous%28NORMAL%29"} {
		if !strings.Contains(reader, want) {
			t.Errorf("reader DSN %q lacks %q", reader, want)
		}
	}
}
