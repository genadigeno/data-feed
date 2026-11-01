package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// migration is one forward-only schema step.
type migration struct {
	version int
	name    string
	sql     string
}

// parseMigrationName reads the version from a name like 0001_initial.sql.
func parseMigrationName(name string) (int, error) {
	prefix, _, ok := strings.Cut(strings.TrimSuffix(name, ".sql"), "_")
	if !ok || !strings.HasSuffix(name, ".sql") {
		return 0, fmt.Errorf("migration %q: want NNNN_name.sql", name)
	}
	v, err := strconv.Atoi(prefix)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("migration %q: bad version", name)
	}
	return v, nil
}

// sortMigrations orders migrations and requires versions to run 1, 2, 3, … so a
// missing or duplicated file is caught before anything is applied.
func sortMigrations(ms []migration) error {
	slices.SortFunc(ms, func(a, b migration) int { return a.version - b.version })
	for i, m := range ms {
		if m.version != i+1 {
			return fmt.Errorf("migrations must be numbered consecutively from 1: %s has version %d at position %d", m.name, m.version, i+1)
		}
	}
	return nil
}

// planMigrations decides what to apply given the highest applied version. A database
// from a newer binary is refused rather than risking a silent downgrade.
func planMigrations(applied int, available []migration) ([]migration, error) {
	latest := 0
	if n := len(available); n > 0 {
		latest = available[n-1].version
	}
	if applied > latest {
		return nil, fmt.Errorf("database is at schema version %d but this binary only knows %d: upgrade the binary or restore a backup", applied, latest)
	}
	return available[applied:], nil
}

func loadMigrations(fsys fs.FS, dir string) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}
	ms := make([]migration, 0, len(entries))
	for _, e := range entries {
		v, err := parseMigrationName(e.Name())
		if err != nil {
			return nil, err
		}
		body, err := fs.ReadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		ms = append(ms, migration{version: v, name: e.Name(), sql: string(body)})
	}
	if err := sortMigrations(ms); err != nil {
		return nil, err
	}
	return ms, nil
}

// migrate applies pending migrations, each in its own write transaction together
// with its schema_migrations row.
func (db *DB) migrate(ctx context.Context) error {
	available, err := loadMigrations(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}

	const createTable = `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)`
	if _, err := db.writer.ExecContext(ctx, createTable); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	var applied int
	row := db.writer.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`)
	if err := row.Scan(&applied); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	pending, err := planMigrations(applied, available)
	if err != nil {
		return err
	}
	for _, m := range pending {
		err := db.writeTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, m.sql); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`,
				m.version, db.clock.Now().UTC().Format(time.RFC3339Nano))
			return err
		})
		if err != nil {
			return fmt.Errorf("apply migration %s: %w", m.name, err)
		}
		db.logger.Info("applied migration", "version", m.version, "name", m.name)
	}
	db.schemaVersion = applied + len(pending)
	return nil
}
