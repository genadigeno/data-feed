// Package sqlite is the Coordinator's embedded persistence: one writer connection
// that serialises all writes, plus a small read-only pool. See
// docs/01-architecture/sqlite-persistence.md.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	sqlite3 "modernc.org/sqlite"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/clock"
)

const (
	sqliteBusy   = 5
	sqliteLocked = 6

	maxBusyRetries = 5
	busyBackoff    = 50 * time.Millisecond
)

// Options configures Open.
type Options struct {
	Path          string
	BusyTimeoutMS int64
	Durability    string // normal | strict
	MaxReadConns  int
	Clock         clock.Clock
	Logger        *slog.Logger
}

// DB is an open database. Only the Coordinator ever creates one.
type DB struct {
	dsnWrite      string // for a backup, which needs its own connection
	writer        *sql.DB
	reader        *sql.DB
	clock         clock.Clock
	logger        *slog.Logger
	schemaVersion int
}

// dsn builds the modernc connection string. Windows paths become file:///C:/x.
func dsn(path string, o Options, readOnly bool) string {
	path = strings.ReplaceAll(path, `\`, "/")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	synchronous := "NORMAL"
	if o.Durability == "strict" {
		synchronous = "FULL"
	}
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", o.BusyTimeoutMS))
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "synchronous("+synchronous+")")
	if readOnly {
		q.Add("_pragma", "query_only(1)")
	} else {
		q.Set("_txlock", "immediate")
	}
	u := url.URL{Scheme: "file", Path: path, RawQuery: q.Encode()}
	return u.String()
}

// Open opens the database and applies pending migrations.
func Open(ctx context.Context, o Options) (*DB, error) {
	abs, err := filepath.Abs(o.Path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}

	writer, err := sql.Open("sqlite", dsn(abs, o, false))
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	writer.SetMaxOpenConns(1)
	writer.SetMaxIdleConns(1)
	writer.SetConnMaxLifetime(0)
	writer.SetConnMaxIdleTime(0)

	db := &DB{writer: writer, clock: o.Clock, logger: o.Logger, dsnWrite: dsn(abs, o, false)}
	if err := db.migrate(ctx); err != nil {
		_ = writer.Close()
		return nil, err
	}

	// The reader is opened after migrating so its first connection sees WAL mode.
	reader, err := sql.Open("sqlite", dsn(abs, o, true))
	if err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("open database reader: %w", err)
	}
	reader.SetMaxOpenConns(o.MaxReadConns)
	reader.SetMaxIdleConns(o.MaxReadConns)
	db.reader = reader

	if err := db.Ping(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// Backup writes a consistent copy of the database to a new file. It reads a snapshot on a
// connection of its own, so generation keeps committing meanwhile.
func (db *DB) Backup(ctx context.Context, dest string) error {
	conn, err := sql.Open("sqlite", db.dsnWrite)
	if err != nil {
		return fmt.Errorf("open backup connection: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `VACUUM INTO ?`, dest); err != nil {
		return fmt.Errorf("back up database: %w", err)
	}
	return nil
}

// SchemaVersion returns the migration version the database is at.
func (db *DB) SchemaVersion() int { return db.schemaVersion }

// Ping checks both pools.
func (db *DB) Ping(ctx context.Context) error {
	for name, pool := range map[string]*sql.DB{"writer": db.writer, "reader": db.reader} {
		var one int
		if err := pool.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil {
			return fmt.Errorf("database %s: %w", name, err)
		}
	}
	return nil
}

// Querier is what transaction callbacks use: repositories are written against it.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type writeTxKey struct{}

// ErrNestedWrite means a write callback started another write. Waiting for the single
// writer connection from inside its own transaction would deadlock forever.
var ErrNestedWrite = apperr.New(apperr.Internal, apperr.CodeInternal, "nested database write transaction")

// Write runs fn in a write transaction, retrying if SQLite reports contention, so fn
// must have no side effects outside the transaction. Callbacks must not do network
// I/O, generation or AI calls, and must pass on the context they receive.
func (db *DB) Write(ctx context.Context, fn func(ctx context.Context, q Querier) error) error {
	if ctx.Value(writeTxKey{}) != nil {
		return ErrNestedWrite
	}
	inner := context.WithValue(ctx, writeTxKey{}, true)
	return db.writeTx(ctx, func(tx *sql.Tx) error { return fn(inner, tx) })
}

func (db *DB) writeTx(ctx context.Context, fn func(*sql.Tx) error) error {
	var err error
	for attempt := range maxBusyRetries {
		err = db.runTx(ctx, db.writer, fn)
		if err == nil || !isBusy(err) {
			return err
		}
		db.logger.Warn("database busy, retrying", "attempt", attempt+1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-db.clock.After(busyBackoff << attempt):
		}
	}
	return fmt.Errorf("database stayed busy: %w", err)
}

// Read runs fn in a read-only transaction on a consistent snapshot.
func (db *DB) Read(ctx context.Context, fn func(ctx context.Context, q Querier) error) error {
	return db.runTx(ctx, db.reader, func(tx *sql.Tx) error { return fn(ctx, tx) })
}

func (db *DB) runTx(ctx context.Context, pool *sql.DB, fn func(*sql.Tx) error) (err error) {
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func isBusy(err error) bool {
	var se *sqlite3.Error
	if !errors.As(err, &se) {
		return false
	}
	code := se.Code() & 0xff // extended codes keep the primary code in the low byte
	return code == sqliteBusy || code == sqliteLocked
}

// Close checkpoints the WAL and closes both pools.
func (db *DB) Close() error {
	var errs []error
	if db.reader != nil {
		errs = append(errs, db.reader.Close())
	}
	if _, err := db.writer.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		errs = append(errs, fmt.Errorf("checkpoint: %w", err))
	}
	errs = append(errs, db.writer.Close())
	return errors.Join(errs...)
}
