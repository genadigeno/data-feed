// Package app wires the roles together: it is the only package that sees every other
// package.
package app

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/genadigeno/data-feed/internal/api"
	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/buildinfo"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/datadir"
	"github.com/genadigeno/data-feed/internal/protocol"
	"github.com/genadigeno/data-feed/internal/security"
	"github.com/genadigeno/data-feed/internal/snapshot"
	"github.com/genadigeno/data-feed/internal/store/sqlite"
)

const readinessTimeout = time.Second

// RunCoordinator starts the Coordinator and blocks until ctx is cancelled, then shuts
// down in order: stop accepting, drain requests, close the database, release the lock.
func RunCoordinator(ctx context.Context, cfg *config.Config, logger *slog.Logger, clk clock.Clock) error {
	applyRuntimeLimits(cfg, logger)

	dir, err := datadir.Open(cfg.Storage.DataDir)
	if err != nil {
		return err
	}
	lock, err := dir.Lock()
	if errors.Is(err, datadir.ErrLocked) {
		return apperr.Wrap(err, apperr.Conflict, "ERR_DATA_DIR_LOCKED",
			"another Coordinator already owns data directory "+dir.Root())
	}
	if err != nil {
		return err
	}
	defer func() {
		if err := lock.Release(); err != nil {
			logger.Error("release data directory lock", "err", err)
		}
	}()

	if err := os.MkdirAll(filepath.Dir(cfg.Persistence.SQLitePath), 0o750); err != nil {
		return fmt.Errorf("create database directory: %w", err)
	}
	if err := checkLocalStorage(cfg, dir); err != nil {
		return err
	}

	db, err := sqlite.Open(ctx, sqlite.Options{
		Path:          cfg.Persistence.SQLitePath,
		BusyTimeoutMS: cfg.Persistence.BusyTimeout.Milliseconds(),
		Durability:    cfg.Persistence.Durability,
		MaxReadConns:  cfg.Persistence.MaxReadConnections,
		Clock:         clk,
		Logger:        logger.With("component", "sqlite"),
	})
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Error("close database", "err", err)
		}
	}()
	logger.Info("database ready", "path", cfg.Persistence.SQLitePath, "schema_version", db.SchemaVersion())

	internalTLS, err := internalListenerTLS(cfg, dir, clk, logger)
	if err != nil {
		return err
	}
	publicTLS, err := publicListenerTLS(cfg)
	if err != nil {
		return err
	}

	var ready atomic.Bool
	publicHandler := api.NewHandler(api.Options{
		Logger:        logger.With("component", "api"),
		Clock:         clk,
		Version:       buildinfo.Version,
		MaxConcurrent: cfg.Server.MaxConcurrentRequests,
		Ready: func(ctx context.Context) []api.Check {
			return readinessChecks(ctx, ready.Load(), db, dir, cfg.Storage.MinFreeBytes)
		},
	})

	plane := NewControlPlane(cfg, db.Store(), dir, Vocabulary, clk, logger)
	if err := plane.Core.Recover(ctx); err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	internalHandler := protocol.NewHandler(logger.With("component", "protocol"), buildinfo.Version, plane.Core)

	// The internal listener opens first, as Workers must be able to connect by the
	// time the Coordinator reports ready.
	internal, err := startServer(cfg.Coordinator.Listen, internalTLS, &http.Server{
		Handler:           internalHandler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
		MaxHeaderBytes:    int(cfg.Server.MaxHeaderBytes),
		ErrorLog:          slog.NewLogLogger(logger.With("component", "protocol").Handler(), slog.LevelWarn),
	})
	if err != nil {
		return fmt.Errorf("worker listener: %w", err)
	}
	public, err := startServer(cfg.Server.Listen, publicTLS, &http.Server{
		Handler:           publicHandler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
		MaxHeaderBytes:    int(cfg.Server.MaxHeaderBytes),
		ErrorLog:          slog.NewLogLogger(logger.With("component", "api").Handler(), slog.LevelWarn),
	})
	if err != nil {
		_ = internal.srv.Close()
		return fmt.Errorf("public listener: %w", err)
	}

	logger.Info("coordinator ready",
		"version", buildinfo.Version,
		"environment", cfg.Application.Environment,
		"public_addr", public.addr,
		"worker_addr", internal.addr,
		"data_dir", dir.Root())
	ready.Store(true)

	// Background work stops before the database closes.
	workCtx, stopWork := context.WithCancel(context.Background())
	var work errgroup.Group
	work.Go(func() error {
		plane.Core.Run(workCtx)
		return nil
	})
	localCtx, stopLocal := context.WithCancel(workCtx)
	defer stopLocal()
	if local := plane.LocalWorker(cfg, dir, Vocabulary, clk, logger); local != nil {
		work.Go(func() error { return local.Run(localCtx) })
	}

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-public.failed:
	case serveErr = <-internal.failed:
	}

	ready.Store(false)
	logger.Info("coordinator shutting down")

	// The embedded worker finishes against a working control plane, then everything stops.
	plane.Core.BeginDrain()
	stopLocal()
	defer func() {
		stopWork()
		if err := work.Wait(); err != nil {
			logger.Error("background work", "err", err)
		}
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownGrace)
	defer cancel()
	var g errgroup.Group
	g.Go(func() error { return public.srv.Shutdown(shutdownCtx) })
	g.Go(func() error { return internal.srv.Shutdown(shutdownCtx) })
	if err := g.Wait(); err != nil {
		logger.Warn("shutdown grace expired, closing remaining connections", "err", err)
		_ = public.srv.Close()
		_ = internal.srv.Close()
	}
	logger.Info("coordinator stopped")
	return serveErr
}

func checkLocalStorage(cfg *config.Config, dir *datadir.Dir) error {
	if cfg.Persistence.AllowNetworkFS {
		return nil
	}
	for _, path := range []string{dir.Root(), filepath.Dir(cfg.Persistence.SQLitePath)} {
		network, err := datadir.IsNetworkFS(path)
		if err != nil {
			return err
		}
		if network {
			return apperr.New(apperr.InvalidArgument, "ERR_NETWORK_FS",
				path+" is on a network filesystem, which can corrupt SQLite; move it to local storage")
		}
	}
	return nil
}

// readinessChecks reports each condition separately so a failure is self-explanatory.
func readinessChecks(ctx context.Context, started bool, db *sqlite.DB, dir *datadir.Dir, minFree config.Size) []api.Check {
	checks := []api.Check{{Name: "startup", OK: started}}
	if !started {
		checks[0].Message = "starting or shutting down"
	}

	dbCheck := api.Check{Name: "database", OK: true}
	pingCtx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	if err := db.Ping(pingCtx); err != nil {
		dbCheck.OK, dbCheck.Message = false, "database not responding"
	}
	checks = append(checks, dbCheck)

	diskCheck := api.Check{Name: "disk", OK: true}
	free, err := datadir.FreeBytes(dir.Root())
	switch {
	case err != nil:
		diskCheck.OK, diskCheck.Message = false, "cannot read free disk space"
	case free < uint64(minFree):
		diskCheck.OK, diskCheck.Message = false,
			fmt.Sprintf("free space %s is below storage.min_free_bytes %s", config.Size(free), minFree)
	}
	return append(checks, diskCheck)
}

// internalListenerTLS picks the worker listener's TLS: provided certificate, else a
// generated one off loopback, else plain HTTP on loopback only.
func internalListenerTLS(cfg *config.Config, dir *datadir.Dir, clk clock.Clock, logger *slog.Logger) (*tls.Config, error) {
	t := cfg.Coordinator.TLS
	var out *tls.Config
	var err error
	switch {
	case t.CertFile != "":
		out, err = security.ServerTLS(t.CertFile, t.KeyFile)
	case !config.IsLoopback(cfg.Coordinator.Listen):
		var fingerprint string
		out, fingerprint, err = security.SelfSignedServerTLS(dir.TLSDir(), clk.Now())
		if err == nil {
			logger.Info("worker listener uses a self-signed certificate; pin it on Workers with worker.coordinator_fingerprint",
				"fingerprint", fingerprint)
		}
	default:
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("worker listener TLS: %w", err)
	}
	if t.ClientCAFile != "" {
		if err := security.RequireClientCerts(out, t.ClientCAFile); err != nil {
			return nil, fmt.Errorf("worker listener TLS: %w", err)
		}
	}
	return out, nil
}

func publicListenerTLS(cfg *config.Config) (*tls.Config, error) {
	if cfg.Server.TLS.CertFile == "" {
		return nil, nil
	}
	out, err := security.ServerTLS(cfg.Server.TLS.CertFile, cfg.Server.TLS.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("public listener TLS: %w", err)
	}
	return out, nil
}

type runningServer struct {
	srv    *http.Server
	addr   string
	failed chan error // receives if serving stops for any reason but Shutdown
}

// startServer binds now, so a port conflict fails startup, and serves in the
// background until Shutdown or Close.
func startServer(addr string, tlsConfig *tls.Config, srv *http.Server) (*runningServer, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv.TLSConfig = tlsConfig
	r := &runningServer{srv: srv, addr: l.Addr().String(), failed: make(chan error, 1)}
	go func() {
		var err error
		if tlsConfig != nil {
			err = srv.ServeTLS(l, "", "")
		} else {
			err = srv.Serve(l)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			r.failed <- err
		}
	}()
	return r, nil
}

// Vocabulary decodes the catalog a snapshot carries. Catalogs arrive with Phase 7.
var Vocabulary snapshot.VocabularyDecoder
