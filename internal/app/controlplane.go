package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/genadigeno/data-feed/internal/artifact"
	"github.com/genadigeno/data-feed/internal/buildinfo"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/coord"
	"github.com/genadigeno/data-feed/internal/datadir"
	"github.com/genadigeno/data-feed/internal/hub"
	"github.com/genadigeno/data-feed/internal/obs"
	"github.com/genadigeno/data-feed/internal/session"
	"github.com/genadigeno/data-feed/internal/snapshot"
	"github.com/genadigeno/data-feed/internal/store"
	"github.com/genadigeno/data-feed/internal/wire"
	"github.com/genadigeno/data-feed/internal/worker"
)

// ControlPlane is the Coordinator's working parts.
type ControlPlane struct {
	Core     *coord.Core
	Sessions *session.Service
	Hub      *hub.Hub
	Logs     *obs.SessionLogs
	Store    *artifact.Store
}

// NewControlPlane assembles the control plane over an open database and data directory.
func NewControlPlane(cfg *config.Config, db store.DB, dir *datadir.Dir, vocab snapshot.VocabularyDecoder, clk clock.Clock, logger *slog.Logger) *ControlPlane {
	h := hub.New()
	logs := obs.NewSessionLogs(filepath.Join(dir.Root(), "sessions"), int64(cfg.Logging.SessionLogMaxSize), cfg.Logging.SessionLogFiles, clk.Now,
		func(err error) { logger.Warn("write session log", "err", err) })
	artifacts := artifact.New(dir.Root())
	core := coord.New(coord.Options{
		DB: db, Artifacts: artifacts, Config: cfg, Clock: clk, Logger: logger, Hub: h, Logs: logs, Vocab: vocab,
		FreeBytes:        func() (uint64, error) { return datadir.FreeBytes(dir.Root()) },
		OpenRegistration: cfg.Application.Environment == config.EnvDevelopment && config.IsLoopback(cfg.Coordinator.Listen),
	})
	return &ControlPlane{
		Core: core, Hub: h, Logs: logs, Store: artifacts,
		Sessions: &session.Service{DB: db, Config: cfg, Clock: clk, Notifier: core, Artifacts: artifacts, Vocab: vocab},
	}
}

// localCoordinator is the Coordinator as the embedded worker sees it. It is this process,
// so it registers without a credential.
type localCoordinator struct{ *coord.Core }

func (l localCoordinator) Register(ctx context.Context, r wire.RegisterRequest) (wire.RegisterResponse, error) {
	return l.RegisterLocal(ctx, r.Capabilities)
}

// LocalWorker returns the embedded worker, or nil if none is configured. It runs the same
// code as a remote one, and differs only in how it reaches the Coordinator.
func (p *ControlPlane) LocalWorker(cfg *config.Config, dir *datadir.Dir, vocab snapshot.VocabularyDecoder, clk clock.Clock, logger *slog.Logger) *worker.Runtime {
	slots := cfg.Worker.LocalSlots
	if slots <= 0 {
		return nil
	}
	return worker.New(worker.Options{
		Name:              "embedded",
		Transport:         localCoordinator{p.Core},
		Capabilities:      worker.Capabilities(slots, int64(cfg.Worker.MaxMemoryBytes), int64(cfg.Worker.SpoolMaxBytes), buildinfo.Version, cfg.Worker.Labels),
		SpoolDir:          filepath.Join(dir.Root(), "staging", "local"),
		SpoolEnabled:      true,
		SpoolMaxBytes:     int64(cfg.Worker.SpoolMaxBytes),
		UploadConcurrency: cfg.Worker.UploadConcurrency,
		DrainTimeout:      cfg.Worker.DrainTimeout,
		LocalFiles:        true,
		Vocab:             vocab,
		Clock:             clk,
		Logger:            logger,
	})
}

// remoteWorkerOptions fills in what a remote Worker needs from configuration.
func remoteWorkerOptions(cfg *config.Config, transport wire.Coordinator, vocab snapshot.VocabularyDecoder, clk clock.Clock, logger *slog.Logger) (worker.Options, error) {
	name := cfg.Worker.Name
	host, _ := os.Hostname()
	if name == "" {
		name = host
	}
	spool := cfg.Worker.SpoolPath
	credentialFile := cfg.Worker.CredentialFile
	if credentialFile == "" {
		credentialFile = filepath.Join(filepath.Dir(spool), "worker.credential")
	}
	credential, err := readCredential(credentialFile)
	if err != nil {
		return worker.Options{}, err
	}
	return worker.Options{
		Name: name, Host: host, Transport: transport,
		Capabilities:      worker.Capabilities(cfg.Worker.Slots, int64(cfg.Worker.MaxMemoryBytes), int64(cfg.Worker.SpoolMaxBytes), buildinfo.Version, cfg.Worker.Labels),
		JoinToken:         cfg.Worker.JoinToken.Reveal(),
		Credential:        credential,
		SaveCredential:    func(c string) error { return writeCredential(credentialFile, c) },
		SpoolDir:          spool,
		SpoolEnabled:      cfg.Worker.SpoolEnabled,
		SpoolMaxBytes:     int64(cfg.Worker.SpoolMaxBytes),
		UploadConcurrency: cfg.Worker.UploadConcurrency,
		DrainTimeout:      cfg.Worker.DrainTimeout,
		Vocab:             vocab,
		Clock:             clk,
		Logger:            logger,
	}, nil
}

func readCredential(path string) (string, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read worker credential: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

func writeCredential(path, credential string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(credential+"\n"), 0o600)
}
