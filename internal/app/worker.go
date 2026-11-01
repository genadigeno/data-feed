package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/protocol"
	"github.com/genadigeno/data-feed/internal/security"
	"github.com/genadigeno/data-feed/internal/worker"
)

// RunWorker starts the Worker role and blocks until ctx is cancelled. It opens no
// database and no Coordinator files: the protocol client is its only link.
func RunWorker(ctx context.Context, cfg *config.Config, logger *slog.Logger, clk clock.Clock) error {
	applyRuntimeLimits(cfg, logger)

	tlsConfig, err := security.ClientTLS(security.ClientTLSOptions{
		CAFile:      cfg.Worker.CoordinatorCAFile,
		Fingerprint: cfg.Worker.CoordinatorFingerprint,
		SkipVerify:  cfg.Worker.CoordinatorInsecureSkipVerify,
	})
	if err != nil {
		return err
	}
	if cfg.Worker.CoordinatorInsecureSkipVerify {
		logger.Warn("coordinator certificate verification is disabled")
	}

	client := protocol.NewClient(cfg.Worker.CoordinatorURL, tlsConfig)
	defer client.Close()

	if addr := cfg.Diagnostics.WorkerPprofListen; addr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		defer srv.Close()
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Warn("profiling listener", "err", err)
			}
		}()
	}

	opts, err := remoteWorkerOptions(cfg, client, Vocabulary, clk, logger)
	if err != nil {
		return err
	}
	return worker.New(opts).Run(ctx)
}
