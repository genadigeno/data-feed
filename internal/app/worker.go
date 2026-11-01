package app

import (
	"context"
	"log/slog"

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

	opts, err := remoteWorkerOptions(cfg, client, Vocabulary, clk, logger)
	if err != nil {
		return err
	}
	return worker.New(opts).Run(ctx)
}
