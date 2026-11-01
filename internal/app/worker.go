package app

import (
	"context"
	"log/slog"
	"os"

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

	name := cfg.Worker.Name
	if name == "" {
		if host, err := os.Hostname(); err == nil {
			name = host
		}
	}
	return worker.New(worker.Options{
		Name:       name,
		Transport:  pingAdapter{client},
		Compatible: protocol.CheckCompatible,
		Clock:      clk,
		Logger:     logger,
	}).Run(ctx)
}

type pingAdapter struct{ c *protocol.Client }

func (p pingAdapter) Ping(ctx context.Context) (int, error) {
	resp, err := p.c.Ping(ctx)
	return resp.ProtocolVersion, err
}
