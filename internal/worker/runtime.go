// Package worker is the Worker runtime. It never touches SQLite or the Coordinator's
// filesystem: everything goes through Transport.
//
// Phase 2 scope: connect to the Coordinator, verify protocol compatibility, keep
// checking the connection, and stop cleanly. Registration and task execution arrive
// in Phase 6.
package worker

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/genadigeno/data-feed/internal/clock"
)

const (
	checkInterval = 5 * time.Second
	backoffBase   = 250 * time.Millisecond
	backoffCap    = 30 * time.Second
)

// Transport is the Worker's view of the Coordinator.
type Transport interface {
	// Ping returns the Coordinator's protocol version.
	Ping(ctx context.Context) (protocolVersion int, err error)
}

// Options configures a Runtime.
type Options struct {
	Name      string
	Transport Transport
	// Compatible rejects Coordinators this binary cannot work with.
	Compatible func(protocolVersion int) error
	Clock      clock.Clock
	Logger     *slog.Logger
	// Rand returns a value in [0, 1) for retry jitter. Nil means math/rand/v2.
	Rand func() float64
}

// Runtime keeps a Worker connected.
type Runtime struct{ o Options }

// New returns a Runtime.
func New(o Options) *Runtime {
	if o.Rand == nil {
		o.Rand = rand.Float64
	}
	return &Runtime{o: o}
}

// Backoff returns the wait before retry number attempt (0-based): exponential with
// full jitter, capped.
func Backoff(attempt int, random float64) time.Duration {
	ceiling := backoffCap
	if attempt < 16 { // beyond this the shift exceeds the cap anyway
		if d := backoffBase << attempt; d < ceiling {
			ceiling = d
		}
	}
	return time.Duration(random * float64(ceiling))
}

// Run blocks until ctx is cancelled, which is a clean stop and returns nil, or until
// the Coordinator is found to be incompatible, which retrying cannot fix.
func (r *Runtime) Run(ctx context.Context) error {
	log := r.o.Logger.With("component", "worker", "worker_name", r.o.Name)
	log.Info("worker started: task execution is not available yet, this build only verifies connectivity")

	connected := false
	failures := 0
	for {
		wait := checkInterval
		version, err := r.o.Transport.Ping(ctx)
		switch {
		case ctx.Err() != nil:
			log.Info("worker stopped")
			return nil
		case err != nil:
			if connected || failures == 0 {
				log.Warn("coordinator unreachable, retrying", "err", err)
			}
			connected = false
			wait = Backoff(failures, r.o.Rand())
			failures++
		default:
			if err := r.o.Compatible(version); err != nil {
				return err
			}
			if !connected {
				log.Info("connected to coordinator", "protocol_version", version)
			}
			connected = true
			failures = 0
		}

		select {
		case <-ctx.Done():
			log.Info("worker stopped")
			return nil
		case <-r.o.Clock.After(wait):
		}
	}
}
