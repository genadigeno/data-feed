package app

import (
	"log/slog"
	"runtime"
	"runtime/debug"

	"github.com/genadigeno/data-feed/internal/config"
)

// cpuLimit converts worker.max_cpu_percent into a GOMAXPROCS value.
func cpuLimit(numCPU, percent int) int {
	return max((numCPU*percent+99)/100, 1)
}

// applyRuntimeLimits turns the worker resource settings into Go runtime limits.
func applyRuntimeLimits(cfg *config.Config, logger *slog.Logger) {
	debug.SetMemoryLimit(int64(cfg.Worker.MaxMemoryBytes))
	if cfg.Worker.MaxCPUPercent < 100 {
		runtime.GOMAXPROCS(cpuLimit(runtime.GOMAXPROCS(0), cfg.Worker.MaxCPUPercent))
	}
	logger.Debug("runtime limits applied", "gomaxprocs", runtime.GOMAXPROCS(0), "memory_limit", cfg.Worker.MaxMemoryBytes)
}
