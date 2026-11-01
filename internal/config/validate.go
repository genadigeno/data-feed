package config

import (
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
)

// Validation problem codes.
const (
	codeConfigValue    = "ERR_CONFIG_VALUE"
	codeConfigTiming   = "ERR_CONFIG_TIMING"
	codeConfigLimit    = "ERR_CONFIG_LIMIT"
	codeConfigMemory   = "ERR_CONFIG_MEMORY"
	codeConfigSpool    = "ERR_CONFIG_SPOOL"
	codeConfigInsecure = "ERR_CONFIG_INSECURE"
	codeConfigSecret   = "ERR_CONFIG_SECRET"
	codeConfigAI       = "ERR_CONFIG_AI"
	codeConfigWorker   = "ERR_CONFIG_WORKER"
)

const (
	memoryBudgetPercent = 80 // of max_memory_bytes usable by unit executors
	defaultMemoryShare  = 2  // max_memory_bytes defaults to 1/2 of detected memory
	unknownMemoryBytes  = 4 * GiB
	minSpoolBytes       = GiB
)

// Facts are host properties needed to resolve automatic settings.
type Facts struct {
	NumCPU      int
	MemoryBytes int64 // 0 if unknown
}

// perSlotMemory is the worst-case heap of one unit executor using default batches:
// (queue_capacity + 2) × batch_size × max_record_size_bytes.
func (c *Config) perSlotMemory() int64 {
	return int64(c.Generation.QueueCapacity+2) * int64(c.Generation.DefaultBatchSize) * int64(c.Generation.MaxRecordSizeBytes)
}

// Normalize fills automatic settings. Automatic slot counts are capped by what fits
// in the memory budget, so defaults work on small machines; explicit values are left
// for Validate to accept or reject.
func (c *Config) Normalize(f Facts) {
	if c.Persistence.SQLitePath == "" {
		c.Persistence.SQLitePath = filepath.Join(c.Storage.DataDir, "datafeed.db")
	}
	if c.Worker.MaxMemoryBytes == 0 {
		if f.MemoryBytes > 0 {
			c.Worker.MaxMemoryBytes = Size(f.MemoryBytes / defaultMemoryShare)
		} else {
			c.Worker.MaxMemoryBytes = unknownMemoryBytes
		}
	}

	autoSlots := max(f.NumCPU, 1)
	if per := c.perSlotMemory(); per > 0 {
		fit := int64(c.Worker.MaxMemoryBytes) / 100 * memoryBudgetPercent / per
		autoSlots = max(int(min(int64(autoSlots), fit)), 1)
	}
	if c.Worker.Slots == 0 {
		c.Worker.Slots = autoSlots
	}
	if c.Worker.LocalSlots < 0 {
		c.Worker.LocalSlots = autoSlots
	}
	if c.Worker.SpoolMaxBytes == 0 {
		need := 2 * Size(max(c.Worker.Slots, c.Worker.LocalSlots)) * c.Generation.PartMaxBytes
		c.Worker.SpoolMaxBytes = max(need, minSpoolBytes)
	}
}

// Validate checks the normalized configuration and reports all problems together.
func (c *Config) Validate(role Role) []apperr.Problem {
	v := &validator{}
	c.validateCommon(v)
	switch role {
	case RoleCoordinator:
		c.validateCoordinator(v)
		c.validateSlots(v, "worker.local_slots", c.Worker.LocalSlots)
	case RoleWorker:
		c.validateWorker(v)
		c.validateSlots(v, "worker.slots", c.Worker.Slots)
	}
	return v.problems
}

type validator struct{ problems []apperr.Problem }

func (v *validator) add(path, code, format string, args ...any) {
	v.problems = append(v.problems, apperr.Problem{Path: path, Code: code, Message: fmt.Sprintf(format, args...)})
}

func (v *validator) oneOf(path, value string, allowed ...string) {
	for _, a := range allowed {
		if value == a {
			return
		}
	}
	v.add(path, codeConfigValue, "%q is not one of %s", value, strings.Join(allowed, ", "))
}

func (v *validator) positive(path string, n int64) {
	if n <= 0 {
		v.add(path, codeConfigValue, "must be greater than zero")
	}
}

func (v *validator) nonNegative(path string, n int64) {
	if n < 0 {
		v.add(path, codeConfigValue, "must not be negative")
	}
}

func (v *validator) listen(path, addr string) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		v.add(path, codeConfigValue, "invalid listen address %q: %v", addr, err)
		return
	}
	if p, err := strconv.Atoi(port); err != nil || p < 0 || p > 65535 {
		v.add(path, codeConfigValue, "invalid port %q", port)
	}
}

func (v *validator) tlsPair(path, cert, key string) {
	if (cert == "") != (key == "") {
		v.add(path, codeConfigValue, "cert_file and key_file must be set together")
	}
}

// IsLoopback reports whether a listen address only accepts local connections.
func IsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	return isLoopbackHost(host)
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *Config) validateCommon(v *validator) {
	v.oneOf("application.environment", c.Application.Environment, EnvDevelopment, EnvTest, EnvProduction)
	v.oneOf("logging.level", c.Logging.Level, "debug", "info", "warn", "error")
	v.oneOf("logging.format", c.Logging.Format, "text", "json")
	v.positive("logging.session_log_max_bytes", int64(c.Logging.SessionLogMaxSize))
	v.positive("logging.session_log_files", int64(c.Logging.SessionLogFiles))

	g := c.Generation
	v.positive("generation.default_batch_size", int64(g.DefaultBatchSize))
	v.positive("generation.max_batch_size", int64(g.MaxBatchSize))
	if g.DefaultBatchSize > g.MaxBatchSize {
		v.add("generation.default_batch_size", codeConfigLimit, "must not exceed generation.max_batch_size (%d)", g.MaxBatchSize)
	}
	if g.QueueCapacity < 1 {
		v.add("generation.queue_capacity", codeConfigLimit, "must be at least 1")
	}
	v.positive("generation.max_record_size_bytes", int64(g.MaxRecordSizeBytes))
	v.positive("generation.unit_records", g.UnitRecords)
	v.positive("generation.unit_max_bytes", int64(g.UnitMaxBytes))
	v.positive("generation.part_max_bytes", int64(g.PartMaxBytes))
	v.nonNegative("generation.part_max_records", g.PartMaxRecords)
	v.positive("generation.max_snapshot_bytes", int64(g.MaxSnapshotBytes))
	v.nonNegative("generation.max_total_records", g.MaxTotalRecords)
	v.positive("generation.random_caps.max_depth", int64(g.RandomCaps.MaxDepth))
	v.positive("generation.random_caps.max_fields", int64(g.RandomCaps.MaxFields))
	v.positive("generation.random_caps.max_array_length", int64(g.RandomCaps.MaxArrayLength))
	v.positive("generation.random_caps.max_string_length", int64(g.RandomCaps.MaxStringLength))

	if c.Worker.MaxCPUPercent < 1 || c.Worker.MaxCPUPercent > 100 {
		v.add("worker.max_cpu_percent", codeConfigValue, "must be between 1 and 100")
	}
}

func (c *Config) validateSlots(v *validator, path string, slots int) {
	if path == "worker.slots" && slots < 1 {
		v.add(path, codeConfigValue, "must be at least 1")
	}
	if slots < 0 {
		v.add(path, codeConfigValue, "must not be negative")
		return
	}
	if slots == 0 {
		return // Coordinator without an embedded worker.
	}
	budget := int64(c.Worker.MaxMemoryBytes) / 100 * memoryBudgetPercent
	if need := int64(slots) * c.perSlotMemory(); need > budget {
		v.add(path, codeConfigMemory, "%d slots may use %s, above the %d%% budget (%s) of worker.max_memory_bytes",
			slots, Size(need), memoryBudgetPercent, Size(budget))
	}
	if c.Worker.SpoolEnabled {
		if need := 2 * Size(slots) * c.Generation.PartMaxBytes; c.Worker.SpoolMaxBytes < need {
			v.add("worker.spool_max_bytes", codeConfigSpool, "must be at least %s (2 × %s × generation.part_max_bytes)", need, path)
		}
	}
	v.positive("worker.upload_concurrency", int64(c.Worker.UploadConcurrency))
}

func (c *Config) validateCoordinator(v *validator) {
	s := c.Server
	v.listen("server.listen", s.Listen)
	v.tlsPair("server.tls", s.TLS.CertFile, s.TLS.KeyFile)
	v.positive("server.read_header_timeout", int64(s.ReadHeaderTimeout))
	v.positive("server.read_timeout", int64(s.ReadTimeout))
	v.positive("server.write_timeout", int64(s.WriteTimeout))
	v.positive("server.idle_timeout", int64(s.IdleTimeout))
	v.positive("server.max_header_bytes", int64(s.MaxHeaderBytes))
	v.positive("server.max_concurrent_requests", int64(s.MaxConcurrentRequests))
	v.positive("server.shutdown_grace", int64(s.ShutdownGrace))

	v.oneOf("auth.mode", c.Auth.Mode, "none", "token")
	if c.Auth.Mode == "none" && !c.Auth.InsecureAllowUnauthenticated {
		if !IsLoopback(s.Listen) {
			v.add("auth.mode", codeConfigInsecure, "server.listen %q is not loopback: use auth.mode=token, or set auth.insecure_allow_unauthenticated for an isolated lab", s.Listen)
		}
		if c.Application.Environment == EnvProduction {
			v.add("auth.mode", codeConfigInsecure, "production requires auth.mode=token")
		}
	}
	if c.Auth.BootstrapAdminToken.IsLiteral() && c.Application.Environment != EnvDevelopment {
		v.add("auth.bootstrap_admin_token", codeConfigSecret, "literal secrets are only allowed in development; use ${ENV} or file:/path")
	}

	p := c.Persistence
	v.oneOf("persistence.durability", p.Durability, "normal", "strict")
	v.positive("persistence.busy_timeout", int64(p.BusyTimeout))
	v.positive("persistence.max_read_connections", int64(p.MaxReadConnections))

	co := c.Coordinator
	v.listen("coordination.listen", co.Listen)
	v.tlsPair("coordination.tls", co.TLS.CertFile, co.TLS.KeyFile)
	if !IsLoopback(co.Listen) && co.TLS.CertFile == "" && !co.TLS.AutoSelfSigned {
		v.add("coordination.tls", codeConfigInsecure, "coordination.listen %q is not loopback: the worker protocol requires TLS", co.Listen)
	}
	v.positive("coordination.lease_duration", int64(co.LeaseDuration))
	v.positive("coordination.heartbeat_persist_interval", int64(co.HeartbeatPersistInterval))
	v.positive("coordination.long_poll_max", int64(co.LongPollMax))
	v.positive("coordination.max_registered_workers", int64(co.MaxRegisteredWorkers))
	v.positive("coordination.max_leases_per_request", int64(co.MaxLeasesPerRequest))
	v.positive("coordination.part_upload_concurrency", int64(co.PartUploadConcurrency))
	v.positive("coordination.max_unit_attempts", int64(co.MaxUnitAttempts))
	v.positive("coordination.orphan_lease_heartbeats", int64(co.OrphanLeaseHeartbeats))
	v.nonNegative("coordination.no_worker_timeout", int64(co.NoWorkerTimeout))
	if co.HeartbeatInterval < time.Second {
		v.add("coordination.heartbeat_interval", codeConfigTiming, "must be at least 1s")
	}
	if co.LeaseDuration < 3*co.HeartbeatInterval {
		v.add("coordination.lease_duration", codeConfigTiming, "must be at least 3 × coordination.heartbeat_interval")
	}
	if co.ReaperInterval <= 0 || co.ReaperInterval > co.LeaseDuration/2 {
		v.add("coordination.reaper_interval", codeConfigTiming, "must be positive and at most coordination.lease_duration / 2")
	}
	if co.RestartGrace < co.LeaseDuration {
		v.add("coordination.restart_grace", codeConfigTiming, "must be at least coordination.lease_duration")
	}
	if co.LongPollMax >= s.WriteTimeout {
		v.add("coordination.long_poll_max", codeConfigTiming, "must be less than server.write_timeout")
	}

	st := c.Storage
	if st.DataDir == "" {
		v.add("storage.data_dir", codeConfigValue, "must not be empty")
	}
	v.nonNegative("storage.min_free_bytes", int64(st.MinFreeBytes))
	v.positive("storage.disk_full_timeout", int64(st.DiskFullTimeout))
	v.oneOf("storage.compression", st.Compression, "none", "gzip")
	v.nonNegative("storage.retention.max_age", int64(st.Retention.MaxAge))
	v.nonNegative("storage.retention.max_total_bytes", int64(st.Retention.MaxTotalBytes))
	v.positive("storage.retention.interval", int64(st.Retention.Interval))

	a := c.API
	v.positive("api.page_default_limit", int64(a.PageDefaultLimit))
	if a.PageDefaultLimit > a.PageMaxLimit {
		v.add("api.page_default_limit", codeConfigLimit, "must not exceed api.page_max_limit (%d)", a.PageMaxLimit)
	}
	v.positive("api.max_body_bytes", int64(a.MaxBodyBytes))
	v.positive("api.max_sse_clients", int64(a.MaxSSEClients))

	g := c.Generation
	v.positive("generation.max_concurrent_sessions", int64(g.MaxConcurrentSessions))
	v.positive("generation.max_workers_per_session", int64(g.MaxWorkersPerSession))

	c.validateAI(v)
}

func (c *Config) validateAI(v *validator) {
	ai := c.AI
	if ai.APIKey.IsLiteral() && c.Application.Environment != EnvDevelopment {
		v.add("ai.api_key", codeConfigSecret, "literal secrets are only allowed in development; use ${ENV} or file:/path")
	}
	if !ai.Enabled {
		return
	}
	v.oneOf("ai.provider", ai.Provider, "anthropic", "offline")
	if ai.Provider == "anthropic" && ai.APIKey.IsZero() {
		v.add("ai.api_key", codeConfigAI, "required when ai.enabled is true and ai.provider is anthropic")
	}
	v.positive("ai.request_timeout", int64(ai.RequestTimeout))
	v.positive("ai.max_concurrent_requests", int64(ai.MaxConcurrentRequests))
	v.positive("ai.batch_values", int64(ai.BatchValues))
	v.nonNegative("ai.max_retries", int64(ai.MaxRetries))
}

func (c *Config) validateWorker(v *validator) {
	w := c.Worker
	if w.JoinToken.IsLiteral() && c.Application.Environment != EnvDevelopment {
		v.add("worker.join_token", codeConfigSecret, "literal secrets are only allowed in development; use ${ENV} or file:/path")
	}
	v.positive("worker.drain_timeout", int64(w.DrainTimeout))
	if w.CoordinatorInsecureSkipVerify && c.Application.Environment != EnvDevelopment {
		v.add("worker.coordinator_insecure_skip_verify", codeConfigInsecure, "only allowed in development")
	}
	if w.CoordinatorFingerprint != "" && !validFingerprint(w.CoordinatorFingerprint) {
		v.add("worker.coordinator_fingerprint", codeConfigWorker, "want sha256: followed by 64 hex digits")
	}

	if w.CoordinatorURL == "" {
		v.add("worker.coordinator_url", codeConfigWorker, "required (e.g. https://coordinator-host:8081)")
		return
	}
	u, err := url.Parse(w.CoordinatorURL)
	if err != nil || u.Host == "" {
		v.add("worker.coordinator_url", codeConfigWorker, "invalid URL %q", w.CoordinatorURL)
		return
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !isLoopbackHost(u.Hostname()) {
			v.add("worker.coordinator_url", codeConfigInsecure, "plain http is only allowed for a loopback Coordinator")
		}
	default:
		v.add("worker.coordinator_url", codeConfigWorker, "scheme must be https or http")
	}
}

func validFingerprint(s string) bool {
	h, ok := strings.CutPrefix(strings.ToLower(s), "sha256:")
	if !ok || len(h) != 64 {
		return false
	}
	_, err := hex.DecodeString(h)
	return err == nil
}
