// Package config defines the application configuration: one struct, built once at
// startup, validated, then passed by constructor injection. See
// docs/01-architecture/configuration-model.md.
package config

import (
	"time"

	"github.com/genadigeno/data-feed/internal/security"
)

// Environments.
const (
	EnvDevelopment = "development"
	EnvTest        = "test"
	EnvProduction  = "production"
)

// Config is the complete application configuration.
type Config struct {
	Application Application `yaml:"application"`
	Server      Server      `yaml:"server"`
	Auth        Auth        `yaml:"auth"`
	API         API         `yaml:"api"`
	Persistence Persistence `yaml:"persistence"`
	Coordinator Coordinator `yaml:"coordination"`
	Generation  Generation  `yaml:"generation"`
	Storage     Storage     `yaml:"storage"`
	Worker      Worker      `yaml:"worker"`
	AI          AI          `yaml:"ai"`
	Vocab       Vocab       `yaml:"vocab"`
	Logging     Logging     `yaml:"logging"`
	Diagnostics Diagnostics `yaml:"diagnostics"`
}

type Application struct {
	Environment string `yaml:"environment"`
}

type TLSFiles struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type Server struct {
	Listen                string        `yaml:"listen"`
	TLS                   TLSFiles      `yaml:"tls"`
	ReadHeaderTimeout     time.Duration `yaml:"read_header_timeout"`
	ReadTimeout           time.Duration `yaml:"read_timeout"`
	WriteTimeout          time.Duration `yaml:"write_timeout"`
	IdleTimeout           time.Duration `yaml:"idle_timeout"`
	MaxHeaderBytes        Size          `yaml:"max_header_bytes"`
	MaxConcurrentRequests int           `yaml:"max_concurrent_requests"`
	ShutdownGrace         time.Duration `yaml:"shutdown_grace"`
}

type Auth struct {
	Mode                         string          `yaml:"mode"`
	InsecureAllowUnauthenticated bool            `yaml:"insecure_allow_unauthenticated"`
	BootstrapAdminToken          security.Secret `yaml:"bootstrap_admin_token"`
	WebSessionTTL                time.Duration   `yaml:"web_session_ttl"`
}

type RateLimit struct {
	RequestsPerSecond float64 `yaml:"requests_per_second"`
	Burst             int     `yaml:"burst"`
}

type API struct {
	MaxBodyBytes       Size          `yaml:"max_body_bytes"`
	MaxImportBytes     Size          `yaml:"max_import_bytes"`
	SyncMaxRecords     int64         `yaml:"sync_max_records"`
	SyncMaxBytes       Size          `yaml:"sync_max_bytes"`
	PageDefaultLimit   int           `yaml:"page_default_limit"`
	PageMaxLimit       int           `yaml:"page_max_limit"`
	RateLimit          RateLimit     `yaml:"rate_limit"`
	IdempotencyTTL     time.Duration `yaml:"idempotency_ttl"`
	MaxSSEClients      int           `yaml:"max_sse_clients"`
	SSEHeartbeat       time.Duration `yaml:"sse_heartbeat"`
	MetricsRequireAuth bool          `yaml:"metrics_require_auth"`
}

type Persistence struct {
	SQLitePath         string        `yaml:"sqlite_path"`
	BusyTimeout        time.Duration `yaml:"busy_timeout"`
	MaxReadConnections int           `yaml:"max_read_connections"`
	Durability         string        `yaml:"durability"`
	AllowNetworkFS     bool          `yaml:"allow_network_fs"`
}

type CoordinatorTLS struct {
	CertFile       string `yaml:"cert_file"`
	KeyFile        string `yaml:"key_file"`
	AutoSelfSigned bool   `yaml:"auto_self_signed"`
	ClientCAFile   string `yaml:"client_ca_file"`
}

type Coordinator struct {
	Listen                   string         `yaml:"listen"`
	TLS                      CoordinatorTLS `yaml:"tls"`
	LeaseDuration            time.Duration  `yaml:"lease_duration"`
	HeartbeatInterval        time.Duration  `yaml:"heartbeat_interval"`
	HeartbeatPersistInterval time.Duration  `yaml:"heartbeat_persist_interval"`
	ReaperInterval           time.Duration  `yaml:"reaper_interval"`
	RestartGrace             time.Duration  `yaml:"restart_grace"`
	LongPollMax              time.Duration  `yaml:"long_poll_max"`
	MaxRegisteredWorkers     int            `yaml:"max_registered_workers"`
	MaxLeasesPerRequest      int            `yaml:"max_leases_per_request"`
	PartUploadConcurrency    int            `yaml:"part_upload_concurrency"`
	MaxUnitAttempts          int            `yaml:"max_unit_attempts"`
	NoWorkerTimeout          time.Duration  `yaml:"no_worker_timeout"`
	OrphanLeaseHeartbeats    int            `yaml:"orphan_lease_heartbeats"`
}

type RandomCaps struct {
	MaxDepth        int `yaml:"max_depth"`
	MaxFields       int `yaml:"max_fields"`
	MaxArrayLength  int `yaml:"max_array_length"`
	MaxStringLength int `yaml:"max_string_length"`
}

type Generation struct {
	MaxConcurrentSessions int           `yaml:"max_concurrent_sessions"`
	MaxWorkersPerSession  int           `yaml:"max_workers_per_session"`
	DefaultBatchSize      int           `yaml:"default_batch_size"`
	MaxBatchSize          int           `yaml:"max_batch_size"`
	QueueCapacity         int           `yaml:"queue_capacity"`
	MaxRecordSizeBytes    Size          `yaml:"max_record_size_bytes"`
	UnitRecords           int64         `yaml:"unit_records"`
	UnitMaxBytes          Size          `yaml:"unit_max_bytes"`
	PartMaxBytes          Size          `yaml:"part_max_bytes"`
	PartMaxRecords        int64         `yaml:"part_max_records"`
	PartMaxAge            time.Duration `yaml:"part_max_age"`
	StreamWindow          int           `yaml:"stream_window"`
	PauseGrace            time.Duration `yaml:"pause_grace"`
	MaxSnapshotBytes      Size          `yaml:"max_snapshot_bytes"`
	MaxTotalRecords       int64         `yaml:"max_total_records"`
	RandomCaps            RandomCaps    `yaml:"random_caps"`
}

type Retention struct {
	MaxAge        time.Duration `yaml:"max_age"`
	MaxTotalBytes Size          `yaml:"max_total_bytes"`
	Interval      time.Duration `yaml:"interval"`
}

type Storage struct {
	DataDir         string        `yaml:"data_dir"`
	MinFreeBytes    Size          `yaml:"min_free_bytes"`
	DiskFullTimeout time.Duration `yaml:"disk_full_timeout"`
	Compression     string        `yaml:"compression"`
	Retention       Retention     `yaml:"retention"`
}

type Worker struct {
	CoordinatorURL                string            `yaml:"coordinator_url"`
	Name                          string            `yaml:"name"`
	Labels                        map[string]string `yaml:"labels"`
	JoinToken                     security.Secret   `yaml:"join_token"`
	CredentialFile                string            `yaml:"credential_file"`
	CoordinatorCAFile             string            `yaml:"coordinator_ca_file"`
	CoordinatorFingerprint        string            `yaml:"coordinator_fingerprint"`
	CoordinatorInsecureSkipVerify bool              `yaml:"coordinator_insecure_skip_verify"`
	Slots                         int               `yaml:"slots"`
	LocalSlots                    int               `yaml:"local_slots"`
	MaxCPUPercent                 int               `yaml:"max_cpu_percent"`
	MaxMemoryBytes                Size              `yaml:"max_memory_bytes"`
	SpoolEnabled                  bool              `yaml:"spool_enabled"`
	SpoolPath                     string            `yaml:"spool_path"`
	SpoolMaxBytes                 Size              `yaml:"spool_max_bytes"`
	UploadConcurrency             int               `yaml:"upload_concurrency"`
	DrainTimeout                  time.Duration     `yaml:"drain_timeout"`
}

type AI struct {
	Enabled               bool            `yaml:"enabled"`
	Provider              string          `yaml:"provider"`
	APIKey                security.Secret `yaml:"api_key"`
	BaseURL               string          `yaml:"base_url"`
	TaxonomyModel         string          `yaml:"taxonomy_model"`
	ValuesModel           string          `yaml:"values_model"`
	TaxonomyEffort        string          `yaml:"taxonomy_effort"`
	ValuesEffort          string          `yaml:"values_effort"`
	RequestTimeout        time.Duration   `yaml:"request_timeout"`
	MaxRetries            int             `yaml:"max_retries"`
	MaxConcurrentRequests int             `yaml:"max_concurrent_requests"`
	RequestsPerMinute     int             `yaml:"requests_per_minute"`
	BatchValues           int             `yaml:"batch_values"`
	MaxValueLength        int             `yaml:"max_value_length"`
	MaxValuesPerGroup     int             `yaml:"max_values_per_group"`
	MaxGroups             int             `yaml:"max_groups"`
	MaxTotalTokens        int64           `yaml:"max_total_tokens"`
	CacheEnabled          bool            `yaml:"cache_enabled"`
}

type SizeLevel struct {
	ValuesPerGroup int `yaml:"values_per_group"`
	MaxGroups      int `yaml:"max_groups"`
}

type SizeLevels struct {
	Low    SizeLevel `yaml:"low"`
	Medium SizeLevel `yaml:"medium"`
	High   SizeLevel `yaml:"high"`
}

type Vocab struct {
	MaxCatalogValues int        `yaml:"max_catalog_values"`
	SizeLevels       SizeLevels `yaml:"size_levels"`
}

type Logging struct {
	Level             string `yaml:"level"`
	Format            string `yaml:"format"`
	SessionLogMaxSize Size   `yaml:"session_log_max_bytes"`
	SessionLogFiles   int    `yaml:"session_log_files"`
}

type Diagnostics struct {
	Pprof             bool   `yaml:"pprof"`
	WorkerPprofListen string `yaml:"worker_pprof_listen"`
}

// Defaults returns the built-in defaults adjusted for an environment.
func Defaults(environment string) Config {
	c := Config{
		Application: Application{Environment: environment},
		Server: Server{
			Listen:                "127.0.0.1:8080",
			ReadHeaderTimeout:     5 * time.Second,
			ReadTimeout:           30 * time.Second,
			WriteTimeout:          60 * time.Second,
			IdleTimeout:           120 * time.Second,
			MaxHeaderBytes:        64 * KiB,
			MaxConcurrentRequests: 512,
			ShutdownGrace:         30 * time.Second,
		},
		Auth: Auth{Mode: "none", WebSessionTTL: 12 * time.Hour},
		API: API{
			MaxBodyBytes:       4 * MiB,
			MaxImportBytes:     32 * MiB,
			SyncMaxRecords:     10000,
			SyncMaxBytes:       16 * MiB,
			PageDefaultLimit:   50,
			PageMaxLimit:       500,
			RateLimit:          RateLimit{RequestsPerSecond: 50, Burst: 100},
			IdempotencyTTL:     24 * time.Hour,
			MaxSSEClients:      256,
			SSEHeartbeat:       15 * time.Second,
			MetricsRequireAuth: true,
		},
		Persistence: Persistence{
			BusyTimeout:        5 * time.Second,
			MaxReadConnections: 4,
			Durability:         "normal",
		},
		Coordinator: Coordinator{
			Listen:                   "127.0.0.1:8081",
			TLS:                      CoordinatorTLS{AutoSelfSigned: true},
			LeaseDuration:            30 * time.Second,
			HeartbeatInterval:        5 * time.Second,
			HeartbeatPersistInterval: 15 * time.Second,
			ReaperInterval:           5 * time.Second,
			RestartGrace:             60 * time.Second,
			LongPollMax:              25 * time.Second,
			MaxRegisteredWorkers:     100,
			MaxLeasesPerRequest:      8,
			PartUploadConcurrency:    16,
			MaxUnitAttempts:          5,
			OrphanLeaseHeartbeats:    2,
		},
		Generation: Generation{
			MaxConcurrentSessions: 10,
			MaxWorkersPerSession:  8,
			DefaultBatchSize:      1000,
			MaxBatchSize:          10000,
			QueueCapacity:         4,
			MaxRecordSizeBytes:    64 * KiB,
			UnitRecords:           250000,
			UnitMaxBytes:          512 * MiB,
			PartMaxBytes:          64 * MiB,
			PartMaxAge:            60 * time.Second,
			PauseGrace:            10 * time.Second,
			MaxSnapshotBytes:      64 * MiB,
			RandomCaps:            RandomCaps{MaxDepth: 16, MaxFields: 256, MaxArrayLength: 1024, MaxStringLength: 65536},
		},
		Storage: Storage{
			DataDir:         "./data",
			MinFreeBytes:    2 * GiB,
			DiskFullTimeout: 10 * time.Minute,
			Compression:     "none",
			Retention:       Retention{Interval: time.Hour},
		},
		Worker: Worker{
			CredentialFile:    "./worker-credential",
			LocalSlots:        -1,
			MaxCPUPercent:     100,
			SpoolEnabled:      true,
			SpoolPath:         "./worker-spool",
			UploadConcurrency: 4,
			DrainTimeout:      60 * time.Second,
		},
		AI: AI{
			Provider:              "anthropic",
			TaxonomyModel:         "claude-opus-5-5",
			ValuesModel:           "claude-haiku-5-5",
			TaxonomyEffort:        "medium",
			ValuesEffort:          "low",
			RequestTimeout:        120 * time.Second,
			MaxRetries:            4,
			MaxConcurrentRequests: 4,
			RequestsPerMinute:     50,
			BatchValues:           200,
			MaxValueLength:        200,
			MaxValuesPerGroup:     10000,
			MaxGroups:             64,
			MaxTotalTokens:        2000000,
			CacheEnabled:          true,
		},
		Vocab: Vocab{
			MaxCatalogValues: 1000000,
			SizeLevels: SizeLevels{
				Low:    SizeLevel{ValuesPerGroup: 50, MaxGroups: 10},
				Medium: SizeLevel{ValuesPerGroup: 250, MaxGroups: 20},
				High:   SizeLevel{ValuesPerGroup: 1000, MaxGroups: 30},
			},
		},
		Logging: Logging{Level: "info", Format: "text", SessionLogMaxSize: 64 * MiB, SessionLogFiles: 4},
	}

	switch environment {
	case EnvTest:
		c.Worker.LocalSlots = 1
	case EnvProduction:
		c.Auth.Mode = "token"
		c.Logging.Format = "json"
	}
	return c
}
