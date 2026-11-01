package config

import (
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
)

// files returns a ReadFile backed by a map.
func files(m map[string]string) func(string) ([]byte, error) {
	return func(p string) ([]byte, error) {
		if s, ok := m[p]; ok {
			return []byte(s), nil
		}
		return nil, fs.ErrNotExist
	}
}

func load(t *testing.T, o Options) *Loaded {
	t.Helper()
	if o.ReadFile == nil {
		o.ReadFile = files(nil)
	}
	l, err := Load(o)
	if err != nil {
		t.Fatalf("Load: %v", problemsOf(t, err))
	}
	return l
}

func problemsOf(t *testing.T, err error) []apperr.Problem {
	t.Helper()
	e, ok := apperr.As(err)
	if !ok {
		t.Fatalf("not an apperr: %v", err)
	}
	return e.Problems
}

func hasProblem(problems []apperr.Problem, path, code string) bool {
	for _, p := range problems {
		if p.Path == path && p.Code == code {
			return true
		}
	}
	return false
}

var bigHost = Facts{NumCPU: 8, MemoryBytes: int64(64 * GiB)}

func valid(env string) *Config {
	c := Defaults(env)
	c.Worker.CoordinatorURL = "https://coordinator:8081"
	c.Normalize(bigHost)
	return &c
}

func TestPrecedenceDefaultsFileEnvFlags(t *testing.T) {
	file := "generation:\n  queue_capacity: 5\n  default_batch_size: 500\n  max_batch_size: 9000\n"
	l := load(t, Options{
		Path:     "c.yaml",
		ReadFile: files(map[string]string{"c.yaml": file}),
		Environ: []string{
			"DATAGEN_GENERATION__DEFAULT_BATCH_SIZE=600",
			"DATAGEN_GENERATION__MAX_BATCH_SIZE=8000",
		},
		Flags: map[string]string{"generation.max_batch_size": "7000"},
	})

	g := l.Config.Generation
	if g.QueueCapacity != 5 || g.DefaultBatchSize != 600 || g.MaxBatchSize != 7000 {
		t.Fatalf("merged = queue %d, batch %d, max %d", g.QueueCapacity, g.DefaultBatchSize, g.MaxBatchSize)
	}
	if g.UnitRecords != Defaults(EnvDevelopment).Generation.UnitRecords {
		t.Fatalf("untouched default changed: %d", g.UnitRecords)
	}

	wantSources := map[string]string{
		"generation.queue_capacity":     "file",
		"generation.default_batch_size": "env:DATAGEN_GENERATION__DEFAULT_BATCH_SIZE",
		"generation.max_batch_size":     "flag",
	}
	for path, want := range wantSources {
		if got := l.Sources[path]; got != want {
			t.Errorf("source of %s = %q, want %q", path, got, want)
		}
	}
	for _, e := range l.Entries() {
		if e.Path == "generation.unit_records" && e.Source != "default" {
			t.Errorf("source of unit_records = %q", e.Source)
		}
	}
}

func TestProfileDefaultsAreBelowFile(t *testing.T) {
	l := load(t, Options{
		Path:     "c.yaml",
		ReadFile: files(map[string]string{"c.yaml": "application:\n  environment: production\n"}),
	})
	if l.Config.Auth.Mode != "token" || l.Config.Logging.Format != "json" {
		t.Fatalf("production profile not applied: %+v %+v", l.Config.Auth.Mode, l.Config.Logging.Format)
	}

	l = load(t, Options{
		Path:     "c.yaml",
		ReadFile: files(map[string]string{"c.yaml": "application:\n  environment: production\nlogging:\n  format: text\n"}),
	})
	if l.Config.Logging.Format != "text" {
		t.Fatal("file did not override profile default")
	}

	l = load(t, Options{Environ: []string{"DATAGEN_APPLICATION__ENVIRONMENT=test"}})
	if l.Config.Worker.LocalSlots != 1 {
		t.Fatalf("environment from env var did not select the profile: %d", l.Config.Worker.LocalSlots)
	}
}

func TestConfigFileDiscovery(t *testing.T) {
	// No file anywhere is fine.
	if l := load(t, Options{}); l.File != "" {
		t.Fatalf("File = %q", l.File)
	}
	// The default name is picked up.
	l := load(t, Options{ReadFile: files(map[string]string{defaultFile: "logging:\n  level: debug\n"})})
	if l.File != defaultFile || l.Config.Logging.Level != "debug" {
		t.Fatalf("default file not used: %q %q", l.File, l.Config.Logging.Level)
	}
	// An explicit path must exist, whether from the flag or the environment.
	if _, err := Load(Options{Path: "missing.yaml", ReadFile: files(nil)}); err == nil {
		t.Fatal("missing explicit file accepted")
	}
	if _, err := Load(Options{Environ: []string{"DATAGEN_CONFIG=missing.yaml"}, ReadFile: files(nil)}); err == nil {
		t.Fatal("missing DATAGEN_CONFIG file accepted")
	}
	// A read error other than not-exist is never ignored.
	_, err := Load(Options{ReadFile: func(string) ([]byte, error) { return nil, errors.New("denied") }})
	if err == nil {
		t.Fatal("unreadable default file ignored")
	}
}

func TestInvalidInputsAreRejected(t *testing.T) {
	cases := map[string]Options{
		"unknown file key": {Path: "c", ReadFile: files(map[string]string{"c": "generation:\n  queue_capacityy: 1\n"})},
		"malformed yaml":   {Path: "c", ReadFile: files(map[string]string{"c": ":\n\t- x"})},
		"bad size":         {Path: "c", ReadFile: files(map[string]string{"c": "generation:\n  part_max_bytes: lots\n"})},
		"unknown env":      {Environ: []string{"DATAGEN_NOPE__X=1"}, ReadFile: files(nil)},
		"section env":      {Environ: []string{"DATAGEN_GENERATION=1"}, ReadFile: files(nil)},
		"bad env int":      {Environ: []string{"DATAGEN_GENERATION__QUEUE_CAPACITY=x"}, ReadFile: files(nil)},
		"bad env duration": {Environ: []string{"DATAGEN_COORDINATION__LEASE_DURATION=30"}, ReadFile: files(nil)},
		"unknown flag":     {Flags: map[string]string{"no.such": "1"}, ReadFile: files(nil)},
	}
	for name, o := range cases {
		if _, err := Load(o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestAllEnvProblemsReportedTogether(t *testing.T) {
	_, err := Load(Options{
		Environ:  []string{"DATAGEN_NOPE=1", "DATAGEN_GENERATION__QUEUE_CAPACITY=x"},
		ReadFile: files(nil),
	})
	if got := len(problemsOf(t, err)); got != 2 {
		t.Fatalf("got %d problems, want 2", got)
	}
}

func TestReservedEnvVarsAreNotSettings(t *testing.T) {
	l := load(t, Options{Environ: []string{"DATAGEN_SERVER=http://x", "DATAGEN_TOKEN=abc", "DATAGEN_DATA_DIR=/d", "UNRELATED=1"}})
	if l.Config.Storage.DataDir != "/d" {
		t.Fatalf("DATAGEN_DATA_DIR not applied: %q", l.Config.Storage.DataDir)
	}
}

func TestValueSyntaxFromEnv(t *testing.T) {
	l := load(t, Options{Environ: []string{
		"DATAGEN_COORDINATION__LEASE_DURATION=45s",
		"DATAGEN_GENERATION__PART_MAX_BYTES=128MiB",
		"DATAGEN_WORKER__SPOOL_ENABLED=false",
		"DATAGEN_WORKER__LABELS=zone=eu, tier=gold",
		"DATAGEN_API__RATE_LIMIT__REQUESTS_PER_SECOND=2.5",
	}})
	c := l.Config
	if c.Coordinator.LeaseDuration != 45*time.Second {
		t.Errorf("duration = %v", c.Coordinator.LeaseDuration)
	}
	if c.Generation.PartMaxBytes != 128*MiB {
		t.Errorf("size = %v", c.Generation.PartMaxBytes)
	}
	if c.Worker.SpoolEnabled {
		t.Error("bool not applied")
	}
	if c.Worker.Labels["zone"] != "eu" || c.Worker.Labels["tier"] != "gold" {
		t.Errorf("labels = %v", c.Worker.Labels)
	}
	if c.API.RateLimit.RequestsPerSecond != 2.5 {
		t.Errorf("float = %v", c.API.RateLimit.RequestsPerSecond)
	}
}

func TestSecretsResolvedOnlyForRole(t *testing.T) {
	file := "ai:\n  api_key: ${AI_KEY}\nworker:\n  join_token: ${JOIN}\n"
	read := files(map[string]string{"c": file})

	// AI disabled: its unset key does not block a Coordinator.
	load(t, Options{Path: "c", ReadFile: read, Role: RoleCoordinator})

	// A Worker needs only its join token.
	if _, err := Load(Options{Path: "c", ReadFile: read, Role: RoleWorker}); err == nil {
		t.Fatal("worker started with unresolved join token")
	}
	l := load(t, Options{Path: "c", ReadFile: read, Role: RoleWorker, Environ: []string{"JOIN=tok"}})
	if l.Config.Worker.JoinToken.Reveal() != "tok" {
		t.Fatalf("join token = %q", l.Config.Worker.JoinToken.Reveal())
	}

	// Enabling AI makes its key required.
	_, err := Load(Options{Path: "c", ReadFile: read, Role: RoleCoordinator, Flags: map[string]string{"ai.enabled": "true"}})
	if err == nil || !hasProblem(problemsOf(t, err), "ai.api_key", codeConfigSecret) {
		t.Fatalf("unresolved AI key: %v", err)
	}
}

func TestEntriesNeverExposeSecrets(t *testing.T) {
	l := load(t, Options{
		Environ: []string{"DATAGEN_AUTH__BOOTSTRAP_ADMIN_TOKEN=${ADMIN}", "ADMIN=hunter2"},
	})
	if l.Config.Auth.BootstrapAdminToken.Reveal() != "hunter2" {
		t.Fatal("secret not resolved")
	}
	for _, e := range l.Entries() {
		if strings.Contains(e.Value, "hunter2") {
			t.Fatalf("%s exposes the secret: %q", e.Path, e.Value)
		}
	}
}

func TestDefaultsAreValidForDevelopment(t *testing.T) {
	for _, role := range []Role{RoleCoordinator, RoleWorker} {
		if problems := valid(EnvDevelopment).Validate(role); len(problems) > 0 {
			t.Errorf("role %d: %v", role, problems)
		}
	}
}

func TestProductionDefaultsAreValid(t *testing.T) {
	if problems := valid(EnvProduction).Validate(RoleCoordinator); len(problems) > 0 {
		t.Fatal(problems)
	}
}

func TestNormalizeResolvesAutomaticSettings(t *testing.T) {
	c := Defaults(EnvDevelopment)
	c.Storage.DataDir = "base"
	c.Normalize(bigHost)

	if !strings.HasSuffix(strings.ReplaceAll(c.Persistence.SQLitePath, `\`, "/"), "base/datagen.db") {
		t.Errorf("sqlite path = %q", c.Persistence.SQLitePath)
	}
	if c.Worker.Slots != 8 || c.Worker.LocalSlots != 8 {
		t.Errorf("slots = %d / %d", c.Worker.Slots, c.Worker.LocalSlots)
	}
	if c.Worker.MaxMemoryBytes != 32*GiB {
		t.Errorf("max memory = %v", c.Worker.MaxMemoryBytes)
	}
	if c.Worker.SpoolMaxBytes != GiB {
		t.Errorf("spool = %v", c.Worker.SpoolMaxBytes)
	}
}

func TestAutomaticSettingsFitEveryHostShape(t *testing.T) {
	for _, f := range []Facts{
		{NumCPU: 1, MemoryBytes: int64(GiB)},
		{NumCPU: 8, MemoryBytes: int64(2 * GiB)},
		{NumCPU: 64, MemoryBytes: int64(8 * GiB)},
		{NumCPU: 64, MemoryBytes: int64(512 * GiB)},
		{NumCPU: 128, MemoryBytes: 0},
	} {
		c := Defaults(EnvDevelopment)
		c.Normalize(f)
		if problems := c.Validate(RoleCoordinator); len(problems) > 0 {
			t.Errorf("%+v: %v", f, problems)
		}
	}
}

func TestExplicitValuesSurviveNormalize(t *testing.T) {
	c := Defaults(EnvDevelopment)
	c.Worker.Slots = 3
	c.Worker.LocalSlots = 0
	c.Worker.MaxMemoryBytes = 16 * GiB
	c.Worker.SpoolMaxBytes = 2 * GiB
	c.Persistence.SQLitePath = "x.db"
	c.Normalize(bigHost)

	if c.Worker.Slots != 3 || c.Worker.LocalSlots != 0 || c.Worker.MaxMemoryBytes != 16*GiB ||
		c.Worker.SpoolMaxBytes != 2*GiB || c.Persistence.SQLitePath != "x.db" {
		t.Fatalf("explicit values changed: %+v", c.Worker)
	}
}

func TestValidationRules(t *testing.T) {
	cases := []struct {
		name   string
		role   Role
		env    string
		mutate func(*Config)
		path   string
		code   string
	}{
		{"lease too short", RoleCoordinator, "", func(c *Config) { c.Coordinator.LeaseDuration = 10 * time.Second }, "coordination.lease_duration", codeConfigTiming},
		{"reaper too slow", RoleCoordinator, "", func(c *Config) { c.Coordinator.ReaperInterval = 20 * time.Second }, "coordination.reaper_interval", codeConfigTiming},
		{"grace below lease", RoleCoordinator, "", func(c *Config) { c.Coordinator.RestartGrace = 10 * time.Second }, "coordination.restart_grace", codeConfigTiming},
		{"long poll vs write timeout", RoleCoordinator, "", func(c *Config) { c.Coordinator.LongPollMax = time.Minute }, "coordination.long_poll_max", codeConfigTiming},
		{"heartbeat below 1s", RoleCoordinator, "", func(c *Config) { c.Coordinator.HeartbeatInterval = time.Millisecond }, "coordination.heartbeat_interval", codeConfigTiming},
		{"batch above max", RoleCoordinator, "", func(c *Config) { c.Generation.DefaultBatchSize = 20000 }, "generation.default_batch_size", codeConfigLimit},
		{"queue zero", RoleCoordinator, "", func(c *Config) { c.Generation.QueueCapacity = 0 }, "generation.queue_capacity", codeConfigLimit},
		{"local slots memory", RoleCoordinator, "", func(c *Config) { c.Worker.LocalSlots = 1000 }, "worker.local_slots", codeConfigMemory},
		{"worker slots memory", RoleWorker, "", func(c *Config) { c.Worker.Slots = 1000 }, "worker.slots", codeConfigMemory},
		{"spool too small", RoleWorker, "", func(c *Config) { c.Worker.SpoolMaxBytes = MiB }, "worker.spool_max_bytes", codeConfigSpool},
		{"public bind without auth", RoleCoordinator, "", func(c *Config) { c.Server.Listen = "0.0.0.0:8080" }, "auth.mode", codeConfigInsecure},
		{"empty host is public", RoleCoordinator, "", func(c *Config) { c.Server.Listen = ":8080" }, "auth.mode", codeConfigInsecure},
		{"production without auth", RoleCoordinator, EnvProduction, func(c *Config) { c.Auth.Mode = "none" }, "auth.mode", codeConfigInsecure},
		{"worker listener without TLS", RoleCoordinator, "", func(c *Config) {
			c.Coordinator.Listen = "0.0.0.0:8081"
			c.Coordinator.TLS.AutoSelfSigned = false
		}, "coordination.tls", codeConfigInsecure},
		{"half a TLS pair", RoleCoordinator, "", func(c *Config) { c.Server.TLS.CertFile = "c.pem" }, "server.tls", codeConfigValue},
		{"bad listen", RoleCoordinator, "", func(c *Config) { c.Server.Listen = "nope" }, "server.listen", codeConfigValue},
		{"bad port", RoleCoordinator, "", func(c *Config) { c.Server.Listen = "127.0.0.1:99999" }, "server.listen", codeConfigValue},
		{"bad durability", RoleCoordinator, "", func(c *Config) { c.Persistence.Durability = "maybe" }, "persistence.durability", codeConfigValue},
		{"bad log level", RoleCoordinator, "", func(c *Config) { c.Logging.Level = "loud" }, "logging.level", codeConfigValue},
		{"bad cpu percent", RoleWorker, "", func(c *Config) { c.Worker.MaxCPUPercent = 150 }, "worker.max_cpu_percent", codeConfigValue},
		{"AI without key", RoleCoordinator, "", func(c *Config) { c.AI.Enabled = true }, "ai.api_key", codeConfigAI},
		{"bad environment", RoleCoordinator, "", func(c *Config) { c.Application.Environment = "staging" }, "application.environment", codeConfigValue},
		{"worker without URL", RoleWorker, "", func(c *Config) { c.Worker.CoordinatorURL = "" }, "worker.coordinator_url", codeConfigWorker},
		{"worker bad scheme", RoleWorker, "", func(c *Config) { c.Worker.CoordinatorURL = "ftp://h:1" }, "worker.coordinator_url", codeConfigWorker},
		{"worker remote http", RoleWorker, "", func(c *Config) { c.Worker.CoordinatorURL = "http://remote:8081" }, "worker.coordinator_url", codeConfigInsecure},
		{"skip verify in production", RoleWorker, EnvProduction, func(c *Config) { c.Worker.CoordinatorInsecureSkipVerify = true }, "worker.coordinator_insecure_skip_verify", codeConfigInsecure},
		{"bad fingerprint", RoleWorker, "", func(c *Config) { c.Worker.CoordinatorFingerprint = "abc" }, "worker.coordinator_fingerprint", codeConfigWorker},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := tc.env
			if env == "" {
				env = EnvDevelopment
			}
			c := valid(env)
			tc.mutate(c)
			if problems := c.Validate(tc.role); !hasProblem(problems, tc.path, tc.code) {
				t.Fatalf("want %s at %s, got %v", tc.code, tc.path, problems)
			}
		})
	}
}

func TestAcceptedInsecureSetups(t *testing.T) {
	c := valid(EnvDevelopment)
	c.Server.Listen = "0.0.0.0:8080"
	c.Auth.InsecureAllowUnauthenticated = true
	c.Coordinator.Listen = "0.0.0.0:8081"
	if problems := c.Validate(RoleCoordinator); len(problems) > 0 {
		t.Errorf("lab setup rejected: %v", problems)
	}

	c = valid(EnvDevelopment)
	c.Worker.CoordinatorURL = "http://127.0.0.1:8081"
	c.Worker.CoordinatorInsecureSkipVerify = true
	if problems := c.Validate(RoleWorker); len(problems) > 0 {
		t.Errorf("loopback worker rejected: %v", problems)
	}
}

func TestLiteralSecretsOnlyInDevelopment(t *testing.T) {
	apply := func(env string) []apperr.Problem {
		l := load(t, Options{Environ: []string{
			"DATAGEN_APPLICATION__ENVIRONMENT=" + env,
			"DATAGEN_AUTH__BOOTSTRAP_ADMIN_TOKEN=literal",
		}})
		l.Config.Normalize(bigHost)
		return l.Config.Validate(RoleCoordinator)
	}
	if !hasProblem(apply(EnvProduction), "auth.bootstrap_admin_token", codeConfigSecret) {
		t.Error("literal secret accepted in production")
	}
	if hasProblem(apply(EnvDevelopment), "auth.bootstrap_admin_token", codeConfigSecret) {
		t.Error("literal secret rejected in development")
	}
}

func TestWorkerRoleIgnoresCoordinatorOnlySettings(t *testing.T) {
	c := valid(EnvDevelopment)
	c.Server.Listen = "nope"
	c.Coordinator.LeaseDuration = 1
	if problems := c.Validate(RoleWorker); len(problems) > 0 {
		t.Fatalf("worker rejected for coordinator settings: %v", problems)
	}
}

func TestAllProblemsReportedTogether(t *testing.T) {
	c := valid(EnvDevelopment)
	c.Logging.Level = "loud"
	c.Server.Listen = "nope"
	c.Generation.QueueCapacity = 0
	if got := len(c.Validate(RoleCoordinator)); got < 3 {
		t.Fatalf("got %d problems, want at least 3", got)
	}
}

func TestSizeParsingAndFormatting(t *testing.T) {
	good := map[string]Size{
		"0": 0, "1024": KiB, "1KiB": KiB, "64MiB": 64 * MiB, "2GiB": 2 * GiB, "1TiB": TiB, "5B": 5, " 3 MiB ": 3 * MiB,
	}
	for in, want := range good {
		got, err := ParseSize(in)
		if err != nil || got != want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "MiB", "-1", "1.5GiB", "9999999999TiB", "10XB"} {
		if _, err := ParseSize(in); err == nil {
			t.Errorf("ParseSize(%q) accepted", in)
		}
	}
	for s, want := range map[Size]string{0: "0", 7: "7", KiB: "1KiB", 64 * MiB: "64MiB", GiB + 1: "1073741825"} {
		if got := s.String(); got != want {
			t.Errorf("Size(%d).String() = %q, want %q", int64(s), got, want)
		}
	}
	for _, s := range []Size{0, 1, KiB, 3 * GiB, GiB + 1} {
		back, err := ParseSize(s.String())
		if err != nil || back != s {
			t.Errorf("round trip of %d: %d, %v", int64(s), back, err)
		}
	}
}

func TestIsLoopback(t *testing.T) {
	want := map[string]bool{
		"127.0.0.1:1": true, "localhost:1": true, "[::1]:1": true,
		"0.0.0.0:1": false, ":1": false, "10.0.0.1:1": false, "example.com:1": false, "junk": false,
	}
	for addr, w := range want {
		if got := IsLoopback(addr); got != w {
			t.Errorf("IsLoopback(%q) = %v", addr, got)
		}
	}
}
