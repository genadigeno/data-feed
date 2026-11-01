package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/security"
)

const (
	envPrefix      = "DATAFEED_"
	envConfigFile  = "DATAFEED_CONFIG"
	envDataDir     = "DATAFEED_DATA_DIR"
	defaultFile    = "datafeed.yaml"
	codeConfigFile = "ERR_CONFIG_FILE"
)

// envReserved lists DATAFEED_* variables that are not settings paths.
var envReserved = map[string]bool{
	envConfigFile:     true,
	envDataDir:        true,
	"DATAFEED_SERVER": true,
	"DATAFEED_TOKEN":  true,
}

// Role selects which settings matter to the running process.
type Role int

const (
	RoleCoordinator Role = iota
	RoleWorker
)

// Options are the inputs to Load. Everything external is injected so loading is
// a pure function of its arguments.
type Options struct {
	Path     string            // explicit --config value
	Environ  []string          // os.Environ() form
	Flags    map[string]string // settings path → value, only flags the user set
	Role     Role
	ReadFile func(string) ([]byte, error)
}

// Loaded is a merged configuration and where each value came from.
type Loaded struct {
	Config  Config
	File    string            // config file used, "" if none
	Sources map[string]string // settings path → "file", "env:NAME" or "flag"
}

// Entries lists every setting with its value and source.
func (l *Loaded) Entries() []Entry {
	entries := l.Config.Flatten()
	for i := range entries {
		entries[i].Source = "default"
		if s, ok := l.Sources[entries[i].Path]; ok {
			entries[i].Source = s
		}
	}
	return entries
}

// Load merges defaults < profile < file < environment < flags and resolves the
// secrets relevant to the role. It does not validate limits; see Validate.
func Load(o Options) (*Loaded, error) {
	if o.ReadFile == nil {
		o.ReadFile = os.ReadFile
	}
	env := environMap(o.Environ)

	path, explicit := o.Path, o.Path != ""
	if !explicit {
		if p, ok := env[envConfigFile]; ok && p != "" {
			path, explicit = p, true
		} else {
			path = defaultFile
		}
	}

	data, err := o.ReadFile(path)
	switch {
	case err == nil:
	case errors.Is(err, os.ErrNotExist) && !explicit:
		path, data = "", nil
	default:
		return nil, apperr.Validation(codeConfigFile, "cannot read configuration file", []apperr.Problem{
			{Path: path, Code: codeConfigFile, Message: err.Error()},
		})
	}

	fileKeys := map[string]any{}
	if len(data) > 0 {
		if err := yaml.Unmarshal(data, &fileKeys); err != nil {
			return nil, fileProblem(path, err)
		}
	}

	environment, err := pickEnvironment(fileKeys, env, o.Flags)
	if err != nil {
		return nil, fileProblem(path, err)
	}
	cfg := Defaults(environment)

	if len(data) > 0 {
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
			return nil, fileProblem(path, err)
		}
	}

	loaded := &Loaded{File: path, Sources: map[string]string{}}
	for _, e := range cfg.Flatten() {
		if hasPath(fileKeys, e.Path) {
			loaded.Sources[e.Path] = "file"
		}
	}

	var problems []apperr.Problem
	apply := func(settingPath, raw, source string) {
		if err := cfg.SetPath(settingPath, raw); err != nil {
			problems = append(problems, apperr.Problem{Path: source, Code: "ERR_CONFIG_VALUE", Message: err.Error()})
			return
		}
		loaded.Sources[settingPath] = source
	}

	if v, ok := env[envDataDir]; ok {
		apply("storage.data_dir", v, "env:"+envDataDir)
	}
	for name, raw := range env {
		if !strings.HasPrefix(name, envPrefix) || envReserved[name] {
			continue
		}
		apply(envToPath(name), raw, "env:"+name)
	}
	for settingPath, raw := range o.Flags {
		apply(settingPath, raw, "flag")
	}

	problems = append(problems, resolveSecrets(&cfg, o, env)...)
	if len(problems) > 0 {
		return nil, apperr.Validation("ERR_CONFIG_VALUE", "invalid configuration", problems)
	}
	loaded.Config = cfg
	return loaded, nil
}

func fileProblem(path string, err error) error {
	return apperr.Validation(codeConfigFile, "invalid configuration file", []apperr.Problem{
		{Path: path, Code: codeConfigFile, Message: err.Error()},
	})
}

// pickEnvironment finds application.environment first: it selects the defaults
// the other sources are layered on.
func pickEnvironment(fileKeys map[string]any, env, flags map[string]string) (string, error) {
	const path = "application.environment"
	environment := EnvDevelopment
	if app, ok := fileKeys["application"].(map[string]any); ok {
		if v, ok := app["environment"]; ok {
			s, ok := v.(string)
			if !ok {
				return "", fmt.Errorf("%s must be a string", path)
			}
			environment = s
		}
	}
	if v, ok := env[envPrefix+"APPLICATION__ENVIRONMENT"]; ok {
		environment = v
	}
	if v, ok := flags[path]; ok {
		environment = v
	}
	return environment, nil
}

func resolveSecrets(cfg *Config, o Options, env map[string]string) []apperr.Problem {
	getenv := func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	}
	type secretField struct {
		path string
		s    *security.Secret
	}
	var fields []secretField
	switch o.Role {
	case RoleCoordinator:
		fields = append(fields, secretField{"auth.bootstrap_admin_token", &cfg.Auth.BootstrapAdminToken})
		if cfg.AI.Enabled {
			fields = append(fields, secretField{"ai.api_key", &cfg.AI.APIKey})
		}
	case RoleWorker:
		fields = append(fields, secretField{"worker.join_token", &cfg.Worker.JoinToken})
	}

	var problems []apperr.Problem
	for _, f := range fields {
		resolved, err := f.s.Resolve(getenv, o.ReadFile)
		if err != nil {
			problems = append(problems, apperr.Problem{Path: f.path, Code: codeConfigSecret, Message: err.Error()})
			continue
		}
		*f.s = resolved
	}
	return problems
}

func environMap(environ []string) map[string]string {
	m := make(map[string]string, len(environ))
	for _, kv := range environ {
		if k, v, ok := strings.Cut(kv, "="); ok {
			m[k] = v
		}
	}
	return m
}

// envToPath maps DATAFEED_GENERATION__QUEUE_CAPACITY to generation.queue_capacity.
func envToPath(name string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(name, envPrefix), "__", "."))
}

func hasPath(m map[string]any, path string) bool {
	head, rest, nested := strings.Cut(path, ".")
	v, ok := m[head]
	if !ok {
		return false
	}
	if !nested {
		return true
	}
	sub, ok := v.(map[string]any)
	return ok && hasPath(sub, rest)
}
