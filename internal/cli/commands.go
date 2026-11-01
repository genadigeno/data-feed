package cli

import (
	"cmp"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/genadigeno/data-feed/internal/apiclient"
	"github.com/genadigeno/data-feed/internal/app"
	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/buildinfo"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/protocol"
)

func newServeCommand(g *globals) *cobra.Command {
	var listen, workerListen string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the Coordinator: public API, worker protocol, SQLite and artifact storage",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			flags := map[string]string{}
			if listen != "" {
				flags["server.listen"] = listen
			}
			if workerListen != "" {
				flags["coordination.listen"] = workerListen
			}
			loaded, err := g.loadConfig(config.RoleCoordinator, flags)
			if err != nil {
				return err
			}
			logger, err := g.newLogger(&loaded.Config)
			if err != nil {
				return usageError(err)
			}
			return app.RunCoordinator(cmd.Context(), &loaded.Config, logger, clock.Real())
		},
	}
	cmd.Flags().StringVar(&listen, "listen", "", "public API address (server.listen)")
	cmd.Flags().StringVar(&workerListen, "worker-listen", "", "worker protocol address (coordination.listen)")
	return cmd
}

type versionOutput struct {
	Version         string `json:"version"`
	Commit          string `json:"commit"`
	GoVersion       string `json:"go_version"`
	EngineVersion   int    `json:"engine_version"`
	ProtocolVersion int    `json:"protocol_version"`
}

func newVersionCommand(g *globals) *cobra.Command {
	var short bool
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(*cobra.Command, []string) error {
			return g.printVersion(short)
		},
	}
	cmd.Flags().BoolVar(&short, "short", false, "print only the version number")
	return cmd
}

func (g *globals) printVersion(short bool) error {
	if short {
		fmt.Fprintln(g.stdout, buildinfo.Version)
		return nil
	}
	v := versionOutput{
		Version:         buildinfo.Version,
		Commit:          buildinfo.Commit,
		GoVersion:       buildinfo.GoVersion(),
		EngineVersion:   gen.Version,
		ProtocolVersion: protocol.Version,
	}
	if g.output == outputJSON {
		return writeJSON(g.stdout, v)
	}
	fmt.Fprintf(g.stdout, "datafeed %s (commit %s, %s, engine %d, protocol %d)\n",
		v.Version, v.Commit, v.GoVersion, v.EngineVersion, v.ProtocolVersion)
	return nil
}

func newHealthCommand(g *globals) *cobra.Command {
	var server string
	var ready bool
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Check a running Coordinator (exit code 6 if unavailable or not ready)",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			base, err := g.serverURL(server)
			if err != nil {
				return err
			}
			client := apiclient.New(base, timeout)
			if !ready {
				h, err := client.Health(cmd.Context())
				if err != nil {
					return err
				}
				if g.output == outputJSON {
					return writeJSON(g.stdout, h)
				}
				fmt.Fprintf(g.stdout, "%s (version %s)\n", h.Status, h.Version)
				return nil
			}

			r, err := client.Ready(cmd.Context())
			if err != nil {
				return err
			}
			if g.output == outputJSON {
				if err := writeJSON(g.stdout, r); err != nil {
					return err
				}
			} else {
				for _, c := range r.Checks {
					state := "ok"
					if !c.OK {
						state = "FAILED"
					}
					fmt.Fprintf(g.stdout, "%-10s %-7s %s\n", c.Name, state, c.Message)
				}
			}
			if !r.Ready {
				return apperr.Wrap(errUnhealthy, apperr.Unavailable, "ERR_NOT_READY", "coordinator is not ready")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&server, "server", "", "Coordinator URL (default: $DATAFEED_SERVER, then server.listen from the configuration)")
	cmd.Flags().BoolVar(&ready, "ready", false, "check readiness instead of liveness")
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Second, "request timeout")
	return cmd
}

// serverURL picks the Coordinator to talk to: flag, environment, then configuration.
func (g *globals) serverURL(flag string) (string, error) {
	if url := cmp.Or(flag, os.Getenv("DATAFEED_SERVER")); url != "" {
		return url, nil
	}
	loaded, err := g.loadConfig(config.RoleCoordinator, nil)
	if err != nil {
		return "", err
	}
	scheme := "http"
	if loaded.Config.Server.TLS.CertFile != "" {
		scheme = "https"
	}
	return scheme + "://" + loaded.Config.Server.Listen, nil
}

func newConfigCommand(g *globals) *cobra.Command {
	cmd := &cobra.Command{Use: "config", Short: "Validate and inspect configuration"}

	var role string
	validate := &cobra.Command{
		Use:   "validate [file]",
		Short: "Validate a configuration; all problems are reported together",
		Args:  usageArgs(cobra.MaximumNArgs(1)),
		RunE: func(_ *cobra.Command, args []string) error {
			r, err := parseRole(role)
			if err != nil {
				return err
			}
			if len(args) == 1 {
				g.configPath = args[0]
			}
			return g.validateAndReport(r)
		},
	}
	validate.Flags().StringVar(&role, "role", "coordinator", "validate for this role: coordinator or worker")

	var showRole string
	show := &cobra.Command{
		Use:   "show",
		Short: "Print the effective configuration and each value's source; secrets are redacted",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(*cobra.Command, []string) error {
			r, err := parseRole(showRole)
			if err != nil {
				return err
			}
			loaded, err := g.loadConfig(r, nil)
			if err != nil {
				return err
			}
			entries := loaded.Entries()
			if g.output == outputJSON {
				return writeJSON(g.stdout, entries)
			}
			w := tabwriter.NewWriter(g.stdout, 0, 0, 2, ' ', 0)
			for _, e := range entries {
				fmt.Fprintf(w, "%s\t%s\t%s\n", e.Path, e.Value, e.Source)
			}
			return w.Flush()
		},
	}
	show.Flags().StringVar(&showRole, "role", "coordinator", "resolve for this role: coordinator or worker")

	cmd.AddCommand(validate, show)
	return cmd
}

func parseRole(s string) (config.Role, error) {
	switch s {
	case "coordinator":
		return config.RoleCoordinator, nil
	case "worker":
		return config.RoleWorker, nil
	}
	return 0, usageError(fmt.Errorf("--role must be coordinator or worker, not %q", s))
}

func (g *globals) validateAndReport(role config.Role) error {
	if _, err := g.loadConfig(role, nil); err != nil {
		return err
	}
	if g.output == outputJSON {
		return writeJSON(g.stdout, map[string]bool{"valid": true})
	}
	fmt.Fprintln(g.stdout, "configuration is valid")
	return nil
}

func newWorkerCommand(g *globals) *cobra.Command {
	cmd := &cobra.Command{Use: "worker", Short: "Run and inspect Worker processes"}

	var coordinator, name, fingerprint, caFile string
	run := &cobra.Command{
		Use:   "run",
		Short: "Run a Worker in the foreground; it connects to a Coordinator and stops on a signal",
		Long: "Run a Worker in the foreground. It connects to a Coordinator over the worker protocol and\n" +
			"never needs the Coordinator's database or files.\n\n" +
			"This build verifies connectivity and protocol compatibility; task execution is not available yet.",
		Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			loaded, err := g.loadConfig(config.RoleWorker, workerFlags(coordinator, name, fingerprint, caFile))
			if err != nil {
				return err
			}
			logger, err := g.newLogger(&loaded.Config)
			if err != nil {
				return usageError(err)
			}
			return app.RunWorker(cmd.Context(), &loaded.Config, logger, clock.Real())
		},
	}
	f := run.Flags()
	f.StringVar(&coordinator, "coordinator", "", "Coordinator worker-protocol URL (worker.coordinator_url)")
	f.StringVar(&name, "name", "", "Worker name (worker.name, default: hostname)")
	f.StringVar(&fingerprint, "coordinator-fingerprint", "", "pin the Coordinator's certificate: sha256:… (worker.coordinator_fingerprint)")
	f.StringVar(&caFile, "coordinator-ca", "", "CA bundle that signed the Coordinator's certificate (worker.coordinator_ca_file)")

	var validateCoordinator string
	validate := &cobra.Command{
		Use:   "validate-config",
		Short: "Validate the configuration for the Worker role",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(*cobra.Command, []string) error {
			if _, err := g.loadConfig(config.RoleWorker, workerFlags(validateCoordinator, "", "", "")); err != nil {
				return err
			}
			if g.output == outputJSON {
				return writeJSON(g.stdout, map[string]bool{"valid": true})
			}
			fmt.Fprintln(g.stdout, "configuration is valid")
			return nil
		},
	}
	validate.Flags().StringVar(&validateCoordinator, "coordinator", "", "Coordinator worker-protocol URL (worker.coordinator_url)")

	var short bool
	version := &cobra.Command{
		Use:   "version",
		Short: "Print version information, including the protocol version Workers speak",
		Args:  usageArgs(cobra.NoArgs),
		RunE:  func(*cobra.Command, []string) error { return g.printVersion(short) },
	}
	version.Flags().BoolVar(&short, "short", false, "print only the version number")

	cmd.AddCommand(run, validate, version)
	return cmd
}

func workerFlags(coordinator, name, fingerprint, caFile string) map[string]string {
	flags := map[string]string{}
	for path, v := range map[string]string{
		"worker.coordinator_url":         coordinator,
		"worker.name":                    name,
		"worker.coordinator_fingerprint": fingerprint,
		"worker.coordinator_ca_file":     caFile,
	} {
		if v != "" {
			flags[path] = v
		}
	}
	return flags
}
