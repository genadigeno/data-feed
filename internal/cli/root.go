// Package cli implements the datafeed command line. Commands hold no application
// logic: they load configuration and call the same services every role uses.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/obs"
	"github.com/genadigeno/data-feed/internal/sysinfo"
)

const (
	outputTable = "table"
	outputJSON  = "json"
)

// globals holds the root command's flags.
type globals struct {
	configPath string
	dataDir    string
	output     string
	verbose    bool
	quiet      bool
	server     string
	token      string
	tokenFile  string
	local      bool
	yes        bool

	stdout, stderr io.Writer
}

// Execute runs the CLI and returns the process exit code.
func Execute(args []string, stdout, stderr io.Writer) int {
	// The first signal cancels ctx for a graceful stop. stop() then restores default
	// handling, so a second signal terminates immediately.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()

	g := &globals{stdout: stdout, stderr: stderr}
	root := newRootCommand(g)
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)

	err := root.ExecuteContext(ctx)
	if err == nil {
		return apperr.ExitOK
	}
	return g.report(err)
}

func newRootCommand(g *globals) *cobra.Command {
	root := &cobra.Command{
		Use:           "datafeed",
		Short:         "High-performance synthetic data generation platform",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(*cobra.Command, []string) error {
			if g.output != outputTable && g.output != outputJSON {
				return usageError(fmt.Errorf("--output must be %s or %s", outputTable, outputJSON))
			}
			if g.local && g.server != "" {
				return usageError(fmt.Errorf("--local and --server rule each other out"))
			}
			return nil
		},
	}
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return usageError(err) })

	f := root.PersistentFlags()
	f.StringVar(&g.configPath, "config", "", "configuration file (default: $DATAFEED_CONFIG, then ./datafeed.yaml if present)")
	f.StringVar(&g.dataDir, "data-dir", "", "data directory (default: $DATAFEED_DATA_DIR, then ./data)")
	f.StringVarP(&g.output, "output", "o", outputTable, "output format: table or json")
	f.BoolVarP(&g.verbose, "verbose", "v", false, "log at debug level")
	f.BoolVarP(&g.quiet, "quiet", "q", false, "log warnings and errors only")
	f.StringVar(&g.server, "server", "", "Coordinator URL (default: $DATAFEED_SERVER, then the configured listen address)")
	f.StringVar(&g.token, "token", "", "API token (default: $DATAFEED_TOKEN)")
	f.StringVar(&g.tokenFile, "token-file", "", "file holding the API token")
	f.BoolVar(&g.local, "local", false, "run a Coordinator on --data-dir just for this command")
	f.BoolVar(&g.yes, "yes", false, "answer yes to confirmations")

	root.AddCommand(
		newServeCommand(g),
		newGenerateCommand(g),
		newVersionCommand(g),
		newHealthCommand(g),
		newConfigCommand(g),
		newWorkerCommand(g),
		newSessionCommand(g),
		newSchemaCommand(g),
		newFeedCommand(g),
		newDomainCommand(g),
		newCatalogCommand(g),
		newJobCommand(g),
		newTokenCommand(g),
		newDashboardCommand(g),
		newAdminCommand(g),
	)
	return root
}

// usageError marks a mistake in the command line itself.
func usageError(err error) error {
	return apperr.Wrap(err, apperr.InvalidArgument, "ERR_USAGE", err.Error())
}

func usageArgs(check cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if err := check(cmd, args); err != nil {
			return usageError(err)
		}
		return nil
	}
}

// loadConfig merges all sources, resolves automatic settings and validates for role.
// flags maps settings paths to values for the flags the user actually set.
func (g *globals) loadConfig(role config.Role, flags map[string]string) (*config.Loaded, error) {
	if flags == nil {
		flags = map[string]string{}
	}
	if g.dataDir != "" {
		flags["storage.data_dir"] = g.dataDir
	}
	switch {
	case g.verbose:
		flags["logging.level"] = "debug"
	case g.quiet:
		flags["logging.level"] = "warn"
	}

	loaded, err := config.Load(config.Options{
		Path:    g.configPath,
		Environ: os.Environ(),
		Flags:   flags,
		Role:    role,
	})
	if err != nil {
		return nil, err
	}
	loaded.Config.Normalize(config.Facts{NumCPU: sysinfo.NumCPU(), MemoryBytes: sysinfo.MemoryBytes()})
	if problems := loaded.Config.Validate(role); len(problems) > 0 {
		return nil, apperr.Validation("ERR_CONFIG_VALUE", "invalid configuration", problems)
	}
	return loaded, nil
}

func (g *globals) newLogger(cfg *config.Config) (*slog.Logger, error) {
	return obs.NewLogger(g.stderr, obs.LogOptions{Level: cfg.Logging.Level, Format: cfg.Logging.Format})
}

// report prints err in the selected format and returns the exit code.
func (g *globals) report(err error) int {
	e, ok := apperr.As(err)
	if !ok {
		code := "ERR_INTERNAL"
		exit := apperr.ExitGeneric
		// cobra has no typed error for an unknown command.
		if strings.HasPrefix(err.Error(), "unknown command") {
			code, exit = "ERR_USAGE", apperr.ExitUsage
		}
		e = &apperr.Error{Kind: apperr.Internal, Code: code, Message: err.Error()}
		g.writeError(e)
		return exit
	}
	g.writeError(e)
	return apperr.ExitCode(e.Kind)
}

type errorOutput struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code     string           `json:"code"`
	Message  string           `json:"message"`
	Problems []apperr.Problem `json:"problems,omitempty"`
}

func (g *globals) writeError(e *apperr.Error) {
	message := e.Message
	if e.Cause != nil && !strings.Contains(message, e.Cause.Error()) {
		message += ": " + e.Cause.Error()
	}
	if g.output == outputJSON {
		_ = writeJSON(g.stderr, errorOutput{errorDetail{Code: e.Code, Message: message, Problems: e.Problems}})
		return
	}
	fmt.Fprintf(g.stderr, "error: %s\n", message)
	for _, p := range e.Problems {
		fmt.Fprintf(g.stderr, "  %s\n", p)
	}
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// errUnhealthy signals a negative answer that has already been printed.
var errUnhealthy = errors.New("not healthy")
