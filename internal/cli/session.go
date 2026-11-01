package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/genadigeno/data-feed/internal/api"
	"github.com/genadigeno/data-feed/internal/apiclient"
)

func oneID(cmd *cobra.Command) {
	cmd.Args = usageArgs(cobra.ExactArgs(1))
}

func newSessionCommand(g *globals) *cobra.Command {
	cmd := &cobra.Command{Use: "session", Short: "Create, run and inspect generation sessions"}
	cmd.AddCommand(
		sessionCreate(g), sessionRun(g), sessionList(g), sessionInspect(g), sessionConfigure(g), sessionValidate(g),
		sessionAction(g, "start", "Start a session"), sessionAction(g, "pause", "Pause a session: running units give back at their next part"),
		sessionAction(g, "resume", "Resume a paused session"), sessionAction(g, "clone", "New session with the same specification and a new seed"),
		sessionAction(g, "retry", "New session from the same snapshot, which reproduces the same bytes"),
		sessionStop(g), sessionWatch(g), sessionLogs(g), sessionMetrics(g), sessionArtifacts(g), sessionDownload(g), sessionDelete(g), sessionPurge(g),
	)
	return cmd
}

// specFlags are the ways of describing a session on the command line.
type specFlags struct {
	file, schema, name, mode, format, catalog, duration string
	count                                               uint64
	rate                                                float64
	seed                                                uint64
	unitRecords                                         int64
}

func (s *specFlags) add(cmd *cobra.Command) {
	f := cmd.Flags()
	f.StringVarP(&s.file, "file", "f", "", "session specification: a YAML or JSON file, or - for standard input")
	f.StringVar(&s.schema, "schema", "", "instead of -f: a schema file, or a saved schema's ID")
	f.StringVar(&s.name, "name", "", "session name (default: the schema's)")
	f.StringVar(&s.mode, "mode", "batch", "batch or stream")
	f.StringVar(&s.format, "format", "ndjson", "json, ndjson, xml or csv")
	f.StringVar(&s.catalog, "catalog", "", "catalog ID, or builtin:<domain>")
	f.Uint64Var(&s.count, "count", 0, "records to generate")
	f.Float64Var(&s.rate, "rate", 0, "records per second, across the whole session")
	f.StringVar(&s.duration, "duration", "", "stream mode: stop after this long, such as 10m")
	f.Uint64Var(&s.seed, "seed", 0, "random seed; default: chosen and reported")
	f.Int64Var(&s.unitRecords, "unit-records", 0, "records per work unit")
}

func (s *specFlags) build(cmd *cobra.Command) ([]byte, error) {
	if s.file != "" {
		return readSpecFile(s.file)
	}
	var doc json.RawMessage
	var ref string
	if _, err := os.Stat(s.schema); err == nil {
		data, err := os.ReadFile(s.schema)
		if err != nil {
			return nil, usageError(err)
		}
		// Either form travels as text: YAML keeps its field order that way.
		if doc, err = json.Marshal(string(data)); err != nil {
			return nil, err
		}
	} else {
		if err := checkSchemaRef(s.schema); err != nil {
			return nil, err
		}
		ref = s.schema
	}
	name := s.name
	if name == "" {
		name = strings.TrimSuffix(strings.TrimSuffix(baseName(s.schema), ".yaml"), ".json")
	}
	var seed *uint64
	if cmd.Flags().Changed("seed") {
		seed = &s.seed
	}
	return BuildSpec(name, s.mode, s.format, doc, ref, s.catalog, s.count, s.rate, s.duration, seed, s.unitRecords)
}

// checkSchemaRef explains a --schema that is neither a file here nor a saved schema's ID,
// which would otherwise reach the server and come back as an unknown schema.
func checkSchemaRef(ref string) error {
	if ref == "" || strings.HasPrefix(ref, "sch_") {
		return nil
	}
	return usageError(fmt.Errorf("no file %s in this folder, and it isn't a saved schema's ID, which starts with sch_", ref))
}

func baseName(path string) string {
	if i := strings.LastIndexAny(path, `/\`); i >= 0 {
		return path[i+1:]
	}
	return path
}

func sessionCreate(g *globals) *cobra.Command {
	var spec specFlags
	var start bool
	var key string
	cmd := &cobra.Command{
		Use: "create", Short: "Create a session", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			body, err := spec.build(cmd)
			if err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				s, err := c.CreateSession(ctx, body, start, key)
				if err != nil {
					return err
				}
				return g.showSession(&s)
			})
		},
	}
	spec.add(cmd)
	cmd.Flags().BoolVar(&start, "start", false, "also start it")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "a repeat with this key returns the first result")
	return cmd
}

func sessionRun(g *globals) *cobra.Command {
	var spec specFlags
	cmd := &cobra.Command{
		Use: "run", Short: "Create and start a session, and follow it to the end", Args: usageArgs(cobra.NoArgs),
		Long: "Create and start a session, and follow it to the end. With --local this is all you need: a\n" +
			"Coordinator and an embedded worker run just for this command, and the output stays in --data-dir.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			body, err := spec.build(cmd)
			if err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				s, err := c.CreateSession(ctx, body, true, "")
				if err != nil {
					return err
				}
				if g.output != outputJSON {
					fmt.Fprintf(g.stderr, "session %s (seed %s)\n", s.ID, s.Seed)
				}
				final, err := g.watchSession(ctx, c, s.ID)
				if err != nil {
					return err
				}
				if ctx.Err() != nil {
					fmt.Fprintf(g.stderr, "stopped following %s; it carries on\n", s.ID)
					return nil
				}
				return sessionFailed(&final)
			})
		},
	}
	spec.add(cmd)
	return cmd
}

func sessionList(g *globals) *cobra.Command {
	var state, mode, query, sort, cursor string
	var limit int
	cmd := &cobra.Command{
		Use: "list", Short: "List sessions", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				page, err := c.ListSessions(ctx, strings.ToUpper(state), mode, query, sort, apiclient.ListOptions{Limit: limit, Cursor: cursor})
				if err != nil {
					return err
				}
				return g.show(page, func(w io.Writer) {
					g.sessionTable(page.Items)(w)
					if page.NextCursor != "" {
						fmt.Fprintf(g.stderr, "more: --cursor %s\n", page.NextCursor)
					}
				})
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&state, "state", "", "only these states, comma separated")
	f.StringVar(&mode, "mode", "", "only batch or stream")
	f.StringVar(&query, "query", "", "name contains")
	f.StringVar(&sort, "sort", "", "created_at or -created_at (default)")
	f.IntVar(&limit, "limit", 0, "page size")
	f.StringVar(&cursor, "cursor", "", "continue from a previous listing")
	return cmd
}

func sessionInspect(g *globals) *cobra.Command {
	var spec bool
	cmd := &cobra.Command{
		Use: "inspect <id>", Short: "Show a session",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				s, err := c.GetSession(ctx, args[0], spec)
				if err != nil {
					return err
				}
				return g.showSession(&s)
			})
		},
	}
	oneID(cmd)
	cmd.Flags().BoolVar(&spec, "spec", false, "include the specification, with -o json")
	return cmd
}

func sessionConfigure(g *globals) *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use: "configure <id> -f file", Short: "Replace the specification of a session that has not started",
		RunE: func(cmd *cobra.Command, args []string) error {
			body, err := readSpecFile(file)
			if err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				s, err := c.ConfigureSession(ctx, args[0], body)
				if err != nil {
					return err
				}
				return g.showSession(&s)
			})
		},
	}
	oneID(cmd)
	cmd.Flags().StringVarP(&file, "file", "f", "", "session specification")
	return cmd
}

func sessionValidate(g *globals) *cobra.Command {
	var spec specFlags
	cmd := &cobra.Command{
		Use: "validate", Short: "Check a specification without creating anything", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			body, err := spec.build(cmd)
			if err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				out, err := c.ValidateSession(ctx, body)
				if err != nil {
					return err
				}
				return g.show(out, func(w io.Writer) { fmt.Fprintf(w, "valid: a %v session\n", out["strategy"]) })
			})
		},
	}
	spec.add(cmd)
	return cmd
}

func sessionAction(g *globals, action, short string) *cobra.Command {
	cmd := &cobra.Command{
		Use: action + " <id>", Short: short,
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				s, err := c.SessionAction(ctx, args[0], action)
				if err != nil {
					return err
				}
				return g.show(s, func(w io.Writer) { fmt.Fprintf(w, "%s: %s\n", s.ID, s.State) })
			})
		},
	}
	oneID(cmd)
	return cmd
}

func sessionStop(g *globals) *cobra.Command {
	var immediate bool
	cmd := &cobra.Command{
		Use: "stop <id>", Short: "Stop a session; output so far stays",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				s, err := c.StopSession(ctx, args[0], immediate)
				if err != nil {
					return err
				}
				return g.show(s, func(w io.Writer) { fmt.Fprintf(w, "%s: %s\n", s.ID, s.State) })
			})
		},
	}
	oneID(cmd)
	cmd.Flags().BoolVar(&immediate, "immediate", false, "discard what is in progress instead of finishing the current parts")
	return cmd
}

func sessionWatch(g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use: "watch <id>", Short: "Follow a session until it ends (exit code 1 if it fails)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				s, err := g.watchSession(ctx, c, args[0])
				if err != nil {
					return err
				}
				return sessionFailed(&s)
			})
		},
	}
	oneID(cmd)
	return cmd
}

func sessionLogs(g *globals) *cobra.Command {
	var level, since string
	var limit int
	var follow, events bool
	cmd := &cobra.Command{
		Use: "logs <id>", Short: "Show a session's log, or with --events its lifecycle history",
		RunE: func(cmd *cobra.Command, args []string) error {
			var from time.Time
			if since != "" {
				d, err := time.ParseDuration(since)
				if err != nil {
					return usageError(fmt.Errorf("--since: %w", err))
				}
				from = time.Now().Add(-d)
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if events {
					rows, err := c.SessionEvents(ctx, args[0])
					if err != nil {
						return err
					}
					return g.show(rows, func(w io.Writer) {
						table := make([][]string, len(rows))
						for i := range rows {
							e := &rows[i]
							table[i] = []string{HumanTime(&e.At), e.From, e.To, e.Event, e.Actor, e.Reason}
						}
						tabulate(w, []string{"TIME", "FROM", "TO", "EVENT", "ACTOR", "REASON"}, table)
					})
				}
				for {
					entries, err := c.SessionLogs(ctx, args[0], level, from, limit)
					if err != nil {
						return err
					}
					for i := range entries {
						e := &entries[i]
						if g.output == outputJSON {
							if err := writeCompactJSON(g.stdout, e); err != nil {
								return err
							}
						} else {
							fmt.Fprintf(g.stdout, "%s %-5s %s%s\n", e.Time.Local().Format("15:04:05.000"), e.Level, e.Message, attrText(e.Attrs))
						}
						from = e.Time.Add(time.Nanosecond)
					}
					if !follow {
						return nil
					}
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(time.Second):
					}
				}
			})
		},
	}
	oneID(cmd)
	f := cmd.Flags()
	f.StringVar(&level, "level", "", "debug, info, warn or error, and above")
	f.StringVar(&since, "since", "", "only this far back, such as 10m")
	f.IntVar(&limit, "limit", 0, "the newest lines, up to this many")
	f.BoolVar(&follow, "follow", false, "keep printing new lines")
	f.BoolVar(&events, "events", false, "show lifecycle history instead")
	return cmd
}

func attrText(attrs map[string]any) string {
	if len(attrs) == 0 {
		return ""
	}
	var b strings.Builder
	for _, k := range sortedStrings(attrs) {
		fmt.Fprintf(&b, " %s=%v", k, attrs[k])
	}
	return b.String()
}

func sortedStrings[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func sessionMetrics(g *globals) *cobra.Command {
	var history string
	cmd := &cobra.Command{
		Use: "metrics <id>", Short: "Show a session's current numbers and throughput history",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				p, err := c.SessionProgress(ctx, args[0])
				if err != nil {
					return err
				}
				m, err := c.SessionMetrics(ctx, args[0], history)
				if err != nil {
					return err
				}
				return g.show(map[string]any{"progress": p, "history": m}, func(w io.Writer) {
					fields(w, "State", p.State, "Records", HumanCount(p.Progress.Records), "Bytes", HumanBytes(p.Progress.Bytes),
						"Rate", fmt.Sprintf("%.0f/s now, %.0f/s average, %.0f/s peak", p.Rates.Current, p.Rates.Average, p.Rates.Peak), "Limited by", Bottleneck(p.Pressure))
					if len(m.Samples) == 0 {
						return
					}
					fmt.Fprintln(w)
					table := make([][]string, len(m.Samples))
					for i := range m.Samples {
						sample := &m.Samples[i]
						table[i] = []string{HumanTime(&sample.At), HumanCount(sample.Records), HumanBytes(sample.Bytes)}
					}
					tabulate(w, []string{"BUCKET", "RECORDS", "BYTES"}, table)
				})
			})
		},
	}
	oneID(cmd)
	cmd.Flags().StringVar(&history, "history", "", "how far back, such as 1h")
	return cmd
}

func sessionArtifacts(g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use: "artifacts <id>", Short: "List a session's output",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				a, err := c.SessionArtifacts(ctx, args[0])
				if err != nil {
					return err
				}
				return g.show(a, func(w io.Writer) {
					rows := make([][]string, len(a.Items))
					for i := range a.Items {
						item := &a.Items[i]
						rows[i] = []string{item.Stream, item.Status, item.Format, strconv.Itoa(len(item.Parts)), HumanCount(item.Records), HumanBytes(item.Bytes)}
					}
					tabulate(w, []string{"STREAM", "STATUS", "FORMAT", "PARTS", "RECORDS", "BYTES"}, rows)
				})
			})
		},
	}
	oneID(cmd)
	return cmd
}

// ChooseStream picks what to download: the one named, or the only one there is.
func ChooseStream(items []api.ArtifactDTO, named string) (string, error) {
	if named != "" {
		return named, nil
	}
	switch len(items) {
	case 0:
		return "", errors.New("this session has no output yet")
	case 1:
		return items[0].Stream, nil
	}
	names := make([]string, len(items))
	for i := range items {
		names[i] = items[i].Stream
	}
	return "", usageError(fmt.Errorf("choose one with --stream: %s", strings.Join(names, ", ")))
}

func sessionDownload(g *globals) *cobra.Command {
	var stream, out string
	var part int
	cmd := &cobra.Command{
		Use: "download <id>", Short: "Download output: a stream as one document, or one part",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				a, err := c.SessionArtifacts(ctx, args[0])
				if err != nil {
					return err
				}
				chosen, err := ChooseStream(a.Items, stream)
				if err != nil {
					return err
				}
				var body io.ReadCloser
				if cmd.Flags().Changed("part") {
					body, err = c.DownloadPart(ctx, args[0], chosen, part)
				} else {
					body, err = c.DownloadMerged(ctx, args[0], chosen)
				}
				if err != nil {
					return err
				}
				return g.copyOut(body, out)
			})
		},
	}
	oneID(cmd)
	f := cmd.Flags()
	f.StringVar(&stream, "stream", "", "which stream, such as table:orders; needed if there are several")
	f.IntVar(&part, "part", 0, "just this part, counting from 0")
	f.StringVarP(&out, "out", "O", "-", "where to write: a file, or - for standard output")
	return cmd
}

func sessionDelete(g *globals) *cobra.Command {
	var purge bool
	cmd := &cobra.Command{
		Use: "delete <id>", Short: "Delete a session that is not running; its files stay on disk unless purged",
		RunE: func(cmd *cobra.Command, args []string) error {
			question := "delete session " + args[0]
			if purge {
				question += " and its output"
			}
			if err := g.confirm(question); err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if err := c.DeleteSession(ctx, args[0], purge); err != nil {
					return err
				}
				return g.show(map[string]any{"deleted": args[0]}, func(w io.Writer) { fmt.Fprintf(w, "deleted %s\n", args[0]) })
			})
		},
	}
	oneID(cmd)
	cmd.Flags().BoolVar(&purge, "purge-artifacts", false, "also delete its output (needs the admin role)")
	return cmd
}

func sessionPurge(g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use: "purge <id>", Short: "Delete a finished session's output, keeping its history",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := g.confirm("delete the output of session " + args[0]); err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if err := c.PurgeArtifacts(ctx, args[0]); err != nil {
					return err
				}
				return g.show(map[string]any{"purged": args[0]}, func(w io.Writer) { fmt.Fprintf(w, "purged %s\n", args[0]) })
			})
		},
	}
	oneID(cmd)
	return cmd
}

func newDashboardCommand(g *globals) *cobra.Command {
	return &cobra.Command{
		Use: "dashboard", Short: "Show an overview of sessions and Workers", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				d, err := c.Dashboard(ctx)
				if err != nil {
					return err
				}
				return g.show(d, func(w io.Writer) {
					fields(w, "Sessions", countsText(d.Sessions), "Workers", countsText(d.Workers), "Records", HumanCount(d.RecordsCommitted),
						"Bytes", HumanBytes(d.BytesCommitted), "Rate now", fmt.Sprintf("%.0f/s", d.CurrentRate))
					if len(d.Recent) > 0 {
						fmt.Fprintln(w)
						g.sessionTable(d.Recent)(w)
					}
				})
			})
		},
	}
}

func countsText[V int | int64](m map[string]V) string {
	if len(m) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(m))
	for _, k := range sortedStrings(m) {
		parts = append(parts, fmt.Sprintf("%d %s", m[k], strings.ToLower(k)))
	}
	return strings.Join(parts, ", ")
}
