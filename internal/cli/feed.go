package cli

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/genadigeno/data-feed/internal/api"
	"github.com/genadigeno/data-feed/internal/apiclient"
)

func newFeedCommand(g *globals) *cobra.Command {
	cmd := &cobra.Command{Use: "feed", Short: "Freeze data as feeds that applications read from over the API"}
	cmd.AddCommand(feedCreate(g), feedList(g), feedInspect(g), feedDelete(g), feedRead(g), feedStream(g))
	return cmd
}

func showFeed(g *globals, f *api.FeedDTO) error {
	return g.show(f, func(w io.Writer) {
		streams := make([]string, len(f.Streams))
		for i, st := range f.Streams {
			streams[i] = st.ID + " (no end)"
			if st.End != "" {
				streams[i] = st.ID + " (ends at " + st.End + ")"
			}
		}
		fields(w, "ID", f.ID, "Name", f.Name, "Strategy", f.Strategy, "Format", f.Format, "Seed", f.Seed,
			"Reference time", f.ReferenceTime.Format("2006-01-02T15:04:05Z07:00"), "Streams", strings.Join(streams, ", "))
	})
}

func feedCreate(g *globals) *cobra.Command {
	spec := specFlags{mode: "stream"}
	cmd := &cobra.Command{
		Use: "create", Short: "Freeze a schema, seed and format as a feed", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			body, err := spec.build(cmd)
			if err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				f, err := c.CreateFeed(ctx, body)
				if err != nil {
					return err
				}
				return showFeed(g, &f)
			})
		},
	}
	f := cmd.Flags()
	f.StringVarP(&spec.file, "file", "f", "", "feed specification: a YAML or JSON file, or - for standard input")
	f.StringVar(&spec.schema, "schema", "", "instead of -f: a schema file, or a saved schema's ID")
	f.StringVar(&spec.name, "name", "", "feed name (default: the schema's)")
	f.StringVar(&spec.format, "format", "ndjson", "json, ndjson, xml or csv; streaming needs ndjson or csv")
	f.StringVar(&spec.catalog, "catalog", "", "catalog ID, or builtin:<domain>")
	f.Uint64Var(&spec.count, "count", 0, "records in the feed; default no end")
	f.Float64Var(&spec.rate, "rate", 0, "default records per second for streams")
	f.Uint64Var(&spec.seed, "seed", 0, "random seed; default: chosen and reported")
	return cmd
}

func feedList(g *globals) *cobra.Command {
	return &cobra.Command{
		Use: "list", Short: "List feeds", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				page, err := c.ListFeeds(ctx)
				if err != nil {
					return err
				}
				return g.show(page, func(w io.Writer) {
					rows := make([][]string, len(page.Items))
					for i := range page.Items {
						f := &page.Items[i]
						rows[i] = []string{f.ID, f.Name, f.Strategy, f.Format, f.Seed, HumanTime(&f.CreatedAt)}
					}
					tabulate(w, []string{"ID", "NAME", "STRATEGY", "FORMAT", "SEED", "CREATED"}, rows)
				})
			})
		},
	}
}

func feedInspect(g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use: "inspect <id>", Short: "Show a feed",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				f, err := c.GetFeed(ctx, args[0])
				if err != nil {
					return err
				}
				return showFeed(g, &f)
			})
		},
	}
	oneID(cmd)
	return cmd
}

func feedDelete(g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use: "delete <id>", Short: "Delete a feed",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := g.confirm("delete feed " + args[0]); err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if err := c.DeleteFeed(ctx, args[0]); err != nil {
					return err
				}
				return g.show(map[string]any{"deleted": args[0]}, func(w io.Writer) { fmt.Fprintf(w, "deleted %s\n", args[0]) })
			})
		},
	}
	oneID(cmd)
	return cmd
}

func feedRead(g *globals) *cobra.Command {
	var table, out string
	var offset, limit uint64
	cmd := &cobra.Command{
		Use: "read <id>", Short: "Read one page of a feed; the next offset goes to standard error",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				q := url.Values{"offset": {strconv.FormatUint(offset, 10)}}
				if limit > 0 {
					q.Set("limit", strconv.FormatUint(limit, 10))
				}
				if table != "" {
					q.Set("table", table)
				}
				page, err := c.FeedRecords(ctx, args[0], q)
				if err != nil {
					return err
				}
				if page.Next != "" {
					fmt.Fprintf(g.stderr, "next offset: %s\n", page.Next)
				}
				return g.copyOut(page, out)
			})
		},
	}
	oneID(cmd)
	f := cmd.Flags()
	f.Uint64Var(&offset, "offset", 0, "position to start from")
	f.Uint64Var(&limit, "limit", 0, "positions to cover; default 1000")
	f.StringVar(&table, "table", "", "which table, for a relational feed")
	f.StringVarP(&out, "out", "O", "-", "where to write: a file, or - for standard output")
	return cmd
}

func feedStream(g *globals) *cobra.Command {
	var table string
	var from, limit uint64
	var rate float64
	cmd := &cobra.Command{
		Use: "stream <id>", Short: "Follow a feed on standard output until it ends or Ctrl+C",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				q := url.Values{"from": {strconv.FormatUint(from, 10)}}
				if limit > 0 {
					q.Set("limit", strconv.FormatUint(limit, 10))
				}
				if cmd.Flags().Changed("rate") {
					q.Set("rate", strconv.FormatFloat(rate, 'f', -1, 64))
				}
				if table != "" {
					q.Set("table", table)
				}
				body, err := c.FeedStream(ctx, args[0], q)
				if err != nil {
					return err
				}
				return g.copyOut(body, "-")
			})
		},
	}
	oneID(cmd)
	f := cmd.Flags()
	f.Uint64Var(&from, "from", 0, "position to start from")
	f.Uint64Var(&limit, "limit", 0, "stop after this many records")
	f.Float64Var(&rate, "rate", 0, "records per second, 0 for no limit; default the feed's own")
	f.StringVar(&table, "table", "", "which table, for a relational feed")
	return cmd
}
