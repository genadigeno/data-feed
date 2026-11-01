package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/genadigeno/data-feed/internal/api"
	"github.com/genadigeno/data-feed/internal/apiclient"
	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/vocab"
)

// Schemas

func newSchemaCommand(g *globals) *cobra.Command {
	cmd := &cobra.Command{Use: "schema", Short: "Save and inspect schemas"}
	var name string
	create := &cobra.Command{
		Use: "create <file>", Short: "Save a schema",
		RunE: func(cmd *cobra.Command, args []string) error {
			doc, err := readSpecFile(args[0])
			if err != nil {
				return err
			}
			if name == "" {
				name = strings.TrimSuffix(strings.TrimSuffix(baseName(args[0]), ".yaml"), ".json")
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				s, err := c.CreateSchema(ctx, name, doc)
				if err != nil {
					return err
				}
				return g.show(s, func(w io.Writer) { fmt.Fprintf(w, "%s version %d\n", s.ID, s.Version) })
			})
		},
	}
	oneID(create)
	create.Flags().StringVar(&name, "name", "", "schema name (default: the file's)")

	update := &cobra.Command{
		Use: "update <id> <file>", Short: "Save a new version of a schema", Args: usageArgs(cobra.ExactArgs(2)),
		RunE: func(cmd *cobra.Command, args []string) error {
			doc, err := readSpecFile(args[1])
			if err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				s, err := c.UpdateSchema(ctx, args[0], doc)
				if err != nil {
					return err
				}
				return g.show(s, func(w io.Writer) { fmt.Fprintf(w, "%s version %d\n", s.ID, s.Version) })
			})
		},
	}
	validate := &cobra.Command{
		Use: "validate <file>", Short: "Check a schema",
		RunE: func(cmd *cobra.Command, args []string) error {
			doc, err := readSpecFile(args[0])
			if err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				out, err := c.ValidateSchema(ctx, doc)
				if err != nil {
					return err
				}
				return g.show(out, func(w io.Writer) { fmt.Fprintf(w, "valid: a %v schema\n", out["kind"]) })
			})
		},
	}
	oneID(validate)
	list := &cobra.Command{
		Use: "list", Short: "List schemas", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				page, err := c.ListSchemas(ctx, apiclient.ListOptions{})
				if err != nil {
					return err
				}
				return g.show(page, func(w io.Writer) {
					rows := make([][]string, len(page.Items))
					for i := range page.Items {
						s := &page.Items[i]
						rows[i] = []string{s.ID, s.Name, s.Kind, strconv.Itoa(s.Version), HumanTime(&s.CreatedAt)}
					}
					tabulate(w, []string{"ID", "NAME", "KIND", "VERSION", "CREATED"}, rows)
				})
			})
		},
	}
	var version int
	export := &cobra.Command{
		Use: "export <id>", Short: "Print a schema as JSON",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				s, err := c.GetSchema(ctx, args[0], version)
				if err != nil {
					return err
				}
				_, err = fmt.Fprintf(g.stdout, "%s\n", s.Definition)
				return err
			})
		},
	}
	oneID(export)
	export.Flags().IntVar(&version, "version", 0, "which version; default the latest")
	del := &cobra.Command{
		Use: "delete <id>", Short: "Delete a schema and its versions",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := g.confirm("delete schema " + args[0]); err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if err := c.DeleteSchema(ctx, args[0]); err != nil {
					return err
				}
				return g.show(map[string]any{"deleted": args[0]}, func(w io.Writer) { fmt.Fprintf(w, "deleted %s\n", args[0]) })
			})
		},
	}
	oneID(del)
	var count int
	var seed uint64
	var format, table, catalog string
	preview := &cobra.Command{
		Use: "preview <id>", Short: "Print sample records",
		RunE: func(cmd *cobra.Command, args []string) error {
			body := map[string]any{"count": count, "format": format, "table": table, "catalog": catalog}
			if cmd.Flags().Changed("seed") {
				body["seed"] = seed
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				out, err := c.PreviewSchema(ctx, args[0], body)
				if err != nil {
					return err
				}
				return g.copyOut(out, "-")
			})
		},
	}
	oneID(preview)
	pf := preview.Flags()
	pf.IntVar(&count, "count", 10, "how many records, up to 1000")
	pf.Uint64Var(&seed, "seed", 0, "random seed")
	pf.StringVar(&format, "format", "ndjson", "json, ndjson, xml or csv")
	pf.StringVar(&table, "table", "", "which table, for a relational schema")
	pf.StringVar(&catalog, "catalog", "", "catalog ID, or builtin:<domain>")
	cmd.AddCommand(create, update, validate, list, export, preview, del)
	return cmd
}

// Domains

func newDomainCommand(g *globals) *cobra.Command {
	cmd := &cobra.Command{Use: "domain", Short: "Manage business domains and fill catalogs for them"}
	var name, description, locale string
	create := &cobra.Command{
		Use: "create", Short: "Add a domain", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				d, err := c.CreateDomain(ctx, name, description, locale)
				if err != nil {
					return err
				}
				return g.show(d, func(w io.Writer) { fmt.Fprintln(w, d.ID) })
			})
		},
	}
	create.Flags().StringVar(&name, "name", "", "domain name")
	create.Flags().StringVar(&description, "description", "", "what the domain is about")
	create.Flags().StringVar(&locale, "locale", "", "language of its vocabulary, such as en")
	list := &cobra.Command{
		Use: "list", Short: "List domains", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				page, err := c.ListDomains(ctx, apiclient.ListOptions{})
				if err != nil {
					return err
				}
				return g.show(page, func(w io.Writer) {
					rows := make([][]string, len(page.Items))
					for i := range page.Items {
						d := &page.Items[i]
						rows[i] = []string{d.ID, d.Name, d.Locale, d.Description}
					}
					tabulate(w, []string{"ID", "NAME", "LOCALE", "DESCRIPTION"}, rows)
				})
			})
		},
	}
	del := &cobra.Command{
		Use: "delete <id>", Short: "Delete a domain with no catalogs",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := g.confirm("delete domain " + args[0]); err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if err := c.DeleteDomain(ctx, args[0]); err != nil {
					return err
				}
				return g.show(map[string]any{"deleted": args[0]}, func(w io.Writer) { fmt.Fprintf(w, "deleted %s\n", args[0]) })
			})
		},
	}
	oneID(del)

	var size, taxonomyModel, valuesModel, catalogID string
	var valuesPerGroup, maxGroups, version int
	var noCache, wait bool
	genVocab := &cobra.Command{
		Use: "generate-vocabulary <domain>", Short: "Start a job that fills a catalog draft",
		Long: "Start a job that fills a catalog draft. With the Anthropic provider this makes paid API calls: start with --size low.",
		RunE: func(cmd *cobra.Command, args []string) error {
			body := map[string]any{"size": strings.ToUpper(size)}
			set := func(k string, ok bool, v any) {
				if ok {
					body[k] = v
				}
			}
			set("values_per_group", valuesPerGroup > 0, valuesPerGroup)
			set("max_groups", maxGroups > 0, maxGroups)
			set("taxonomy_model", taxonomyModel != "", taxonomyModel)
			set("values_model", valuesModel != "", valuesModel)
			set("catalog_id", catalogID != "", catalogID)
			set("version", version > 0, version)
			set("no_cache", noCache, noCache)
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				job, err := c.StartVocabularyJob(ctx, args[0], body)
				if err != nil {
					return err
				}
				// With --local the job dies with this command, so it always waits there.
				if wait || g.local {
					if job, err = g.waitJob(ctx, c, job.ID); err != nil {
						return err
					}
				}
				return g.showJob(&job)
			})
		},
	}
	oneID(genVocab)
	gf := genVocab.Flags()
	gf.StringVar(&size, "size", "low", "low, medium, high or custom")
	gf.IntVar(&valuesPerGroup, "values-per-group", 0, "with --size custom")
	gf.IntVar(&maxGroups, "max-groups", 0, "with --size custom")
	gf.StringVar(&taxonomyModel, "taxonomy-model", "", "model for planning groups")
	gf.StringVar(&valuesModel, "values-model", "", "model for writing values")
	gf.StringVar(&catalogID, "catalog", "", "add to this catalog's draft instead of a new catalog")
	gf.IntVar(&version, "catalog-version", 0, "which draft version")
	gf.BoolVar(&noCache, "no-cache", false, "ask again even if an identical answer is stored")
	gf.BoolVar(&wait, "wait", false, "wait for the job to end")
	cmd.AddCommand(create, list, del, genVocab)
	return cmd
}

func (g *globals) showJob(j *api.JobDTO) error {
	return g.show(j, func(w io.Writer) {
		fields(w, "Job", j.ID, "State", j.State, "Catalog", fmt.Sprintf("%s version %d", j.CatalogID, j.Version), "Error", j.Error)
		var p struct {
			GroupsDone     int            `json:"groups_done"`
			GroupsPlanned  int            `json:"groups_planned"`
			ValuesAccepted int            `json:"values_accepted"`
			ValuesRejected int            `json:"values_rejected"`
			Reasons        map[string]int `json:"rejected_reasons"`
			Saturated      []string       `json:"saturated_groups"`
			Refused        []string       `json:"refused_groups"`
			Calls          int            `json:"calls"`
			Cached         int            `json:"cached_calls"`
			Usage          struct {
				Input  int64 `json:"input_tokens"`
				Output int64 `json:"output_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(j.Progress, &p) == nil && p.GroupsPlanned > 0 {
			fields(w, "Groups", fmt.Sprintf("%d of %d", p.GroupsDone, p.GroupsPlanned), "Values", fmt.Sprintf("%d accepted, %d rejected", p.ValuesAccepted, p.ValuesRejected),
				"Rejected for", countsText(p.Reasons), "Saturated", strings.Join(p.Saturated, ", "), "Refused", strings.Join(p.Refused, ", "),
				"Calls", fmt.Sprintf("%d, plus %d from cache", p.Calls, p.Cached), "Tokens", fmt.Sprintf("%d in, %d out", p.Usage.Input, p.Usage.Output))
		}
	})
}

func (g *globals) waitJob(ctx context.Context, c *apiclient.Client, id string) (api.JobDTO, error) {
	for {
		job, err := c.GetJob(ctx, id)
		if err != nil {
			return job, err
		}
		if job.State != "QUEUED" && job.State != "RUNNING" {
			return job, nil
		}
		select {
		case <-ctx.Done():
			return job, nil
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func newJobCommand(g *globals) *cobra.Command {
	cmd := &cobra.Command{Use: "job", Short: "Follow vocabulary jobs"}
	inspect := &cobra.Command{
		Use: "inspect <id>", Short: "Show a job",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				j, err := c.GetJob(ctx, args[0])
				if err != nil {
					return err
				}
				return g.showJob(&j)
			})
		},
	}
	oneID(inspect)
	wait := &cobra.Command{
		Use: "wait <id>", Short: "Wait for a job to end",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				j, err := g.waitJob(ctx, c, args[0])
				if err != nil {
					return err
				}
				return g.showJob(&j)
			})
		},
	}
	oneID(wait)
	cancel := &cobra.Command{
		Use: "cancel <id>", Short: "Cancel a job; what it made stays in the draft",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				j, err := c.CancelJob(ctx, args[0])
				if err != nil {
					return err
				}
				return g.showJob(&j)
			})
		},
	}
	oneID(cancel)
	cmd.AddCommand(inspect, wait, cancel)
	return cmd
}

// Catalogs

// CatalogDiff says how two versions differ.
type CatalogDiff struct {
	GroupsAdded   []string            `json:"groups_added,omitempty"`
	GroupsRemoved []string            `json:"groups_removed,omitempty"`
	ValuesAdded   map[string][]string `json:"values_added,omitempty"`
	ValuesRemoved map[string][]string `json:"values_removed,omitempty"`
}

// DiffCatalogs compares two versions' content.
func DiffCatalogs(from, to []vocab.Group) CatalogDiff {
	d := CatalogDiff{ValuesAdded: map[string][]string{}, ValuesRemoved: map[string][]string{}}
	values := func(g *vocab.Group) map[string]bool {
		out := make(map[string]bool, len(g.Values))
		for i := range g.Values {
			out[g.Values[i].Value] = true
		}
		return out
	}
	index := func(groups []vocab.Group) map[string]*vocab.Group {
		out := make(map[string]*vocab.Group, len(groups))
		for i := range groups {
			out[groups[i].Key] = &groups[i]
		}
		return out
	}
	old, cur := index(from), index(to)
	for key, g := range cur {
		o, ok := old[key]
		if !ok {
			d.GroupsAdded = append(d.GroupsAdded, key)
			continue
		}
		before, after := values(o), values(g)
		for v := range after {
			if !before[v] {
				d.ValuesAdded[key] = append(d.ValuesAdded[key], v)
			}
		}
		for v := range before {
			if !after[v] {
				d.ValuesRemoved[key] = append(d.ValuesRemoved[key], v)
			}
		}
		slices.Sort(d.ValuesAdded[key])
		slices.Sort(d.ValuesRemoved[key])
	}
	for key := range old {
		if _, ok := cur[key]; !ok {
			d.GroupsRemoved = append(d.GroupsRemoved, key)
		}
	}
	slices.Sort(d.GroupsAdded)
	slices.Sort(d.GroupsRemoved)
	for k, v := range d.ValuesAdded {
		if len(v) == 0 {
			delete(d.ValuesAdded, k)
		}
	}
	for k, v := range d.ValuesRemoved {
		if len(v) == 0 {
			delete(d.ValuesRemoved, k)
		}
	}
	return d
}

// ReadValues reads values to add: one per line, optionally followed by a tab and a weight.
func ReadValues(r io.Reader) ([]vocab.Entry, error) {
	var out []vocab.Entry
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 64<<10), 1<<20)
	for scan.Scan() {
		line := strings.TrimRight(scan.Text(), "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		value, weightText, hasWeight := strings.Cut(line, "\t")
		e := vocab.Entry{Value: value}
		if hasWeight {
			w, err := strconv.ParseFloat(strings.TrimSpace(weightText), 64)
			if err != nil {
				return nil, usageError(fmt.Errorf("line %q: the weight after the tab must be a number", line))
			}
			e.Weight = w
		}
		out = append(out, e)
	}
	return out, scan.Err()
}

func newCatalogCommand(g *globals) *cobra.Command {
	cmd := &cobra.Command{Use: "catalog", Short: "Manage vocabulary catalogs"}
	builtins := &cobra.Command{
		Use: "builtins", Short: "List the catalogs built into this binary", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				items, err := c.ListBuiltins(ctx)
				if err != nil {
					return err
				}
				return g.show(items, func(w io.Writer) {
					rows := make([][]string, len(items))
					for i := range items {
						b := &items[i]
						rows[i] = []string{b.ID, b.Name, strconv.Itoa(b.Groups), strconv.Itoa(b.Values), b.Description}
					}
					tabulate(w, []string{"ID", "NAME", "GROUPS", "VALUES", "DESCRIPTION"}, rows)
				})
			})
		},
	}
	var domain string
	list := &cobra.Command{
		Use: "list", Short: "List catalogs", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				page, err := c.ListCatalogs(ctx, domain, apiclient.ListOptions{})
				if err != nil {
					return err
				}
				return g.show(page, func(w io.Writer) {
					rows := make([][]string, len(page.Items))
					for i := range page.Items {
						item := &page.Items[i]
						rows[i] = []string{item.ID, strconv.Itoa(item.Version), item.Status, item.DomainID, HumanTime(&item.CreatedAt)}
					}
					tabulate(w, []string{"ID", "VERSION", "STATUS", "DOMAIN", "CREATED"}, rows)
				})
			})
		},
	}
	list.Flags().StringVar(&domain, "domain", "", "only this domain's")

	var createDomain string
	create := &cobra.Command{
		Use: "create", Short: "Start an empty draft", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				out, err := c.CreateCatalog(ctx, createDomain)
				if err != nil {
					return err
				}
				return g.show(out, func(w io.Writer) { fmt.Fprintf(w, "%s version %d (%s)\n", out.ID, out.Version, out.Status) })
			})
		},
	}
	create.Flags().StringVar(&createDomain, "domain", "", "the domain it belongs to")

	var version int
	addVersionFlag := func(c *cobra.Command) { c.Flags().IntVar(&version, "version", 0, "which version; default the latest") }
	inspect := &cobra.Command{
		Use: "inspect <id>", Short: "Show a catalog version's groups",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				out, err := c.GetCatalog(ctx, args[0], version)
				if err != nil {
					return err
				}
				return g.show(out, func(w io.Writer) {
					fields(w, "Catalog", out.Catalog.ID, "Version", strconv.Itoa(out.Catalog.Version), "Status", out.Catalog.Status, "Domain", out.Catalog.DomainID, "Hash", out.Catalog.ContentHash)
					fmt.Fprintln(w)
					rows := make([][]string, len(out.Groups))
					for i := range out.Groups {
						group := &out.Groups[i]
						rows[i] = []string{group.Key, group.DataType, strconv.Itoa(len(group.Values)), group.Category, group.Description}
					}
					tabulate(w, []string{"GROUP", "TYPE", "VALUES", "CATEGORY", "DESCRIPTION"}, rows)
				})
			})
		},
	}
	oneID(inspect)
	addVersionFlag(inspect)
	versions := &cobra.Command{
		Use: "versions <id>", Short: "List a catalog's versions",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				items, err := c.CatalogVersions(ctx, args[0])
				if err != nil {
					return err
				}
				return g.show(items, func(w io.Writer) {
					rows := make([][]string, len(items))
					for i := range items {
						item := &items[i]
						rows[i] = []string{strconv.Itoa(item.Version), item.Status, HumanTime(&item.CreatedAt), HumanTime(item.PublishedAt), item.ContentHash}
					}
					tabulate(w, []string{"VERSION", "STATUS", "CREATED", "PUBLISHED", "HASH"}, rows)
				})
			})
		},
	}
	oneID(versions)
	draftAction := func(use, short string, run func(context.Context, *apiclient.Client, string, int) (api.CatalogDTO, error)) *cobra.Command {
		var v int
		c := &cobra.Command{
			Use: use + " <id>", Short: short,
			RunE: func(cmd *cobra.Command, args []string) error {
				return g.runRemote(cmd, func(ctx context.Context, cl *apiclient.Client) error {
					out, err := run(ctx, cl, args[0], v)
					if err != nil {
						return err
					}
					return g.show(out, func(w io.Writer) { fmt.Fprintf(w, "%s version %d (%s)\n", out.ID, out.Version, out.Status) })
				})
			},
		}
		oneID(c)
		c.Flags().IntVar(&v, "version", 1, "which version")
		return c
	}
	publish := draftAction("publish", "Freeze a draft, so sessions can use it", func(ctx context.Context, c *apiclient.Client, id string, v int) (api.CatalogDTO, error) {
		return c.PublishCatalog(ctx, id, v)
	})
	newVersion := draftAction("new-version", "Start a draft from a version", func(ctx context.Context, c *apiclient.Client, id string, v int) (api.CatalogDTO, error) {
		return c.NewCatalogVersion(ctx, id, v)
	})
	var cloneDomain string
	clone := &cobra.Command{
		Use: "clone <id|builtin:domain>", Short: "Copy a version, or a built-in catalog, to a new draft",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				out, err := c.CloneCatalog(ctx, args[0], version, cloneDomain)
				if err != nil {
					return err
				}
				return g.show(out, func(w io.Writer) { fmt.Fprintf(w, "%s version %d (%s)\n", out.ID, out.Version, out.Status) })
			})
		},
	}
	oneID(clone)
	addVersionFlag(clone)
	clone.Flags().StringVar(&cloneDomain, "domain", "", "the domain of the copy")

	var exportVersion int
	var exportOut string
	export := &cobra.Command{
		Use: "export <id>", Short: "Write a version in the portable format",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				doc, err := c.ExportCatalog(ctx, args[0], exportVersion)
				if err != nil {
					return err
				}
				data, err := json.MarshalIndent(doc, "", "  ")
				if err != nil {
					return err
				}
				return g.copyOut(io.NopCloser(strings.NewReader(string(data)+"\n")), exportOut)
			})
		},
	}
	oneID(export)
	export.Flags().IntVar(&exportVersion, "version", 1, "which version")
	export.Flags().StringVarP(&exportOut, "out", "O", "-", "where to write")
	var importDomain string
	importCmd := &cobra.Command{
		Use: "import <file>", Short: "Create a draft from an exported document; values are validated",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := readSpecFile(args[0])
			if err != nil {
				return err
			}
			if !json.Valid(data) {
				return usageError(errors.New(args[0] + " is not valid JSON"))
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				out, err := c.ImportCatalog(ctx, importDomain, data)
				if err != nil {
					return err
				}
				return g.show(out, func(w io.Writer) {
					fmt.Fprintf(w, "%s version %d (%s)\n", out.Catalog.ID, out.Catalog.Version, out.Catalog.Status)
					if len(out.Rejected) > 0 {
						fmt.Fprintf(w, "rejected: %s\n", countsText(out.Rejected))
					}
				})
			})
		},
	}
	oneID(importCmd)
	importCmd.Flags().StringVar(&importDomain, "domain", "", "the domain it belongs to")

	var groupVersion int
	var groupName, groupDescription, groupType, groupCategory string
	putGroup := &cobra.Command{
		Use: "put-group <id> <key>", Short: "Add or change a group of a draft", Args: usageArgs(cobra.ExactArgs(2)),
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				group := vocab.Group{Key: args[1], Name: groupName, Description: groupDescription, DataType: groupType, Category: groupCategory}
				if err := c.PutGroup(ctx, args[0], groupVersion, &group); err != nil {
					return err
				}
				return g.show(map[string]any{"group": args[1]}, func(w io.Writer) { fmt.Fprintf(w, "saved group %s\n", args[1]) })
			})
		},
	}
	pg := putGroup.Flags()
	pg.IntVar(&groupVersion, "version", 1, "which draft")
	pg.StringVar(&groupName, "name", "", "a title")
	pg.StringVar(&groupDescription, "description", "", "what the values are")
	pg.StringVar(&groupType, "type", "string", "string, integer, decimal or enum")
	pg.StringVar(&groupCategory, "category", "", "a broad theme")
	delGroup := &cobra.Command{
		Use: "delete-group <id> <key>", Short: "Remove a group from a draft", Args: usageArgs(cobra.ExactArgs(2)),
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if err := c.DeleteGroup(ctx, args[0], groupVersion, args[1]); err != nil {
					return err
				}
				return g.show(map[string]any{"deleted": args[1]}, func(w io.Writer) { fmt.Fprintf(w, "deleted group %s\n", args[1]) })
			})
		},
	}
	delGroup.Flags().IntVar(&groupVersion, "version", 1, "which draft")
	var valuesFile string
	addValues := &cobra.Command{
		Use: "add-values <id> <key> -f file", Short: "Add values to a draft group, one per line; an optional tab and weight may follow", Args: usageArgs(cobra.ExactArgs(2)),
		RunE: func(cmd *cobra.Command, args []string) error {
			var source io.Reader = os.Stdin
			if valuesFile != "" && valuesFile != "-" {
				f, err := os.Open(valuesFile)
				if err != nil {
					return usageError(err)
				}
				defer f.Close()
				source = f
			}
			values, err := ReadValues(source)
			if err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				out, err := c.AddValues(ctx, args[0], groupVersion, args[1], values)
				if err != nil {
					return err
				}
				return g.show(out, func(w io.Writer) {
					fmt.Fprintf(w, "added %d\n", out.Added)
					if len(out.Rejected) > 0 {
						fmt.Fprintf(w, "rejected: %s\n", countsText(out.Rejected))
					}
				})
			})
		},
	}
	addValues.Flags().IntVar(&groupVersion, "version", 1, "which draft")
	addValues.Flags().StringVarP(&valuesFile, "file", "f", "-", "a file of values, or - for standard input")
	delValue := &cobra.Command{
		Use: "delete-value <id> <key> <value>", Short: "Remove a value from a draft group", Args: usageArgs(cobra.ExactArgs(3)),
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if err := c.DeleteValue(ctx, args[0], groupVersion, args[1], args[2]); err != nil {
					return err
				}
				return g.show(map[string]any{"deleted": args[2]}, func(w io.Writer) { fmt.Fprintf(w, "deleted %q\n", args[2]) })
			})
		},
	}
	delValue.Flags().IntVar(&groupVersion, "version", 1, "which draft")
	diff := &cobra.Command{
		Use: "diff <id> <from-version> <to-version>", Short: "Show what changed between versions", Args: usageArgs(cobra.ExactArgs(3)),
		RunE: func(cmd *cobra.Command, args []string) error {
			from, err1 := strconv.Atoi(args[1])
			to, err2 := strconv.Atoi(args[2])
			if err1 != nil || err2 != nil {
				return usageError(errors.New("versions must be numbers"))
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				a, err := c.GetCatalog(ctx, args[0], from)
				if err != nil {
					return err
				}
				b, err := c.GetCatalog(ctx, args[0], to)
				if err != nil {
					return err
				}
				d := DiffCatalogs(a.Groups, b.Groups)
				return g.show(d, func(w io.Writer) { writeDiff(w, &d) })
			})
		},
	}
	cmd.AddCommand(builtins, list, create, inspect, versions, publish, newVersion, clone, export, importCmd, putGroup, delGroup, addValues, delValue, diff)
	return cmd
}

func writeDiff(w io.Writer, d *CatalogDiff) {
	if len(d.GroupsAdded)+len(d.GroupsRemoved)+len(d.ValuesAdded)+len(d.ValuesRemoved) == 0 {
		fmt.Fprintln(w, "no differences")
		return
	}
	for _, g := range d.GroupsAdded {
		fmt.Fprintf(w, "+ group %s\n", g)
	}
	for _, g := range d.GroupsRemoved {
		fmt.Fprintf(w, "- group %s\n", g)
	}
	for _, g := range sortedStrings(d.ValuesAdded) {
		for _, v := range d.ValuesAdded[g] {
			fmt.Fprintf(w, "+ %s: %s\n", g, v)
		}
	}
	for _, g := range sortedStrings(d.ValuesRemoved) {
		for _, v := range d.ValuesRemoved[g] {
			fmt.Fprintf(w, "- %s: %s\n", g, v)
		}
	}
}

// Workers and tokens

func addWorkerManagement(g *globals, worker *cobra.Command) {
	list := &cobra.Command{
		Use: "list", Short: "List Workers", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				items, err := c.ListWorkers(ctx)
				if err != nil {
					return err
				}
				return g.show(items, func(w io.Writer) {
					rows := make([][]string, len(items))
					for i := range items {
						item := &items[i]
						rows[i] = []string{item.ID, item.Name, item.Host, item.State, strconv.Itoa(item.Slots), HumanTime(item.LastHeartbeatAt)}
					}
					tabulate(w, []string{"ID", "NAME", "HOST", "STATE", "SLOTS", "LAST SEEN"}, rows)
				})
			})
		},
	}
	inspect := &cobra.Command{
		Use: "inspect <id>", Short: "Show a Worker",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				out, err := c.GetWorker(ctx, args[0])
				if err != nil {
					return err
				}
				return g.show(out, func(w io.Writer) {
					fields(w, "ID", out.ID, "Name", out.Name, "Host", out.Host, "State", out.State, "Slots", strconv.Itoa(out.Slots), "Registered", HumanTime(&out.RegisteredAt),
						"Last seen", HumanTime(out.LastHeartbeatAt), "Capabilities", string(out.Capabilities), "Last error", out.LastError)
				})
			})
		},
	}
	oneID(inspect)
	drain := &cobra.Command{
		Use: "drain <id>", Short: "Give a Worker no new units; those it has run to the end",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if err := c.DrainWorker(ctx, args[0]); err != nil {
					return err
				}
				return g.show(map[string]any{"draining": args[0]}, func(w io.Writer) { fmt.Fprintf(w, "draining %s\n", args[0]) })
			})
		},
	}
	oneID(drain)
	revoke := &cobra.Command{
		Use: "revoke <id>", Short: "Cut a Worker off for good; its units go to others",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := g.confirm("revoke worker " + args[0]); err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if err := c.RevokeWorker(ctx, args[0]); err != nil {
					return err
				}
				return g.show(map[string]any{"revoked": args[0]}, func(w io.Writer) { fmt.Fprintf(w, "revoked %s\n", args[0]) })
			})
		},
	}
	oneID(revoke)
	del := &cobra.Command{
		Use: "delete <id>", Short: "Forget a Worker that is no longer connected, freeing its registration",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := g.confirm("forget worker " + args[0]); err != nil {
				return err
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if err := c.DeleteWorker(ctx, args[0]); err != nil {
					return err
				}
				return g.show(map[string]any{"deleted": args[0]}, func(w io.Writer) { fmt.Fprintf(w, "deleted %s\n", args[0]) })
			})
		},
	}
	oneID(del)
	var ttl string
	var maxUses int
	tokenCreate := &cobra.Command{
		Use: "create", Short: "Create a join token; it is shown only now", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				out, err := c.CreateJoinToken(ctx, maxUses, ttl)
				if err != nil {
					return err
				}
				return g.show(out, func(w io.Writer) { fmt.Fprintln(w, out.Token) })
			})
		},
	}
	tokenCreate.Flags().StringVar(&ttl, "ttl", "", "how long it lasts, such as 24h; default for ever")
	tokenCreate.Flags().IntVar(&maxUses, "max-uses", 0, "how many Workers it can register; default no limit")
	token := &cobra.Command{Use: "token", Short: "Manage join tokens"}
	token.AddCommand(tokenCreate)
	worker.AddCommand(list, inspect, drain, revoke, del, token)
}

func newTokenCommand(g *globals) *cobra.Command {
	cmd := &cobra.Command{Use: "token", Short: "Manage API tokens"}
	var name, role, ttl string
	create := &cobra.Command{
		Use: "create", Short: "Create an API token; it is shown only now", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				out, err := c.CreateToken(ctx, name, role, ttl)
				if err != nil {
					return err
				}
				return g.show(out, func(w io.Writer) { fmt.Fprintln(w, out.Token) })
			})
		},
	}
	create.Flags().StringVar(&name, "name", "", "what it is for")
	create.Flags().StringVar(&role, "role", api.RoleViewer, "viewer, operator or admin")
	create.Flags().StringVar(&ttl, "ttl", "", "how long it lasts, such as 720h; default for ever")
	list := &cobra.Command{
		Use: "list", Short: "List API tokens", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				items, err := c.ListTokens(ctx)
				if err != nil {
					return err
				}
				return g.show(items, func(w io.Writer) {
					rows := make([][]string, len(items))
					for i := range items {
						item := &items[i]
						rows[i] = []string{item.ID, item.Name, item.Role, HumanTime(&item.CreatedAt), HumanTime(item.ExpiresAt), HumanTime(item.RevokedAt)}
					}
					tabulate(w, []string{"ID", "NAME", "ROLE", "CREATED", "EXPIRES", "REVOKED"}, rows)
				})
			})
		},
	}
	revoke := &cobra.Command{
		Use: "revoke <id>", Short: "Revoke an API token",
		RunE: func(cmd *cobra.Command, args []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				if err := c.RevokeToken(ctx, args[0]); err != nil {
					return err
				}
				return g.show(map[string]any{"revoked": args[0]}, func(w io.Writer) { fmt.Fprintf(w, "revoked %s\n", args[0]) })
			})
		},
	}
	oneID(revoke)
	cmd.AddCommand(create, list, revoke)
	return cmd
}

func newAdminCommand(g *globals) *cobra.Command {
	cmd := &cobra.Command{Use: "admin", Short: "Back up the database and check stored output"}
	backup := &cobra.Command{
		Use: "backup <file>", Short: "Save a consistent copy of the database while the Coordinator runs",
		Long: "Save a consistent copy of the database while the Coordinator runs. Output files are not included:\n" +
			"copy <data_dir>/outputs separately, after this finishes. Restoring is described in the operations guide.",
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			if args[0] == "-" {
				return usageError(errors.New("a backup goes to a file"))
			}
			if _, err := os.Stat(args[0]); err == nil {
				return usageError(fmt.Errorf("%s already exists", args[0]))
			}
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				body, err := c.Backup(ctx)
				if err != nil {
					return err
				}
				return g.copyOut(body, args[0])
			})
		},
	}
	var sessionID string
	verify := &cobra.Command{
		Use: "verify", Short: "Compare stored output with its records (exit code 1 if anything differs)", Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return g.runRemote(cmd, func(ctx context.Context, c *apiclient.Client) error {
				reports, err := c.Verify(ctx, sessionID)
				if err != nil {
					return err
				}
				parts, damaged := 0, 0
				for i := range reports {
					parts += reports[i].Parts
					damaged += len(reports[i].Problems)
				}
				if err := g.show(map[string]any{"items": reports}, func(w io.Writer) {
					var rows [][]string
					for i := range reports {
						for _, p := range reports[i].Problems {
							rows = append(rows, []string{reports[i].SessionID, p.Stream, p.Reason, p.RelPath})
						}
					}
					if len(rows) > 0 {
						tabulate(w, []string{"SESSION", "STREAM", "PROBLEM", "PATH"}, rows)
					}
					fmt.Fprintf(w, "checked %d parts in %d sessions: %d damaged\n", parts, len(reports), damaged)
				}); err != nil {
					return err
				}
				if damaged > 0 {
					return apperr.New(apperr.Internal, "ERR_VERIFY_FAILED", fmt.Sprintf("%d parts are damaged: a retry of the session regenerates the same bytes", damaged))
				}
				return nil
			})
		},
	}
	verify.Flags().StringVar(&sessionID, "session", "", "only this session; default every finished one")
	cmd.AddCommand(backup, verify)
	return cmd
}
