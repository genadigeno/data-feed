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
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/genadigeno/data-feed/internal/api"
	"github.com/genadigeno/data-feed/internal/apiclient"
	"github.com/genadigeno/data-feed/internal/app"
	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/config"
)

const (
	envToken       = "DATAFEED_TOKEN"
	defaultTimeout = 60 * time.Second
)

// runRemote runs a command against the Coordinator it should talk to: a running one, or
// with --local one started just for this command. Both go through the same client.
func (g *globals) runRemote(cmd *cobra.Command, fn func(ctx context.Context, c *apiclient.Client) error) error {
	ctx := cmd.Context()
	if !g.local {
		base, err := g.serverURL(g.server)
		if err != nil {
			return err
		}
		token, err := g.apiToken()
		if err != nil {
			return err
		}
		return fn(ctx, apiclient.New(base, defaultTimeout).WithToken(token))
	}

	flags := map[string]string{"server.listen": "127.0.0.1:0", "coordination.listen": "127.0.0.1:0", "auth.mode": "none"}
	if !g.verbose {
		g.quiet = true // the server is a detail of this command: only its problems are of interest
	}
	loaded, err := g.loadConfig(config.RoleCoordinator, flags)
	if err != nil {
		return err
	}
	logger, err := g.newLogger(&loaded.Config)
	if err != nil {
		return usageError(err)
	}
	// The Coordinator stops when the command finishes; a first Ctrl+C reaches both.
	serverCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	defer stop()
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- app.RunCoordinatorNotify(serverCtx, &loaded.Config, logger, clock.Real(), func(addr string) { ready <- addr })
	}()
	select {
	case addr := <-ready:
		err := fn(ctx, apiclient.New("http://"+addr, defaultTimeout))
		stop()
		return errors.Join(err, <-done)
	case err := <-done:
		return err
	case <-ctx.Done():
		stop()
		<-done
		return ctx.Err()
	}
}

// apiToken finds the token: --token, then --token-file, then $DATAFEED_TOKEN.
func (g *globals) apiToken() (string, error) {
	if g.token != "" {
		return g.token, nil
	}
	if g.tokenFile != "" {
		data, err := os.ReadFile(g.tokenFile)
		if err != nil {
			return "", usageError(fmt.Errorf("read --token-file: %w", err))
		}
		return strings.TrimSpace(string(data)), nil
	}
	return os.Getenv(envToken), nil
}

// show prints a value as JSON, or through its table form.
func (g *globals) show(v any, table func(io.Writer)) error {
	if g.output == outputJSON {
		return writeJSON(g.stdout, v)
	}
	table(g.stdout)
	return nil
}

// tabulate prints rows under a header, aligned.
func tabulate(w io.Writer, header []string, rows [][]string) {
	t := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	fmt.Fprintln(t, strings.Join(header, "\t"))
	for _, row := range rows {
		fmt.Fprintln(t, strings.Join(row, "\t"))
	}
	_ = t.Flush() // the destination is the terminal: nothing useful to do about a failure
}

// fields prints label and value pairs, aligned, leaving out what is empty.
func fields(w io.Writer, pairs ...string) {
	t := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			fmt.Fprintf(t, "%s:\t%s\n", pairs[i], pairs[i+1])
		}
	}
	_ = t.Flush()
}

// HumanCount renders a count string with thousands separators.
func HumanCount(s string) string {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return s
	}
	text := strconv.FormatUint(n, 10)
	var b strings.Builder
	for i, c := range text {
		if i > 0 && (len(text)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// HumanBytes renders a byte count string.
func HumanBytes(s string) string {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return s
	}
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	value, suffix := float64(n), "B"
	for _, next := range []string{"KiB", "MiB", "GiB", "TiB", "PiB"} {
		if value < unit {
			break
		}
		value /= unit
		suffix = next
	}
	return fmt.Sprintf("%.1f %s", value, suffix)
}

// HumanTime renders a moment, or nothing.
func HumanTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

// ProgressLine describes a session in one line.
func ProgressLine(s *api.SessionDTO) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-9s %s records  %s", s.State, HumanCount(s.Progress.Records), HumanBytes(s.Progress.Bytes))
	if s.Progress.Fraction != nil {
		fmt.Fprintf(&b, "  %.1f%%", *s.Progress.Fraction*100)
	}
	if s.Rates.Current > 0 {
		fmt.Fprintf(&b, "  %s/s", HumanCount(strconv.FormatFloat(s.Rates.Current, 'f', 0, 64)))
	}
	if s.Errors.Count > 0 {
		fmt.Fprintf(&b, "  %d errors", s.Errors.Count)
	}
	return b.String()
}

// Bottleneck says in words what a session is waiting for.
func Bottleneck(p *api.PressureDTO) string {
	if p == nil {
		return ""
	}
	switch p.LimitedBy {
	case "output":
		return "output: generators wait for the encoder"
	case "generation":
		return "generation: the encoder waits for records"
	case "delivery":
		return "delivery: waiting for parts to reach the Coordinator"
	case "rate":
		return "the configured rate"
	}
	return ""
}

func (g *globals) sessionTable(items []api.SessionDTO) func(io.Writer) {
	return func(w io.Writer) {
		rows := make([][]string, len(items))
		for i := range items {
			s := &items[i]
			rows[i] = []string{s.ID, s.Name, s.State, s.Mode, s.Format, HumanCount(s.Progress.Records), HumanBytes(s.Progress.Bytes), HumanTime(&s.CreatedAt)}
		}
		tabulate(w, []string{"ID", "NAME", "STATE", "MODE", "FORMAT", "RECORDS", "BYTES", "CREATED"}, rows)
	}
}

func (g *globals) showSession(s *api.SessionDTO) error {
	return g.show(s, func(w io.Writer) {
		var catalog, units, fraction string
		if s.Catalog != nil {
			catalog = fmt.Sprintf("%s v%d", s.Catalog.ID, s.Catalog.Version)
		}
		if s.Units != nil {
			units = fmt.Sprintf("%d pending, %d leased, %d committed, %d failed", s.Units.Pending, s.Units.Leased, s.Units.Committed, s.Units.Failed)
		}
		if s.Progress.Fraction != nil {
			fraction = fmt.Sprintf("%.1f%%", *s.Progress.Fraction*100)
		}
		fields(w,
			"ID", s.ID, "Name", s.Name, "Description", s.Description, "State", s.State, "Reason", s.StateReason,
			"Actions", strings.Join(s.AvailableActions, ", "), "Strategy", s.Strategy, "Mode", s.Mode, "Format", s.Format,
			"Seed", s.Seed, "Reference time", s.ReferenceTime.UTC().Format(time.RFC3339), "Snapshot", s.SnapshotHash, "Catalog", catalog,
			"Created", HumanTime(&s.CreatedAt), "Started", HumanTime(s.StartedAt), "Ended", HumanTime(s.EndedAt),
			"Records", HumanCount(s.Progress.Records), "Bytes", HumanBytes(s.Progress.Bytes), "Done", fraction,
			"Rate", fmt.Sprintf("%.0f/s now, %.0f/s average, %.0f/s peak", s.Rates.Current, s.Rates.Average, s.Rates.Peak),
			"Units", units, "Limited by", Bottleneck(s.Pressure), "Last error", s.Errors.Last,
		)
	})
}

func readSpecFile(path string) ([]byte, error) {
	if path == "" {
		return nil, usageError(errors.New("-f is required: a specification file, or - for standard input"))
	}
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, usageError(fmt.Errorf("read %s: %w", path, err))
	}
	return data, nil
}

// BuildSpec makes a session specification from command line flags.
func BuildSpec(name, mode, format string, schemaDoc json.RawMessage, schemaRef, catalog string, count uint64, rate float64, duration string, seed *uint64, unitRecords int64) ([]byte, error) {
	spec := map[string]any{"name": name, "mode": mode, "output": map[string]any{"format": format}}
	switch {
	case schemaRef != "":
		spec["schema"] = map[string]any{"ref": map[string]any{"id": schemaRef}}
	case len(schemaDoc) > 0:
		spec["schema"] = map[string]any{"inline": schemaDoc}
	default:
		return nil, usageError(errors.New("--schema is required: a file, or a saved schema's ID"))
	}
	limits := map[string]any{}
	if count > 0 {
		limits["count"] = count
	}
	if rate > 0 {
		limits["rate"] = rate
	}
	if duration != "" {
		limits["duration"] = duration
	}
	if len(limits) > 0 {
		spec["limits"] = limits
	}
	if seed != nil {
		spec["seed"] = *seed
	}
	if catalog != "" {
		spec["catalog"] = map[string]any{"ref": map[string]any{"id": catalog}}
	}
	if unitRecords > 0 {
		spec["execution"] = map[string]any{"unit_records": unitRecords}
	}
	return json.MarshalIndent(spec, "", "  ")
}

// IsTerminal says whether a session state is final.
func IsTerminal(state string) bool {
	return state == "COMPLETED" || state == "STOPPED" || state == "FAILED"
}

// watchSession follows a session until it ends. Updates only trigger a read, so that what
// is printed always comes from the server's current state; a lost stream is reopened.
func (g *globals) watchSession(ctx context.Context, c *apiclient.Client, id string) (api.SessionDTO, error) {
	report := func(s *api.SessionDTO) error {
		if g.output == outputJSON {
			return writeCompactJSON(g.stdout, s)
		}
		fmt.Fprintln(g.stderr, ProgressLine(s))
		return nil
	}
	const minGap = time.Second
	var last time.Time
	var lastState string
	check := func() (api.SessionDTO, bool, error) {
		s, err := c.GetSession(ctx, id, false)
		if err != nil {
			return s, false, err
		}
		if s.State != lastState || time.Since(last) >= minGap || IsTerminal(s.State) {
			last, lastState = time.Now(), s.State
			if err := report(&s); err != nil {
				return s, false, err
			}
		}
		return s, IsTerminal(s.State), nil
	}
	s, done, err := check()
	for !done && err == nil {
		streamErr := c.Events(ctx, id, func(apiclient.SSEEvent) (bool, error) {
			var checkErr error
			s, done, checkErr = check()
			return !done, checkErr
		})
		if ctx.Err() != nil {
			return s, nil // Ctrl+C only stops watching
		}
		if done {
			break
		}
		if e, ok := apperr.As(streamErr); ok && !e.Retryable && e.Kind != apperr.Unavailable {
			return s, streamErr
		}
		time.Sleep(minGap)
		s, done, err = check()
		if e, ok := apperr.As(err); ok && e.Kind == apperr.Unavailable {
			err = nil // the server may be restarting: keep trying
		}
	}
	return s, err
}

// sessionFailed makes a failed session a failed command, for scripts.
func sessionFailed(s *api.SessionDTO) error {
	if s.State != "FAILED" {
		return nil
	}
	return apperr.New(apperr.Internal, "ERR_SESSION_FAILED", fmt.Sprintf("session %s failed: %s", s.ID, s.Errors.Last))
}

func writeCompactJSON(w io.Writer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "%s\n", data)
	return err
}

// confirm asks before something can't be undone. Without a terminal there is nobody to
// ask, so --yes is needed.
func (g *globals) confirm(question string) error {
	if g.yes {
		return nil
	}
	if !isTerminal(os.Stdin) {
		return usageError(errors.New(question + ": refused without --yes, as nobody can be asked here"))
	}
	fmt.Fprintf(g.stderr, "%s? [y/N] ", question)
	var answer string
	_, err := fmt.Fscanln(os.Stdin, &answer)
	if errors.Is(err, io.EOF) {
		// Some things that are not terminals look like one: NUL on Windows, for instance.
		return usageError(errors.New(question + ": refused without --yes, as nobody can be asked here"))
	}
	if err != nil || !strings.EqualFold(answer, "y") {
		return apperr.New(apperr.Canceled, "ERR_CANCELLED", "cancelled")
	}
	return nil
}

// copyOut writes a download to a file, or to standard output with - or nothing.
func (g *globals) copyOut(r io.ReadCloser, path string) error {
	defer r.Close()
	if path == "" || path == "-" {
		_, err := io.Copy(g.stdout, r)
		return err
	}
	// Written to the side first, so that an interrupted download never looks complete.
	tmp := path + ".partial"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	n, err := io.Copy(f, r)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	fmt.Fprintf(g.stderr, "wrote %s (%s)\n", path, config.Size(n))
	return nil
}
