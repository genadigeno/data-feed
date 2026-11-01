package cli

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/genadigeno/data-feed/internal/api"
	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/vocab"
)

func execute(t *testing.T, args ...string) (string, string, int) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := Execute(args, &out, &errOut)
	return out.String(), errOut.String(), code
}

func TestEveryCommandHasHelpAndParses(t *testing.T) {
	g := &globals{}
	var walk func(*cobra.Command)
	count := 0
	walk = func(c *cobra.Command) {
		count++
		if c.Short == "" {
			t.Errorf("%q has no description", c.CommandPath())
		}
		for _, sub := range c.Commands() {
			walk(sub)
		}
	}
	walk(newRootCommand(g))
	if count < 60 {
		t.Fatalf("only %d commands", count)
	}
}

func TestLocalAndServerConflict(t *testing.T) {
	_, errOut, code := execute(t, "dashboard", "--local", "--server", "http://x")
	if code != apperr.ExitUsage || !strings.Contains(errOut, "--local") {
		t.Fatalf("%d %s", code, errOut)
	}
}

func TestUsageErrorsExitTwo(t *testing.T) {
	cases := [][]string{
		{"session", "inspect"},
		{"session", "inspect", "a", "b"},
		{"session", "create", "--nonsense"},
		{"catalog", "diff", "x"},
		{"-o", "xml", "version"},
	}
	for _, args := range cases {
		if _, errOut, code := execute(t, args...); code != apperr.ExitUsage {
			t.Errorf("%v: exit %d: %s", args, code, errOut)
		}
	}
}

func TestDestructiveCommandsNeedConfirmationWithoutATerminal(t *testing.T) {
	// Standard input is not a terminal under go test, so there is nobody to ask.
	for _, args := range [][]string{{"session", "delete", "x"}, {"session", "purge", "x"}, {"schema", "delete", "x"}, {"worker", "revoke", "x"}, {"domain", "delete", "x"}} {
		_, errOut, code := execute(t, args...)
		if code != apperr.ExitUsage || !strings.Contains(errOut, "--yes") {
			t.Errorf("%v: exit %d: %s", args, code, errOut)
		}
	}
}

func TestBuildSpec(t *testing.T) {
	seed := uint64(0) // zero is a real seed: it must not vanish
	data, err := BuildSpec("n", "stream", "csv", json.RawMessage(`"kind: hybrid"`), "", "builtin:gaming", 10, 5.5, "10m", &seed, 100)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	limits := got["limits"].(map[string]any)
	if limits["count"] != float64(10) || limits["rate"] != 5.5 || limits["duration"] != "10m" {
		t.Fatalf("%v", limits)
	}
	if _, ok := got["seed"]; !ok {
		t.Fatal("seed 0 was lost")
	}
	if got["schema"].(map[string]any)["inline"] != "kind: hybrid" {
		t.Fatalf("%v", got["schema"])
	}
	if got["catalog"].(map[string]any)["ref"].(map[string]any)["id"] != "builtin:gaming" {
		t.Fatalf("%v", got["catalog"])
	}

	data, err = BuildSpec("n", "batch", "json", nil, "sch_1", "", 0, 0, "", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	got = map[string]any{}
	_ = json.Unmarshal(data, &got)
	for _, key := range []string{"limits", "seed", "catalog", "execution"} {
		if _, ok := got[key]; ok {
			t.Errorf("sent an empty %s", key)
		}
	}
	if got["schema"].(map[string]any)["ref"].(map[string]any)["id"] != "sch_1" {
		t.Fatalf("%v", got["schema"])
	}
	if _, err := BuildSpec("n", "batch", "json", nil, "", "", 0, 0, "", nil, 0); err == nil {
		t.Fatal("no schema accepted")
	}
}

func TestHumanForms(t *testing.T) {
	for in, want := range map[string]string{"0": "0", "999": "999", "1000": "1,000", "1234567": "1,234,567", "x": "x"} {
		if got := HumanCount(in); got != want {
			t.Errorf("%s: %s", in, got)
		}
	}
	if got := HumanBytes("x"); got != "x" {
		t.Fatal(got)
	}
	for in, want := range map[string]string{"0": "0 B", "1023": "1023 B", "1024": "1.0 KiB", "290000": "283.2 KiB", "1073741824": "1.0 GiB"} {
		if got := HumanBytes(in); got != want {
			t.Errorf("%s: %s", in, got)
		}
	}
	if HumanTime(nil) != "" {
		t.Fatal("a missing time prints nothing")
	}
}

func TestProgressLine(t *testing.T) {
	f := 0.5
	s := api.SessionDTO{State: "RUNNING", Progress: api.ProgressDTO{Records: "1000", Bytes: "2048", Fraction: &f}, Rates: api.RatesDTO{Current: 2500}, Errors: api.ErrorsDTO{Count: 2}}
	line := ProgressLine(&s)
	for _, want := range []string{"RUNNING", "1,000", "50.0%", "2,500/s", "2 errors"} {
		if !strings.Contains(line, want) {
			t.Errorf("no %q in %q", want, line)
		}
	}
	quiet := ProgressLine(&api.SessionDTO{State: "PAUSED", Progress: api.ProgressDTO{Records: "0", Bytes: "0"}})
	if strings.Contains(quiet, "%") || strings.Contains(quiet, "/s") || strings.Contains(quiet, "errors") {
		t.Errorf("%q", quiet)
	}
}

func TestBottleneck(t *testing.T) {
	if Bottleneck(nil) != "" || Bottleneck(&api.PressureDTO{}) != "" {
		t.Fatal("nothing is limiting")
	}
	if got := Bottleneck(&api.PressureDTO{LimitedBy: "rate"}); got != "the configured rate" {
		t.Fatal(got)
	}
}

func TestChooseStream(t *testing.T) {
	one := []api.ArtifactDTO{{Stream: "main"}}
	two := []api.ArtifactDTO{{Stream: "table:a"}, {Stream: "table:b"}}
	if got, err := ChooseStream(one, ""); err != nil || got != "main" {
		t.Fatal(got, err)
	}
	if got, err := ChooseStream(two, "table:b"); err != nil || got != "table:b" {
		t.Fatal(got, err)
	}
	_, err := ChooseStream(two, "")
	if e, ok := apperr.As(err); !ok || e.Kind != apperr.InvalidArgument || !strings.Contains(e.Message, "table:a, table:b") {
		t.Fatalf("%v", err)
	}
	if _, err := ChooseStream(nil, ""); err == nil {
		t.Fatal("nothing to choose")
	}
}

func TestSessionFailedIsACommandFailure(t *testing.T) {
	if err := sessionFailed(&api.SessionDTO{State: "COMPLETED"}); err != nil {
		t.Fatal(err)
	}
	if err := sessionFailed(&api.SessionDTO{State: "STOPPED"}); err != nil {
		t.Fatal(err)
	}
	if err := sessionFailed(&api.SessionDTO{State: "FAILED", Errors: api.ErrorsDTO{Last: "boom"}}); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("%v", err)
	}
}

func TestDiffCatalogs(t *testing.T) {
	entry := func(values ...string) []vocab.Entry {
		out := make([]vocab.Entry, len(values))
		for i, v := range values {
			out[i].Value = v
		}
		return out
	}
	from := []vocab.Group{{Key: "same", Values: entry("a")}, {Key: "changed", Values: entry("x", "y")}, {Key: "gone", Values: entry("z")}}
	to := []vocab.Group{{Key: "same", Values: entry("a")}, {Key: "changed", Values: entry("y", "w")}, {Key: "new", Values: entry("q")}}
	d := DiffCatalogs(from, to)
	if !slices.Equal(d.GroupsAdded, []string{"new"}) || !slices.Equal(d.GroupsRemoved, []string{"gone"}) {
		t.Fatalf("%+v", d)
	}
	if !slices.Equal(d.ValuesAdded["changed"], []string{"w"}) || !slices.Equal(d.ValuesRemoved["changed"], []string{"x"}) {
		t.Fatalf("%+v", d)
	}
	if _, ok := d.ValuesAdded["same"]; ok {
		t.Fatalf("%+v", d)
	}
	var b bytes.Buffer
	writeDiff(&b, &d)
	for _, want := range []string{"+ group new", "- group gone", "+ changed: w", "- changed: x"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("no %q in:\n%s", want, b.String())
		}
	}
	var none bytes.Buffer
	same := DiffCatalogs(from, from)
	writeDiff(&none, &same)
	if !strings.Contains(none.String(), "no differences") {
		t.Fatal(none.String())
	}
}

func TestReadValues(t *testing.T) {
	got, err := ReadValues(strings.NewReader("plain\r\n\nwith weight\t2.5\n  spaced  \n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Value != "plain" || got[1].Weight != 2.5 || got[2].Value != "  spaced  " {
		t.Fatalf("%+v", got)
	}
	if _, err := ReadValues(strings.NewReader("x\tnot a number\n")); err == nil {
		t.Fatal("accepted a bad weight")
	}
}

func TestTabulateAligns(t *testing.T) {
	var b bytes.Buffer
	tabulate(&b, []string{"A", "BB"}, [][]string{{"long value", "x"}})
	lines := strings.Split(strings.TrimSpace(b.String()), "\n")
	if strings.Index(lines[0], "BB") != strings.Index(lines[1], "x") {
		t.Fatalf("%q", lines)
	}
}

func TestIsTerminal(t *testing.T) {
	for state, want := range map[string]bool{"COMPLETED": true, "STOPPED": true, "FAILED": true, "RUNNING": false, "PAUSED": false} {
		if IsTerminal(state) != want {
			t.Errorf("%s", state)
		}
	}
}

func TestCheckSchemaRef(t *testing.T) {
	for _, ok := range []string{"", "sch_01a1975678ac701793e64f2abd97d439"} {
		if err := checkSchemaRef(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	err := checkSchemaRef("customer.yaml")
	if err == nil || !strings.Contains(err.Error(), "no file customer.yaml") {
		t.Fatalf("got %v", err)
	}
}
