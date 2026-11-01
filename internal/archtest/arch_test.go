// Package archtest enforces the package boundaries in
// docs/01-architecture/package-structure.md by parsing imports.
package archtest

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/genadigeno/data-feed/internal/"

// rule forbids packages under Forbidden for every package under Packages. Entries
// without a dot are relative to internal/; entries with one are third-party modules.
type rule struct {
	Packages  []string
	Forbidden []string
}

var engine = []string{"record", "schema", "gen", "serial", "vocab"}

var rules = []rule{
	{
		Packages: engine,
		Forbidden: []string{
			"session", "coord", "worker", "protocol", "store", "artifact", "api", "apiclient", "cli", "app",
			"web", "ai", "pipeline", "output", "apperr", "net/http", "database/sql",
		},
	},
	{Packages: []string{"hub", "wire"}, Forbidden: []string{"store", "session", "coord", "worker", "protocol", "api", "apiclient", "cli", "app", "database/sql"}},
	{Packages: []string{"snapshot"}, Forbidden: []string{"store", "session", "coord", "worker", "protocol", "api", "apiclient", "cli", "app", "net/http", "database/sql"}},
	{Packages: []string{"ai"}, Forbidden: []string{"store", "api", "cli", "app", "session", "coord", "worker"}},
	{Packages: []string{"catalog"}, Forbidden: []string{"store/sqlite", "api", "apiclient", "protocol", "cli", "app", "session", "coord", "worker", "ai/claude"}},
	{Packages: []string{"pipeline", "output"}, Forbidden: []string{"store", "coord", "protocol", "api", "cli", "app", "session", "worker"}},
	{Packages: []string{"session", "coord"}, Forbidden: []string{"store/sqlite", "api", "apiclient", "protocol", "cli", "app"}},
	{Packages: []string{"worker"}, Forbidden: []string{"store", "artifact", "api", "apiclient", "protocol", "cli", "app", "session", "coord", "database/sql"}},
	{Packages: []string{"protocol", "api", "apiclient"}, Forbidden: []string{"store/sqlite", "cli", "app"}},
	{Packages: []string{"cli"}, Forbidden: []string{"store/sqlite"}},
}

// matches reports whether pkg is base or inside it.
func matches(pkg, base string) bool {
	return pkg == base || strings.HasPrefix(pkg, base+"/")
}

// violations lists "pkg imports forbidden" for the given imports of one package.
// The SQLite driver is special: only store/sqlite may touch it.
func violations(pkg string, imports []string, rs []rule) []string {
	var out []string
	check := func(forbidden string) {
		for _, imp := range imports {
			name := strings.TrimPrefix(imp, module)
			if matches(name, forbidden) {
				out = append(out, pkg+" imports "+imp)
			}
		}
	}
	for _, r := range rs {
		if !slices.ContainsFunc(r.Packages, func(p string) bool { return matches(pkg, p) }) {
			continue
		}
		for _, f := range r.Forbidden {
			// A package's own subpackages are part of it.
			if !matches(f, pkg) {
				check(f)
			}
		}
	}
	if !matches(pkg, "store/sqlite") {
		check("modernc.org/sqlite")
	}
	return out
}

func TestViolationsDetection(t *testing.T) {
	rs := []rule{{Packages: []string{"gen"}, Forbidden: []string{"store", "net/http"}}}

	cases := []struct {
		pkg     string
		imports []string
		want    int
	}{
		{"gen", []string{module + "store/sqlite"}, 1},
		{"gen/value", []string{module + "store", "net/http"}, 2},
		{"gen", []string{module + "record", "fmt"}, 0},
		{"generator", []string{module + "store"}, 0}, // name prefix is not containment
		{"app", []string{"modernc.org/sqlite"}, 1},
		{"store/sqlite", []string{"modernc.org/sqlite"}, 0},
	}
	for _, c := range cases {
		if got := violations(c.pkg, c.imports, rs); len(got) != c.want {
			t.Errorf("violations(%s, %v) = %v, want %d", c.pkg, c.imports, got, c.want)
		}
	}
}

func TestPackageBoundaries(t *testing.T) {
	root := filepath.Join("..")
	imports := map[string][]string{}

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, filepath.Dir(p))
		if err != nil {
			return err
		}
		pkg := filepath.ToSlash(rel)
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, src, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range f.Imports {
			imp, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			imports[pkg] = append(imports[pkg], imp)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := imports["store/sqlite"]; !ok {
		t.Fatal("did not find the packages: wrong root?")
	}

	for pkg, imps := range imports {
		for _, v := range violations(pkg, imps, rules) {
			t.Error(v)
		}
	}
}
