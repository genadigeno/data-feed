package web

import (
	"regexp"
	"strings"
	"testing"
)

func TestResolve(t *testing.T) {
	exists := func(name string) bool { return name == "app.js" || name == "vendor/x.js" || name == indexFile }
	cases := []struct {
		path, name string
		found      bool
	}{
		{"/", indexFile, true},
		{"", indexFile, true},
		{"/app.js", "app.js", true},
		{"/vendor/x.js", "vendor/x.js", true},
		{"/sessions", indexFile, true},
		{"/sessions/ses_1", indexFile, true},
		{"/missing.js", "", false},
		{"/../../etc/passwd", indexFile, true}, // cleaned to a page, never to a file outside
		{"/../app.js", "app.js", true},
	}
	for _, tc := range cases {
		name, found := Resolve(tc.path, exists)
		if name != tc.name || found != tc.found {
			t.Errorf("%q: got %q %v", tc.path, name, found)
		}
	}
}

func TestAssetsAreBuiltIn(t *testing.T) {
	assets, err := load()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{indexFile, "app.js", "app.css", "vendor/preact-htm.js"} {
		a, ok := assets[name]
		if !ok || len(a.body) == 0 || a.etag == "" || a.contentType == "" {
			t.Errorf("%s: %+v", name, ok)
		}
	}
	if !strings.HasPrefix(assets["app.js"].contentType, "text/javascript") {
		t.Fatal(assets["app.js"].contentType)
	}
}

// The policy forbids inline scripts and outside sources, so the page must use neither.
func TestIndexObeysThePolicy(t *testing.T) {
	assets, err := load()
	if err != nil {
		t.Fatal(err)
	}
	index := string(assets[indexFile].body)
	if regexp.MustCompile(`<script(?:\s[^>]*)?>\s*[^<\s]`).MatchString(index) {
		t.Fatal("inline script")
	}
	if regexp.MustCompile(`(?i)\son[a-z]+=`).MatchString(index) {
		t.Fatal("inline event handler")
	}
	if strings.Contains(index, "<style") {
		t.Fatal("inline style")
	}
	for name, a := range assets {
		if strings.HasPrefix(name, "vendor/") || strings.HasSuffix(name, ".txt") {
			continue
		}
		if m := regexp.MustCompile(`(?:src|href)=["']https?://[^"']+`).FindString(string(a.body)); m != "" {
			t.Errorf("%s loads from elsewhere: %s", name, m)
		}
		if strings.Contains(string(a.body), "eval(") || strings.Contains(string(a.body), "new Function") {
			t.Errorf("%s evaluates code", name)
		}
	}
}

func TestPolicyIsStrict(t *testing.T) {
	for _, want := range []string{"default-src 'none'", "script-src 'self'", "frame-ancestors 'none'"} {
		if !strings.Contains(ContentSecurityPolicy, want) {
			t.Errorf("no %s", want)
		}
	}
	if strings.Contains(ContentSecurityPolicy, "unsafe") {
		t.Fatal("unsafe source allowed")
	}
}

func TestJavaScriptModulesResolve(t *testing.T) {
	assets, err := load()
	if err != nil {
		t.Fatal(err)
	}
	importRe := regexp.MustCompile(`(?m)^import\s.*?from\s+["'](\./[^"']+)["']`)
	for name, a := range assets {
		if !strings.HasSuffix(name, ".js") || strings.HasPrefix(name, "vendor/") {
			continue
		}
		for _, m := range importRe.FindAllStringSubmatch(string(a.body), -1) {
			target := strings.TrimPrefix(m[1], "./")
			if _, ok := assets[target]; !ok {
				t.Errorf("%s imports %s, which does not exist", name, m[1])
			}
		}
	}
}
