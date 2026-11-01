// Package web serves the browser interface: static files built into the binary. It has no
// private endpoints; the pages use the public API like any other client.
// See docs/03-interfaces/web-interface.md.
package web

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"
)

//go:embed static
var files embed.FS

// ContentSecurityPolicy allows nothing from elsewhere and no inline scripts.
const ContentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; " +
	"font-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"

const indexFile = "index.html"

var zeroTime time.Time // no modification date: files are identified by ETag

type asset struct {
	body        []byte
	etag        string
	contentType string
}

// Resolve decides what a path serves. A path naming no file is a page of the application,
// which index.html handles; a missing asset is an error rather than a page.
func Resolve(requestPath string, exists func(string) bool) (name string, found bool) {
	clean := strings.TrimPrefix(path.Clean("/"+requestPath), "/")
	if clean == "" {
		return indexFile, true
	}
	if exists(clean) {
		return clean, true
	}
	if path.Ext(clean) != "" {
		return "", false
	}
	return indexFile, true
}

func load() (map[string]asset, error) {
	root, err := fs.Sub(files, "static")
	if err != nil {
		return nil, err
	}
	out := map[string]asset{}
	err = fs.WalkDir(root, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		body, err := fs.ReadFile(root, name)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		kind := mime.TypeByExtension(path.Ext(name))
		switch path.Ext(name) { // the system's table can be wrong, as on some Windows installations
		case ".js":
			kind = "text/javascript; charset=utf-8"
		case ".css":
			kind = "text/css; charset=utf-8"
		case ".html":
			kind = "text/html; charset=utf-8"
		case ".txt":
			kind = "text/plain; charset=utf-8"
		case ".svg":
			kind = "image/svg+xml"
		}
		out[name] = asset{body: body, etag: `"` + hex.EncodeToString(sum[:8]) + `"`, contentType: kind}
		return nil
	})
	return out, err
}

// Handler serves the interface. Assets are revalidated by ETag rather than cached for a
// time, so an upgraded binary takes effect at once.
func Handler() (http.Handler, error) {
	assets, err := load()
	if err != nil {
		return nil, err
	}
	exists := func(name string) bool {
		_, ok := assets[name]
		return ok
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", ContentSecurityPolicy)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			h.Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name, found := Resolve(r.URL.Path, exists)
		if !found {
			http.NotFound(w, r)
			return
		}
		a := assets[name]
		h.Set("Content-Type", a.contentType)
		h.Set("ETag", a.etag)
		h.Set("Cache-Control", "no-cache")
		http.ServeContent(w, r, "", zeroTime, bytes.NewReader(a.body))
	}), nil
}
