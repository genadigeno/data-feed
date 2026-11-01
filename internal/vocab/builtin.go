package vocab

import (
	"embed"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"
)

// BuiltinPrefix marks a built-in catalog in a reference: builtin:<domain>.
const BuiltinPrefix = "builtin:"

//go:embed builtin/*.json
var builtinFiles embed.FS

// BuiltinDoc is a built-in catalog's embedded document.
type BuiltinDoc struct {
	Domain      string  `json:"domain"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Groups      []Group `json:"groups"`
}

// Builtins lists the built-in domains, in order.
func Builtins() []string {
	entries, err := builtinFiles.ReadDir("builtin")
	if err != nil {
		panic(fmt.Sprintf("embedded catalogs: %v", err)) // the embed directive guarantees it exists
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, strings.TrimSuffix(e.Name(), ".json"))
	}
	slices.Sort(out)
	return out
}

// Builtin returns a built-in catalog, read fresh, so callers can't change what others see.
func Builtin(domain string) (*BuiltinDoc, bool) {
	if strings.ContainsAny(domain, `/\.`) {
		return nil, false
	}
	data, err := builtinFiles.ReadFile(path.Join("builtin", domain+".json"))
	if err != nil {
		return nil, false
	}
	var doc BuiltinDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		panic(fmt.Sprintf("embedded catalog %s: %v", domain, err)) // a test reads every one
	}
	return &doc, true
}
