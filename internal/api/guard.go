package api

import (
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/genadigeno/data-feed/internal/config"
)

// HostAllowed says whether a request's Host fits a listener. A server on loopback only ever
// has local names: any other means a page used DNS rebinding to reach it from a browser.
func HostAllowed(listen, host string) bool {
	if !config.IsLoopback(listen) {
		return true
	}
	name := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		name = h
	}
	name = strings.Trim(name, "[]")
	if strings.EqualFold(name, "localhost") || strings.HasSuffix(strings.ToLower(name), ".localhost") {
		return true
	}
	ip := net.ParseIP(name)
	return ip != nil && ip.IsLoopback()
}

// CrossOrigin reports a browser write from another site. Browsers add Origin to those and
// pages can't change it, and tools without one aren't browsers. This is what protects a
// server running with no authentication, which has no token for a site to lack.
func CrossOrigin(method, origin, host string) bool {
	if origin == "" || method == http.MethodGet || method == http.MethodHead {
		return false
	}
	u, err := url.Parse(origin)
	return err != nil || !strings.EqualFold(u.Host, host)
}

// GuardHost refuses requests with a Host that can't be right for a listener.
func GuardHost(listen string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !HostAllowed(listen, r.Host) {
			http.Error(w, "unexpected Host", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
