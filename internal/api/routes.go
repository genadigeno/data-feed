package api

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/store"
)

// Roles, each allowed all that the one before it is.
const (
	RoleViewer   = "viewer"
	RoleOperator = "operator"
	RoleAdmin    = "admin"
)

const prefix = "/api/v1"

// handlerFunc is a route's handler. Errors are written by the wrapper.
type handlerFunc func(http.ResponseWriter, *http.Request) error

// route describes one endpoint.
type route struct {
	Method, Path string
	// Role is the least role allowed. Empty means no authentication at all.
	Role string
	// Stream marks long-lived responses, which hold no concurrency slot and no write deadline.
	Stream bool
	// Idempotent routes honour the Idempotency-Key header.
	Idempotent bool
	// Heavy routes wait their turn for one of a few slots.
	Heavy bool
	// Import allows the larger request body limit.
	Import  bool
	Handler handlerFunc
}

// routes is the whole public API; openapi.yaml must document exactly these (a test checks).
func (s *server) routes() []route {
	return []route{
		{Method: "GET", Path: "/health", Handler: s.health},
		{Method: "GET", Path: "/ready", Handler: s.ready},
		{Method: "GET", Path: "/openapi.yaml", Handler: s.openAPI},
		{Method: "GET", Path: "/metrics", Role: s.metricsRole(), Handler: s.metrics},
		{Method: "GET", Path: "/whoami", Role: RoleViewer, Handler: s.whoAmI},
		{Method: "POST", Path: "/auth/login", Handler: s.login},
		{Method: "POST", Path: "/auth/logout", Handler: s.logout},
		{Method: "GET", Path: "/dashboard", Role: RoleViewer, Handler: s.dashboard},
		{Method: "GET", Path: "/events", Role: RoleViewer, Stream: true, Handler: s.events},

		{Method: "POST", Path: "/sessions", Role: RoleOperator, Heavy: true, Idempotent: true, Handler: s.createSession},
		{Method: "GET", Path: "/sessions", Role: RoleViewer, Handler: s.listSessions},
		{Method: "POST", Path: "/sessions/validate", Role: RoleViewer, Heavy: true, Handler: s.validateSession},
		{Method: "GET", Path: "/sessions/{id}", Role: RoleViewer, Handler: s.getSession},
		{Method: "DELETE", Path: "/sessions/{id}", Role: RoleOperator, Handler: s.deleteSession},
		{Method: "PUT", Path: "/sessions/{id}/config", Role: RoleOperator, Handler: s.configureSession},
		{Method: "POST", Path: "/sessions/{id}/start", Role: RoleOperator, Idempotent: true, Handler: s.startSession},
		{Method: "POST", Path: "/sessions/{id}/pause", Role: RoleOperator, Handler: s.pauseSession},
		{Method: "POST", Path: "/sessions/{id}/resume", Role: RoleOperator, Handler: s.resumeSession},
		{Method: "POST", Path: "/sessions/{id}/stop", Role: RoleOperator, Idempotent: true, Handler: s.stopSession},
		{Method: "POST", Path: "/sessions/{id}/clone", Role: RoleOperator, Idempotent: true, Handler: s.cloneSession},
		{Method: "POST", Path: "/sessions/{id}/retry", Role: RoleOperator, Idempotent: true, Handler: s.retrySession},
		{Method: "GET", Path: "/sessions/{id}/progress", Role: RoleViewer, Handler: s.sessionProgress},
		{Method: "GET", Path: "/sessions/{id}/metrics", Role: RoleViewer, Handler: s.sessionMetrics},
		{Method: "GET", Path: "/sessions/{id}/logs", Role: RoleViewer, Handler: s.sessionLogs},
		{Method: "GET", Path: "/sessions/{id}/events", Role: RoleViewer, Handler: s.sessionEvents},
		{Method: "GET", Path: "/sessions/{id}/artifacts", Role: RoleViewer, Handler: s.listArtifacts},
		{Method: "GET", Path: "/sessions/{id}/artifacts/manifest", Role: RoleViewer, Handler: s.artifactManifest},
		{Method: "DELETE", Path: "/sessions/{id}/artifacts", Role: RoleAdmin, Handler: s.purgeArtifacts},
		{Method: "GET", Path: "/sessions/{id}/artifacts/{stream}/parts/{n}", Role: RoleViewer, Stream: true, Handler: s.downloadPart},
		{Method: "GET", Path: "/sessions/{id}/artifacts/{stream}/merged", Role: RoleViewer, Heavy: false, Stream: true, Handler: s.downloadMerged},

		{Method: "POST", Path: "/generate", Role: RoleOperator, Heavy: true, Handler: s.generate},

		{Method: "POST", Path: "/schemas", Role: RoleOperator, Idempotent: true, Handler: s.createSchema},
		{Method: "GET", Path: "/schemas", Role: RoleViewer, Handler: s.listSchemas},
		{Method: "POST", Path: "/schemas/validate", Role: RoleViewer, Heavy: true, Handler: s.validateSchema},
		{Method: "GET", Path: "/schemas/{id}", Role: RoleViewer, Handler: s.getSchema},
		{Method: "PUT", Path: "/schemas/{id}", Role: RoleOperator, Handler: s.updateSchema},
		{Method: "DELETE", Path: "/schemas/{id}", Role: RoleOperator, Handler: s.deleteSchema},
		{Method: "POST", Path: "/schemas/{id}/preview", Role: RoleViewer, Heavy: true, Handler: s.previewSchema},

		{Method: "POST", Path: "/domains", Role: RoleOperator, Idempotent: true, Handler: s.createDomain},
		{Method: "GET", Path: "/domains", Role: RoleViewer, Handler: s.listDomains},
		{Method: "GET", Path: "/domains/{id}", Role: RoleViewer, Handler: s.getDomain},
		{Method: "PATCH", Path: "/domains/{id}", Role: RoleOperator, Handler: s.updateDomain},
		{Method: "DELETE", Path: "/domains/{id}", Role: RoleOperator, Handler: s.deleteDomain},
		{Method: "POST", Path: "/domains/{id}/vocabulary/generate", Role: RoleOperator, Idempotent: true, Handler: s.startVocabularyJob},

		{Method: "GET", Path: "/builtin-catalogs", Role: RoleViewer, Handler: s.listBuiltins},
		{Method: "GET", Path: "/builtin-catalogs/{domain}", Role: RoleViewer, Handler: s.getBuiltin},

		{Method: "POST", Path: "/catalogs", Role: RoleOperator, Idempotent: true, Handler: s.createCatalog},
		{Method: "GET", Path: "/catalogs", Role: RoleViewer, Handler: s.listCatalogs},
		{Method: "POST", Path: "/catalogs/import", Role: RoleOperator, Import: true, Handler: s.importCatalog},
		{Method: "GET", Path: "/catalogs/{id}", Role: RoleViewer, Heavy: true, Handler: s.getCatalog},
		{Method: "POST", Path: "/catalogs/{id}/clone", Role: RoleOperator, Handler: s.cloneCatalog},
		{Method: "GET", Path: "/catalogs/{id}/versions", Role: RoleViewer, Handler: s.catalogVersions},
		{Method: "POST", Path: "/catalogs/{id}/versions", Role: RoleOperator, Handler: s.newCatalogVersion},
		{Method: "POST", Path: "/catalogs/{id}/versions/{v}/publish", Role: RoleOperator, Handler: s.publishCatalog},
		{Method: "GET", Path: "/catalogs/{id}/versions/{v}/export", Role: RoleViewer, Heavy: true, Handler: s.exportCatalog},
		{Method: "PUT", Path: "/catalogs/{id}/versions/{v}/groups/{key}", Role: RoleOperator, Handler: s.putGroup},
		{Method: "DELETE", Path: "/catalogs/{id}/versions/{v}/groups/{key}", Role: RoleOperator, Handler: s.deleteGroup},
		{Method: "POST", Path: "/catalogs/{id}/versions/{v}/groups/{key}/values", Role: RoleOperator, Import: true, Handler: s.addValues},
		{Method: "DELETE", Path: "/catalogs/{id}/versions/{v}/groups/{key}/values", Role: RoleOperator, Handler: s.deleteValue},

		{Method: "GET", Path: "/vocabulary-jobs/{id}", Role: RoleViewer, Handler: s.getJob},
		{Method: "POST", Path: "/vocabulary-jobs/{id}/cancel", Role: RoleOperator, Handler: s.cancelJob},

		{Method: "GET", Path: "/workers", Role: RoleViewer, Handler: s.listWorkers},
		{Method: "GET", Path: "/workers/{id}", Role: RoleViewer, Handler: s.getWorker},
		{Method: "DELETE", Path: "/workers/{id}", Role: RoleAdmin, Handler: s.deleteWorker},
		{Method: "POST", Path: "/workers/{id}/drain", Role: RoleAdmin, Handler: s.drainWorker},
		{Method: "POST", Path: "/workers/{id}/revoke", Role: RoleAdmin, Handler: s.revokeWorker},
		{Method: "POST", Path: "/worker-tokens", Role: RoleAdmin, Handler: s.createJoinToken},

		{Method: "GET", Path: "/admin/backup", Role: RoleAdmin, Stream: true, Handler: s.backup},
		{Method: "POST", Path: "/admin/verify", Role: RoleAdmin, Stream: true, Handler: s.verify},

		{Method: "POST", Path: "/tokens", Role: RoleAdmin, Handler: s.createToken},
		{Method: "GET", Path: "/tokens", Role: RoleAdmin, Handler: s.listTokens},
		{Method: "DELETE", Path: "/tokens/{id}", Role: RoleAdmin, Handler: s.revokeToken},
	}
}

// AllowedMethods lists what a path takes. It is only asked once the method has failed to
// match, as the catch-all route would otherwise turn every 405 into a 404.
func AllowedMethods(routes []route, path string) []string {
	var out []string
	for i := range routes {
		if pathMatches(routes[i].Path, path) && !slices.Contains(out, routes[i].Method) {
			out = append(out, routes[i].Method)
		}
	}
	slices.Sort(out)
	return out
}

func pathMatches(pattern, path string) bool {
	want, have := strings.Split(pattern, "/"), strings.Split(path, "/")
	if len(want) != len(have) {
		return false
	}
	for i := range want {
		if !strings.HasPrefix(want[i], "{") && want[i] != have[i] {
			return false
		}
	}
	return true
}

func (s *server) metricsRole() string {
	if s.o.Config != nil && s.o.Config.API.MetricsRequireAuth {
		return RoleViewer
	}
	return ""
}

// asAppError turns anything a handler returns into what the client may see.
func asAppError(err error) *apperr.Error {
	if e, ok := apperr.As(err); ok {
		return e
	}
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		return apperr.New(apperr.PayloadTooLarge, "ERR_BODY_TOO_LARGE", fmt.Sprintf("the request body exceeds %d bytes", tooLarge.Limit))
	case errors.Is(err, store.ErrNotFound):
		return apperr.New(apperr.NotFound, "ERR_NOT_FOUND", "not found")
	case errors.Is(err, store.ErrConflict):
		return apperr.New(apperr.Conflict, "ERR_CONFLICT", "changed by someone else: read it again and retry")
	}
	return apperr.Wrap(err, apperr.Internal, apperr.CodeInternal, "internal error")
}
