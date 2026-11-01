// Package api is the public HTTP API. Handlers parse, call a service and map the result;
// decisions stay in the services. See docs/03-interfaces/api-reference.md.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"regexp"
	"runtime/debug"
	"strings"
	"sync/atomic"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/artifact"
	"github.com/genadigeno/data-feed/internal/catalog"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/coord"
	"github.com/genadigeno/data-feed/internal/hub"
	"github.com/genadigeno/data-feed/internal/obs"
	"github.com/genadigeno/data-feed/internal/session"
	"github.com/genadigeno/data-feed/internal/store"
)

const requestIDHeader = "X-Request-Id"

// Check is one readiness condition.
type Check struct {
	Name    string `json:"name"`
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// Options configures the handler.
type Options struct {
	Logger        *slog.Logger
	Clock         clock.Clock
	Version       string
	Ready         func(ctx context.Context) []Check
	MaxConcurrent int

	Config    *config.Config
	DB        store.DB
	Core      *coord.Core
	Sessions  *session.Service
	Catalogs  *catalog.Service
	Artifacts *artifact.Store
	Logs      *obs.SessionLogs
	Hub       *hub.Hub
	// Done closes when the server starts shutting down, which ends live update streams.
	Done <-chan struct{}
	// Backup writes a consistent copy of the database to a new file.
	Backup func(ctx context.Context, dest string) error
	// Extra, if set, handles what no API route matches: the web interface.
	Extra http.Handler
}

// HealthResponse is the body of GET /api/v1/health.
type HealthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

// ReadyResponse is the body of GET /api/v1/ready.
type ReadyResponse struct {
	Ready  bool    `json:"ready"`
	Checks []Check `json:"checks"`
}

// summarize decides readiness: every check must pass, and there must be at least one.
func summarize(checks []Check) ReadyResponse {
	r := ReadyResponse{Ready: len(checks) > 0, Checks: checks}
	for _, c := range checks {
		if !c.OK {
			r.Ready = false
		}
	}
	return r
}

// maxHeavyRequests bounds requests that can load a whole catalog or compile a schema, which
// even a viewer can ask for, while the rest of the API stays quick.
const maxHeavyRequests = 4

// server holds what handlers share.
type server struct {
	o          Options
	limiters   *limiters
	keys       keyLocks
	sseClients atomic.Int64
	requests   statusCounts
	slots      chan struct{}
	verifying  chan struct{}
	heavy      chan struct{}

	feeds       feedCache
	feedStreams atomic.Int64
	feedMemory  atomic.Int64 // bytes reads from feeds may be holding
}

func (s *server) writeJSON(w http.ResponseWriter, status int, body any) {
	writeJSON(w, s.o.Logger, status, body)
}

// NewHandler builds the public API handler.
func NewHandler(o Options) http.Handler {
	s := &server{
		o: o, slots: make(chan struct{}, o.MaxConcurrent), verifying: make(chan struct{}, 1), heavy: make(chan struct{}, maxHeavyRequests),
		limiters: newLimiters(o.Config.API.RateLimit.RequestsPerSecond, o.Config.API.RateLimit.Burst),
	}
	mux := http.NewServeMux()
	for _, rt := range s.routes() {
		mux.Handle(rt.Method+" "+prefix+rt.Path, s.wrap(rt))
	}
	routes := s.routes()
	mux.HandleFunc(prefix+"/", func(w http.ResponseWriter, r *http.Request) {
		if allowed := AllowedMethods(routes, strings.TrimPrefix(r.URL.Path, prefix)); len(allowed) > 0 {
			w.Header().Set("Allow", strings.Join(allowed, ", "))
			writeError(w, r, o.Logger, apperr.New(apperr.MethodNotAllowed, "ERR_METHOD_NOT_ALLOWED", "this endpoint does not take "+r.Method))
			return
		}
		writeError(w, r, o.Logger, apperr.New(apperr.NotFound, "ERR_NOT_FOUND", "no such endpoint"))
	})
	if o.Config.Diagnostics.Pprof {
		// Profiles reveal memory contents, so they are for admins only.
		for pattern, h := range map[string]http.HandlerFunc{
			"/debug/pprof/": pprof.Index, "/debug/pprof/cmdline": pprof.Cmdline, "/debug/pprof/profile": pprof.Profile,
			"/debug/pprof/symbol": pprof.Symbol, "/debug/pprof/trace": pprof.Trace,
		} {
			mux.Handle("GET "+pattern, s.wrap(route{Role: RoleAdmin, Handler: func(w http.ResponseWriter, r *http.Request) error {
				noWriteDeadline(w)
				h(w, r)
				return nil
			}}))
		}
	}
	if o.Extra != nil {
		mux.Handle("/", o.Extra)
	} else {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			writeError(w, r, o.Logger, apperr.New(apperr.NotFound, "ERR_NOT_FOUND", "no such endpoint"))
		})
	}
	return s.middleware(mux)
}

// wrap applies one route's rules, in order: who is calling, how often, what they may do,
// and how much they may send.
func (s *server) wrap(rt route) http.Handler {
	handler := rt.Handler
	if rt.Heavy {
		next := handler
		handler = func(w http.ResponseWriter, r *http.Request) error {
			select {
			case s.heavy <- struct{}{}:
				defer func() { <-s.heavy }()
			case <-r.Context().Done():
				return r.Context().Err()
			}
			return next(w, r)
		}
	}
	if rt.Idempotent {
		handler = s.idempotent(handler)
	}
	limit := int64(s.o.Config.API.MaxBodyBytes)
	if rt.Import {
		limit = int64(s.o.Config.API.MaxImportBytes)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fail := func(err error) {
			e := asAppError(err)
			if e.Kind == apperr.ResourceExhausted && e.Code == "ERR_RATE_LIMITED" {
				w.Header().Set("Retry-After", "1")
			}
			writeError(w, r, s.o.Logger, e)
		}
		if rt.Role != "" {
			p, err := s.authenticate(r)
			if err != nil {
				if e, ok := apperr.As(err); ok && e.Kind == apperr.Unauthenticated {
					w.Header().Set("WWW-Authenticate", "Bearer")
				}
				fail(err)
				return
			}
			if ok, _ := s.limiters.allow(p.ID, s.o.Clock.Now()); !ok {
				fail(apperr.New(apperr.ResourceExhausted, "ERR_RATE_LIMITED", "too many requests"))
				return
			}
			if !RoleAllows(p.Role, rt.Role) {
				fail(apperr.New(apperr.PermissionDenied, "ERR_FORBIDDEN", "this needs the "+rt.Role+" role"))
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), principalKey{}, p))
		}
		// A declared size can be refused before reading any of it, which spares the upload.
		if r.ContentLength > limit {
			fail(&http.MaxBytesError{Limit: limit})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		if err := handler(w, r); err != nil {
			fail(err)
		}
	})
}

// isLongLived spots responses that may stay open for a long time. They must not use up
// the slots that keep ordinary requests answered quickly.
func isLongLived(path string) bool {
	if strings.Contains(path, "/artifacts/") {
		return strings.HasSuffix(path, "/merged") || strings.Contains(path, "/parts/")
	}
	if strings.HasPrefix(path, prefix+"/feeds/") && strings.HasSuffix(path, "/stream") {
		return true
	}
	return strings.HasSuffix(path, prefix+"/events") || strings.HasPrefix(path, prefix+"/admin/") || strings.HasPrefix(path, "/debug/pprof/")
}

func (s *server) middleware(next http.Handler) http.Handler {
	o := s.o
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := requestID(r.Header.Get(requestIDHeader))
		w.Header().Set(requestIDHeader, id)
		r = r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id))

		if !HostAllowed(o.Config.Server.Listen, r.Host) || CrossOrigin(r.Method, r.Header.Get("Origin"), r.Host) {
			writeError(w, r, o.Logger, apperr.New(apperr.PermissionDenied, "ERR_CROSS_SITE", "requests from other sites are refused"))
			return
		}
		if !isLongLived(r.URL.Path) {
			select {
			case s.slots <- struct{}{}:
				defer func() { <-s.slots }()
			default:
				w.Header().Set("Retry-After", "1")
				writeError(w, r, o.Logger, apperr.New(apperr.Unavailable, "ERR_OVERLOADED", "too many concurrent requests"))
				return
			}
		}

		start := o.Clock.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			p := recover()
			if p == http.ErrAbortHandler {
				panic(p) // deliberately cutting the connection
			}
			if p != nil {
				o.Logger.Error("panic in request handler", "request_id", id, "panic", p, "stack", string(debug.Stack()))
				if !rec.wrote {
					writeError(rec, r, o.Logger, apperr.New(apperr.Internal, apperr.CodeInternal, "internal error"))
				}
			}
			s.requests.add(rec.status)
			o.Logger.Info("http request", "request_id", id, "method", r.Method, "route", r.Pattern,
				"status", rec.status, "duration_ms", o.Clock.Now().Sub(start).Milliseconds())
		}()
		next.ServeHTTP(rec, r)
	})
}

type requestIDKey struct{}

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// requestID keeps a well-formed caller-supplied ID and otherwise generates one, so
// untrusted header values never reach the logs.
func requestID(supplied string) string {
	if validRequestID.MatchString(supplied) {
		return supplied
	}
	var b [12]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error
	return hex.EncodeToString(b[:])
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

// Unwrap lets http.ResponseController reach the real writer, for flushing and deadlines.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (s *statusRecorder) WriteHeader(code int) {
	s.status, s.wrote = code, true
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wrote = true
	return s.ResponseWriter.Write(b)
}

// ErrorBody is the JSON form of every error response.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes an error to clients.
type ErrorDetail struct {
	Code      string           `json:"code"`
	Message   string           `json:"message"`
	Problems  []apperr.Problem `json:"problems,omitempty"`
	Details   map[string]any   `json:"details,omitempty"`
	RequestID string           `json:"request_id,omitempty"`
}

// toErrorBody exposes only what is safe. Internal errors never reveal their message
// or cause; the request ID links the client's report to the server log.
func toErrorBody(e *apperr.Error, requestID string) ErrorBody {
	d := ErrorDetail{Code: e.Code, Message: e.Message, Problems: e.Problems, Details: e.Details, RequestID: requestID}
	if e.Kind == apperr.Internal {
		d = ErrorDetail{Code: apperr.CodeInternal, Message: "internal error", RequestID: requestID}
	}
	return ErrorBody{Error: d}
}

func writeError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, e *apperr.Error) {
	id, _ := r.Context().Value(requestIDKey{}).(string)
	if e.Kind == apperr.Internal {
		logger.Error("request failed", "request_id", id, "err", e)
	}
	writeJSON(w, logger, apperr.HTTPStatusFor(e), toErrorBody(e, id))
}

func writeJSON(w http.ResponseWriter, logger *slog.Logger, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		logger.Debug("write response", "err", err) // the client went away
	}
}
