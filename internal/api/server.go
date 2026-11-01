// Package api is the public HTTP API. Phase 2 serves health and readiness; the
// remaining resources are added in Phase 8.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/clock"
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

// NewHandler builds the public API handler.
func NewHandler(o Options) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, o.Logger, http.StatusOK, HealthResponse{Status: "ok", Version: o.Version})
	})
	mux.HandleFunc("GET /api/v1/ready", func(w http.ResponseWriter, r *http.Request) {
		resp := summarize(o.Ready(r.Context()))
		status := http.StatusOK
		if !resp.Ready {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, o.Logger, status, resp)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, o.Logger, apperr.New(apperr.NotFound, "ERR_NOT_FOUND", "no such endpoint"))
	})
	return middleware(o, mux)
}

func middleware(o Options, next http.Handler) http.Handler {
	slots := make(chan struct{}, o.MaxConcurrent)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := requestID(r.Header.Get(requestIDHeader))
		w.Header().Set(requestIDHeader, id)
		r = r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id))

		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			w.Header().Set("Retry-After", "1")
			writeError(w, r, o.Logger, apperr.New(apperr.Unavailable, "ERR_OVERLOADED", "too many concurrent requests"))
			return
		}

		start := o.Clock.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if p := recover(); p != nil {
				o.Logger.Error("panic in request handler", "request_id", id, "panic", p, "stack", string(debug.Stack()))
				if !rec.wrote {
					writeError(rec, r, o.Logger, apperr.New(apperr.Internal, apperr.CodeInternal, "internal error"))
				}
			}
			o.Logger.Info("http request", "request_id", id, "method", r.Method, "route", r.Pattern,
				"status", rec.status, "duration_ms", o.Clock.Now().Sub(start)/time.Millisecond)
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
