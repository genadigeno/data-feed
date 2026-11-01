package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/ids"
	"github.com/genadigeno/data-feed/internal/store"
)

// SessionCookie holds the token for the web interface. It is HttpOnly, so scripts can't read it.
const SessionCookie = "datafeed_token"

// TokenPrefix starts every API token, which makes one easy to recognise in a leak.
const TokenPrefix = "dft_"

const (
	limiterIdleTTL = 10 * time.Minute
	limiterMax     = 10000
)

// Principal is who is calling.
type Principal struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Role string `json:"role"`
}

type principalKey struct{}

func principalOf(r *http.Request) Principal {
	p, _ := r.Context().Value(principalKey{}).(Principal)
	return p
}

// RoleAllows reports whether a role may do what needs another.
func RoleAllows(have, need string) bool {
	rank := func(role string) int {
		return slices.Index([]string{RoleViewer, RoleOperator, RoleAdmin}, role)
	}
	return rank(have) >= 0 && rank(have) >= rank(need)
}

// ValidRole reports whether a role exists.
func ValidRole(role string) bool { return RoleAllows(role, RoleViewer) }

// HashToken is how tokens are stored and looked up.
func HashToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// NewToken makes a token.
func NewToken() string { return TokenPrefix + ids.Secret() }

// TokenUsable says whether a token row still works.
func TokenUsable(t *store.APIToken, now time.Time) bool {
	return t.RevokedAt == nil && (t.ExpiresAt == nil || now.Before(*t.ExpiresAt))
}

// UIHeader marks a request as made by the web interface's own script. A cookie travels with
// any request the browser sends, but another site's page cannot set this header on one.
const UIHeader = "X-Datafeed-UI"

// bearer reads a token from the Authorization header or, for the web interface, its cookie.
func bearer(r *http.Request) (token string, fromCookie bool) {
	if token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		return strings.TrimSpace(token), false
	}
	if c, err := r.Cookie(SessionCookie); err == nil {
		return c.Value, true
	}
	return "", false
}

// CrossSiteRisk reports a request a cookie alone must not be enough for: one that changes
// something, and doesn't show it came from the interface.
func CrossSiteRisk(method string, fromCookie bool, uiHeader string) bool {
	if !fromCookie || uiHeader != "" {
		return false
	}
	return method != http.MethodGet && method != http.MethodHead && method != http.MethodOptions
}

// login checks a token and keeps it in a cookie scripts can't read. Tokens are long and
// random, so the rate limit here is only there to keep this from being a free load test.
func (s *server) login(w http.ResponseWriter, r *http.Request) error {
	if ok, _ := s.limiters.allow("login", s.o.Clock.Now()); !ok {
		return apperr.New(apperr.ResourceExhausted, "ERR_RATE_LIMITED", "too many attempts")
	}
	var req struct {
		Token string `json:"token"`
	}
	if err := readJSON(r, &req); err != nil {
		return err
	}
	var row *store.APIToken
	err := s.o.DB.Read(r.Context(), func(ctx context.Context, tx store.Tx) error {
		var err error
		row, err = tx.FindAPIToken(ctx, HashToken(strings.TrimSpace(req.Token)))
		return err
	})
	if errors.Is(err, store.ErrNotFound) || (err == nil && !TokenUsable(row, s.o.Clock.Now())) {
		return apperr.New(apperr.Unauthenticated, "ERR_UNAUTHENTICATED", "that token is not valid")
	}
	if err != nil {
		return err
	}
	http.SetCookie(w, sessionCookie(strings.TrimSpace(req.Token), s.o.Config.Auth.WebSessionTTL, r.TLS != nil))
	s.writeJSON(w, http.StatusOK, Principal{ID: row.ID, Name: row.Name, Role: row.Role})
	return nil
}

func (s *server) logout(w http.ResponseWriter, r *http.Request) error {
	http.SetCookie(w, sessionCookie("", -time.Hour, r.TLS != nil))
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// sessionCookie is out of reach of scripts and other sites.
func sessionCookie(value string, ttl time.Duration, secure bool) *http.Cookie {
	return &http.Cookie{
		Name: SessionCookie, Value: value, Path: "/", MaxAge: int(ttl.Seconds()), HttpOnly: true, Secure: secure, SameSite: http.SameSiteStrictMode,
	}
}

func unauthenticated() error {
	return apperr.New(apperr.Unauthenticated, "ERR_UNAUTHENTICATED", "a valid API token is required")
}

// authenticate finds the caller.
func (s *server) authenticate(r *http.Request) (Principal, error) {
	if s.o.Config.Auth.Mode != "token" {
		return Principal{ID: "local", Name: "local", Role: RoleAdmin}, nil
	}
	token, fromCookie := bearer(r)
	if token == "" {
		return Principal{}, unauthenticated()
	}
	if CrossSiteRisk(r.Method, fromCookie, r.Header.Get(UIHeader)) {
		return Principal{}, apperr.New(apperr.PermissionDenied, "ERR_CROSS_SITE", "this request needs a token, or the "+UIHeader+" header")
	}
	var row *store.APIToken
	err := s.o.DB.Read(r.Context(), func(ctx context.Context, tx store.Tx) error {
		var err error
		row, err = tx.FindAPIToken(ctx, HashToken(token))
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return Principal{}, unauthenticated()
	}
	if err != nil {
		return Principal{}, err
	}
	if !TokenUsable(row, s.o.Clock.Now()) {
		return Principal{}, unauthenticated()
	}
	return Principal{ID: row.ID, Name: row.Name, Role: row.Role}, nil
}

// EnsureBootstrapToken creates the first admin token from configuration, when there are none.
func EnsureBootstrapToken(ctx context.Context, db store.DB, secret string, now time.Time) error {
	if secret == "" {
		return nil
	}
	return db.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		count, err := tx.CountAPITokens(ctx)
		if err != nil || count > 0 {
			return err
		}
		return tx.InsertAPIToken(ctx, &store.APIToken{ID: ids.New("tok"), Name: "bootstrap", Hash: HashToken(secret), Role: RoleAdmin, CreatedAt: now})
	})
}

// limiters keeps one rate limiter per caller.
type limiters struct {
	mu     sync.Mutex
	limit  rate.Limit
	burst  int
	byName map[string]*limiterEntry
}

type limiterEntry struct {
	lim  *rate.Limiter
	seen time.Time
}

func newLimiters(perSecond float64, burst int) *limiters {
	return &limiters{limit: rate.Limit(perSecond), burst: burst, byName: map[string]*limiterEntry{}}
}

// allow reports whether a caller may proceed now, and if not, how long until it may.
func (l *limiters) allow(name string, now time.Time) (bool, time.Duration) {
	if l.limit <= 0 {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.byName[name]
	if e == nil {
		if len(l.byName) >= limiterMax {
			for key, entry := range l.byName {
				if now.Sub(entry.seen) > limiterIdleTTL {
					delete(l.byName, key)
				}
			}
		}
		e = &limiterEntry{lim: rate.NewLimiter(l.limit, l.burst)}
		l.byName[name] = e
	}
	e.seen = now
	res := e.lim.ReserveN(now, 1)
	delay := res.DelayFrom(now)
	if delay > 0 {
		res.CancelAt(now)
		return false, delay
	}
	return true, 0
}

// Idempotency outcomes.
const (
	idempotencyRun      = iota // first time: run it
	idempotencyReplay          // seen with the same body: answer as before
	idempotencyMismatch        // seen with another body: a client bug
)

// IdempotencyDecision decides what a repeated key means.
func IdempotencyDecision(rec *store.IdempotencyRecord, hash []byte, now time.Time, ttl time.Duration) int {
	if rec == nil || now.Sub(rec.CreatedAt) >= ttl {
		return idempotencyRun
	}
	if !bytes.Equal(rec.RequestHash, hash) {
		return idempotencyMismatch
	}
	return idempotencyReplay
}

// keyLocks serializes requests with one key, so concurrent retries can't both run.
type keyLocks struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

type keyLock struct {
	mu   sync.Mutex
	uses int
}

func (k *keyLocks) lock(key string) func() {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = map[string]*keyLock{}
	}
	l := k.locks[key]
	if l == nil {
		l = &keyLock{}
		k.locks[key] = l
	}
	l.uses++
	k.mu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		k.mu.Lock()
		defer k.mu.Unlock()
		if l.uses--; l.uses == 0 {
			delete(k.locks, key)
		}
	}
}

// captureWriter keeps a response, for storing.
type captureWriter struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (c *captureWriter) WriteHeader(code int) {
	c.status = code
	c.ResponseWriter.WriteHeader(code)
}

func (c *captureWriter) Write(b []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	c.body.Write(b)
	return c.ResponseWriter.Write(b)
}

// idempotent wraps a route so a repeated Idempotency-Key gets the first answer.
func (s *server) idempotent(next handlerFunc) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request) error {
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			return next(w, r)
		}
		if len(key) > 255 {
			return apperr.New(apperr.InvalidArgument, "ERR_BAD_REQUEST", "Idempotency-Key is too long")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		sum := sha256.New()
		_, _ = io.WriteString(sum, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery+"\n")
		sum.Write(body)
		hash := sum.Sum(nil)

		principal := principalOf(r).ID
		defer s.keys.lock(principal + "\x00" + key)()

		var rec *store.IdempotencyRecord
		err = s.o.DB.Read(r.Context(), func(ctx context.Context, tx store.Tx) error {
			var err error
			rec, err = tx.GetIdempotency(ctx, principal, key)
			if errors.Is(err, store.ErrNotFound) {
				rec = nil
				return nil
			}
			return err
		})
		if err != nil {
			return err
		}
		now := s.o.Clock.Now()
		switch IdempotencyDecision(rec, hash, now, s.o.Config.API.IdempotencyTTL) {
		case idempotencyMismatch:
			return apperr.Validation("ERR_IDEMPOTENCY_MISMATCH", "this Idempotency-Key was used with a different request", []apperr.Problem{
				{Path: "Idempotency-Key", Code: "ERR_IDEMPOTENCY_MISMATCH", Message: "already used with a different request"},
			})
		case idempotencyReplay:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotency-Replayed", "true")
			w.WriteHeader(rec.Status)
			_, err := io.WriteString(w, rec.ResponseJSON)
			return err
		}

		capture := &captureWriter{ResponseWriter: w}
		if err := next(capture, r); err != nil {
			return err // a failure is not remembered: the retry should run
		}
		// Only what has a JSON body can be replayed.
		if capture.status >= 200 && capture.status < 300 && json.Valid(capture.body.Bytes()) {
			err := s.o.DB.Write(context.WithoutCancel(r.Context()), func(ctx context.Context, tx store.Tx) error {
				return tx.PutIdempotency(ctx, &store.IdempotencyRecord{
					Principal: principal, Key: key, RequestHash: hash, Status: capture.status, ResponseJSON: capture.body.String(), CreatedAt: now,
				})
			})
			if err != nil {
				s.o.Logger.Warn("store idempotency record", "err", err)
			}
		}
		return nil
	}
}
