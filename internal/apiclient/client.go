// Package apiclient is the typed Go client for the public API, used by the CLI.
package apiclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/genadigeno/data-feed/internal/api"
	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/obs"
	"github.com/genadigeno/data-feed/internal/vocab"
)

const (
	prefix           = "/api/v1"
	maxResponseBytes = 256 << 20 // catalogs can be large
	maxSSELineBytes  = 1 << 20
)

// Client calls one Coordinator.
type Client struct {
	base    string
	token   string
	timeout time.Duration
	http    *http.Client
}

// New returns a client for baseURL such as http://127.0.0.1:8080. The timeout covers
// ordinary calls; downloads and live streams last as long as their context.
func New(baseURL string, timeout time.Duration) *Client {
	return &Client{base: strings.TrimRight(baseURL, "/"), timeout: timeout, http: &http.Client{}}
}

// WithToken sets the API token.
func (c *Client) WithToken(token string) *Client {
	c.token = token
	return c
}

// statusKinds restores an error's kind from its status, which sets the CLI's exit code.
var statusKinds = map[int]apperr.Kind{
	http.StatusBadRequest:            apperr.InvalidArgument,
	http.StatusUnprocessableEntity:   apperr.InvalidArgument,
	http.StatusUnauthorized:          apperr.Unauthenticated,
	http.StatusForbidden:             apperr.PermissionDenied,
	http.StatusNotFound:              apperr.NotFound,
	http.StatusMethodNotAllowed:      apperr.MethodNotAllowed,
	http.StatusConflict:              apperr.Conflict,
	http.StatusPreconditionFailed:    apperr.PreconditionFailed,
	http.StatusRequestEntityTooLarge: apperr.PayloadTooLarge,
	http.StatusTooManyRequests:       apperr.ResourceExhausted,
	http.StatusServiceUnavailable:    apperr.Unavailable,
}

// DecodeError turns an error response into what the server meant.
func DecodeError(status int, body []byte) error {
	var parsed api.ErrorBody
	kind, known := statusKinds[status]
	if !known {
		kind = apperr.Internal
	}
	if json.Unmarshal(body, &parsed) != nil || parsed.Error.Code == "" {
		return apperr.New(kind, "ERR_SERVER_RESPONSE", fmt.Sprintf("unexpected status %d", status))
	}
	e := parsed.Error
	return &apperr.Error{Kind: kind, Code: e.Code, Message: e.Message, Problems: e.Problems, Details: e.Details, Retryable: kind == apperr.Unavailable}
}

// request is one call.
type request struct {
	method, path string
	query        url.Values
	body         []byte
	contentType  string
	headers      map[string]string
	stream       bool
}

func (c *Client) send(ctx context.Context, q request) (*http.Response, context.CancelFunc, error) {
	cancel := func() {}
	if !q.stream && c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
	}
	target := c.base + prefix + q.path
	if len(q.query) > 0 {
		target += "?" + q.query.Encode()
	}
	var body io.Reader
	if q.body != nil {
		body = bytes.NewReader(q.body)
	}
	req, err := http.NewRequestWithContext(ctx, q.method, target, body)
	if err != nil {
		cancel()
		return nil, nil, apperr.Wrap(err, apperr.InvalidArgument, "ERR_SERVER_URL", "invalid server URL "+c.base)
	}
	if q.body != nil {
		ct := q.contentType
		if ct == "" {
			ct = "application/json"
		}
		req.Header.Set("Content-Type", ct)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	for k, v := range q.headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		cancel()
		if errors.Is(err, context.Canceled) {
			return nil, nil, apperr.Wrap(err, apperr.Canceled, "ERR_INTERRUPTED", "interrupted")
		}
		return nil, nil, apperr.Wrap(err, apperr.Unavailable, "ERR_SERVER_UNAVAILABLE", "cannot reach the server at "+c.base)
	}
	if resp.StatusCode >= 400 {
		defer cancel()
		defer resp.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return nil, nil, DecodeError(resp.StatusCode, data)
	}
	return resp, cancel, nil
}

// call does a request and decodes a JSON answer into out, if given.
func (c *Client) call(ctx context.Context, q request, out any) (http.Header, error) {
	resp, cancel, err := c.send(ctx, q)
	if err != nil {
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.Header, nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(out); err != nil {
		return nil, apperr.Wrap(err, apperr.Unavailable, "ERR_SERVER_RESPONSE", "unreadable response from "+c.base)
	}
	return resp.Header, nil
}

func jsonBody(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("encode request: %v", err)) // only ever given plain data
	}
	return data
}

func get[T any](ctx context.Context, c *Client, path string, query url.Values) (T, error) {
	var out T
	_, err := c.call(ctx, request{method: http.MethodGet, path: path, query: query}, &out)
	return out, err
}

func post[T any](ctx context.Context, c *Client, path string, body any) (T, error) {
	var out T
	q := request{method: http.MethodPost, path: path}
	if body != nil {
		q.body = jsonBody(body)
	}
	_, err := c.call(ctx, q, &out)
	return out, err
}

func (c *Client) del(ctx context.Context, path string, query url.Values) error {
	_, err := c.call(ctx, request{method: http.MethodDelete, path: path, query: query}, nil)
	return err
}

// Health calls GET /health.
func (c *Client) Health(ctx context.Context) (api.HealthResponse, error) {
	return get[api.HealthResponse](ctx, c, "/health", nil)
}

// Ready calls GET /ready. Not being ready is a normal answer, not an error.
func (c *Client) Ready(ctx context.Context) (api.ReadyResponse, error) {
	var out api.ReadyResponse
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+prefix+"/ready", nil)
	if err != nil {
		return out, apperr.Wrap(err, apperr.InvalidArgument, "ERR_SERVER_URL", "invalid server URL "+c.base)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return out, apperr.Wrap(err, apperr.Unavailable, "ERR_SERVER_UNAVAILABLE", "cannot reach the server at "+c.base)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
		return out, apperr.New(apperr.Unavailable, "ERR_SERVER_RESPONSE", fmt.Sprintf("unexpected status %d", resp.StatusCode))
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return out, apperr.Wrap(err, apperr.Unavailable, "ERR_SERVER_RESPONSE", "unreadable response from "+c.base)
	}
	return out, nil
}

// WhoAmI identifies the token.
func (c *Client) WhoAmI(ctx context.Context) (api.Principal, error) {
	return get[api.Principal](ctx, c, "/whoami", nil)
}

// Dashboard reads the overview.
func (c *Client) Dashboard(ctx context.Context) (api.DashboardDTO, error) {
	return get[api.DashboardDTO](ctx, c, "/dashboard", nil)
}

// Sessions

// ListOptions select a page.
type ListOptions struct {
	Limit  int
	Cursor string
}

func (o ListOptions) values() url.Values {
	q := url.Values{}
	if o.Limit > 0 {
		q.Set("limit", strconv.Itoa(o.Limit))
	}
	if o.Cursor != "" {
		q.Set("cursor", o.Cursor)
	}
	return q
}

// CreateSession creates a session from a specification in YAML or JSON.
func (c *Client) CreateSession(ctx context.Context, spec []byte, start bool, idempotencyKey string) (api.SessionDTO, error) {
	var out api.SessionDTO
	q := request{method: http.MethodPost, path: "/sessions", body: spec, contentType: "application/yaml"}
	if start {
		q.query = url.Values{"start": {"true"}}
	}
	if idempotencyKey != "" {
		q.headers = map[string]string{"Idempotency-Key": idempotencyKey}
	}
	_, err := c.call(ctx, q, &out)
	return out, err
}

// ValidateSession checks a specification.
func (c *Client) ValidateSession(ctx context.Context, spec []byte) (map[string]any, error) {
	var out map[string]any
	_, err := c.call(ctx, request{method: http.MethodPost, path: "/sessions/validate", body: spec, contentType: "application/yaml"}, &out)
	return out, err
}

// ListSessions lists sessions.
func (c *Client) ListSessions(ctx context.Context, states, mode, query, sort string, o ListOptions) (api.PageDTO[api.SessionDTO], error) {
	q := o.values()
	for k, v := range map[string]string{"state": states, "mode": mode, "q": query, "sort": sort} {
		if v != "" {
			q.Set(k, v)
		}
	}
	return get[api.PageDTO[api.SessionDTO]](ctx, c, "/sessions", q)
}

// GetSession reads a session.
func (c *Client) GetSession(ctx context.Context, id string, withSpec bool) (api.SessionDTO, error) {
	var q url.Values
	if withSpec {
		q = url.Values{"include": {"spec"}}
	}
	return get[api.SessionDTO](ctx, c, "/sessions/"+url.PathEscape(id), q)
}

// ConfigureSession replaces a session's specification, based on its current version.
func (c *Client) ConfigureSession(ctx context.Context, id string, spec []byte) (api.SessionDTO, error) {
	current, err := c.GetSession(ctx, id, false)
	if err != nil {
		return current, err
	}
	var out api.SessionDTO
	_, err = c.call(ctx, request{
		method: http.MethodPut, path: "/sessions/" + url.PathEscape(id) + "/config", body: spec, contentType: "application/yaml",
		headers: map[string]string{"If-Match": strconv.Quote(strconv.FormatInt(current.Version, 10))},
	}, &out)
	return out, err
}

// SessionAction performs start, pause, resume, clone or retry.
func (c *Client) SessionAction(ctx context.Context, id, action string) (api.SessionDTO, error) {
	return post[api.SessionDTO](ctx, c, "/sessions/"+url.PathEscape(id)+"/"+action, nil)
}

// StopSession stops a session.
func (c *Client) StopSession(ctx context.Context, id string, immediate bool) (api.SessionDTO, error) {
	mode := "graceful"
	if immediate {
		mode = "immediate"
	}
	return post[api.SessionDTO](ctx, c, "/sessions/"+url.PathEscape(id)+"/stop", map[string]string{"mode": mode})
}

// DeleteSession deletes a session.
func (c *Client) DeleteSession(ctx context.Context, id string, purge bool) error {
	var q url.Values
	if purge {
		q = url.Values{"purge_artifacts": {"true"}}
	}
	return c.del(ctx, "/sessions/"+url.PathEscape(id), q)
}

// Progress is a session's current numbers.
type Progress struct {
	State    string           `json:"state"`
	Progress api.ProgressDTO  `json:"progress"`
	Rates    api.RatesDTO     `json:"rates"`
	Units    *api.UnitsDTO    `json:"units"`
	Pressure *api.PressureDTO `json:"pressure"`
	Errors   api.ErrorsDTO    `json:"errors"`
}

// SessionProgress reads a session's progress.
func (c *Client) SessionProgress(ctx context.Context, id string) (Progress, error) {
	return get[Progress](ctx, c, "/sessions/"+url.PathEscape(id)+"/progress", nil)
}

// MetricsSample is one bucket of throughput history.
type MetricsSample struct {
	At      time.Time `json:"at"`
	Records string    `json:"records"`
	Bytes   string    `json:"bytes"`
}

// Metrics is throughput history.
type Metrics struct {
	BucketSeconds int             `json:"bucket_seconds"`
	Samples       []MetricsSample `json:"samples"`
}

// SessionMetrics reads throughput history.
func (c *Client) SessionMetrics(ctx context.Context, id, history string) (Metrics, error) {
	var q url.Values
	if history != "" {
		q = url.Values{"history": {history}}
	}
	return get[Metrics](ctx, c, "/sessions/"+url.PathEscape(id)+"/metrics", q)
}

// Items wraps a plain list.
type Items[T any] struct {
	Items []T `json:"items"`
}

// SessionLogs reads a session's log.
func (c *Client) SessionLogs(ctx context.Context, id, level string, since time.Time, limit int) ([]obs.LogEntry, error) {
	q := url.Values{}
	if level != "" {
		q.Set("level", level)
	}
	if !since.IsZero() {
		q.Set("since", since.UTC().Format(time.RFC3339Nano))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	out, err := get[Items[obs.LogEntry]](ctx, c, "/sessions/"+url.PathEscape(id)+"/logs", q)
	return out.Items, err
}

// SessionEvent is a lifecycle transition.
type SessionEvent struct {
	At     time.Time `json:"at"`
	From   string    `json:"from"`
	To     string    `json:"to"`
	Event  string    `json:"event"`
	Actor  string    `json:"actor"`
	Reason string    `json:"reason"`
}

// SessionEvents reads lifecycle history.
func (c *Client) SessionEvents(ctx context.Context, id string) ([]SessionEvent, error) {
	out, err := get[Items[SessionEvent]](ctx, c, "/sessions/"+url.PathEscape(id)+"/events", nil)
	return out.Items, err
}

// Artifacts is a session's output.
type Artifacts struct {
	Status string            `json:"status"`
	Items  []api.ArtifactDTO `json:"items"`
}

// SessionArtifacts lists output.
func (c *Client) SessionArtifacts(ctx context.Context, id string) (Artifacts, error) {
	return get[Artifacts](ctx, c, "/sessions/"+url.PathEscape(id)+"/artifacts", nil)
}

// PurgeArtifacts deletes a finished session's output.
func (c *Client) PurgeArtifacts(ctx context.Context, id string) error {
	return c.del(ctx, "/sessions/"+url.PathEscape(id)+"/artifacts", nil)
}

// DownloadMerged streams a stream as one document.
func (c *Client) DownloadMerged(ctx context.Context, id, stream string) (io.ReadCloser, error) {
	return c.download(ctx, "/sessions/"+url.PathEscape(id)+"/artifacts/"+url.PathEscape(stream)+"/merged")
}

// DownloadPart streams one part.
func (c *Client) DownloadPart(ctx context.Context, id, stream string, n int) (io.ReadCloser, error) {
	return c.download(ctx, "/sessions/"+url.PathEscape(id)+"/artifacts/"+url.PathEscape(stream)+"/parts/"+strconv.Itoa(n))
}

type streamBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (s streamBody) Close() error {
	defer s.cancel()
	return s.ReadCloser.Close()
}

func (c *Client) download(ctx context.Context, path string) (io.ReadCloser, error) {
	resp, cancel, err := c.send(ctx, request{method: http.MethodGet, path: path, stream: true})
	if err != nil {
		return nil, err
	}
	return streamBody{resp.Body, cancel}, nil
}

// Generate runs small synchronous generation, returning the data.
func (c *Client) Generate(ctx context.Context, spec []byte, table string) (io.ReadCloser, error) {
	q := request{method: http.MethodPost, path: "/generate", body: spec, contentType: "application/yaml", stream: true}
	if table != "" {
		q.query = url.Values{"table": {table}}
	}
	resp, cancel, err := c.send(ctx, q)
	if err != nil {
		return nil, err
	}
	return streamBody{resp.Body, cancel}, nil
}

// SSEEvent is one live update.
type SSEEvent struct {
	Type string
	Data json.RawMessage
}

// ReadSSE reads Server-Sent Events until the stream ends, an error occurs, or handle stops it.
func ReadSSE(r io.Reader, handle func(SSEEvent) (bool, error)) error {
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 64<<10), maxSSELineBytes)
	var event string
	var data bytes.Buffer
	for scan.Scan() {
		line := scan.Text()
		switch {
		case line == "":
			if event != "" || data.Len() > 0 {
				more, err := handle(SSEEvent{Type: event, Data: bytes.Clone(data.Bytes())})
				if err != nil || !more {
					return err
				}
			}
			event = ""
			data.Reset()
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	return scan.Err()
}

// Events streams live updates to handle, for one session or, with "", for all.
func (c *Client) Events(ctx context.Context, session string, handle func(SSEEvent) (bool, error)) error {
	var q url.Values
	if session != "" {
		q = url.Values{"session": {session}}
	}
	resp, cancel, err := c.send(ctx, request{method: http.MethodGet, path: "/events", query: q, stream: true})
	if err != nil {
		return err
	}
	defer cancel()
	defer resp.Body.Close()
	return ReadSSE(resp.Body, handle)
}

// Schemas

// CreateSchema saves a schema; document is YAML or JSON text.
func (c *Client) CreateSchema(ctx context.Context, name string, document []byte) (api.SchemaDTO, error) {
	return post[api.SchemaDTO](ctx, c, "/schemas", map[string]any{"name": name, "document": string(document)})
}

// UpdateSchema saves a new version.
func (c *Client) UpdateSchema(ctx context.Context, id string, document []byte) (api.SchemaDTO, error) {
	var out api.SchemaDTO
	_, err := c.call(ctx, request{method: http.MethodPut, path: "/schemas/" + url.PathEscape(id), body: jsonBody(map[string]any{"document": string(document)})}, &out)
	return out, err
}

// ValidateSchema checks a schema.
func (c *Client) ValidateSchema(ctx context.Context, document []byte) (map[string]any, error) {
	return post[map[string]any](ctx, c, "/schemas/validate", map[string]any{"document": string(document)})
}

// ListSchemas lists schemas.
func (c *Client) ListSchemas(ctx context.Context, o ListOptions) (api.PageDTO[api.SchemaDTO], error) {
	return get[api.PageDTO[api.SchemaDTO]](ctx, c, "/schemas", o.values())
}

// GetSchema reads a schema; version 0 means the latest.
func (c *Client) GetSchema(ctx context.Context, id string, version int) (api.SchemaDTO, error) {
	var q url.Values
	if version > 0 {
		q = url.Values{"version": {strconv.Itoa(version)}}
	}
	return get[api.SchemaDTO](ctx, c, "/schemas/"+url.PathEscape(id), q)
}

// DeleteSchema deletes a schema.
func (c *Client) DeleteSchema(ctx context.Context, id string) error {
	return c.del(ctx, "/schemas/"+url.PathEscape(id), nil)
}

// PreviewSchema generates sample records.
func (c *Client) PreviewSchema(ctx context.Context, id string, body map[string]any) (io.ReadCloser, error) {
	resp, cancel, err := c.send(ctx, request{method: http.MethodPost, path: "/schemas/" + url.PathEscape(id) + "/preview", body: jsonBody(body), stream: true})
	if err != nil {
		return nil, err
	}
	return streamBody{resp.Body, cancel}, nil
}

// Domains and catalogs

// CreateDomain adds a domain.
func (c *Client) CreateDomain(ctx context.Context, name, description, locale string) (api.DomainDTO, error) {
	return post[api.DomainDTO](ctx, c, "/domains", map[string]string{"name": name, "description": description, "locale": locale})
}

// ListDomains lists domains.
func (c *Client) ListDomains(ctx context.Context, o ListOptions) (api.PageDTO[api.DomainDTO], error) {
	return get[api.PageDTO[api.DomainDTO]](ctx, c, "/domains", o.values())
}

// GetDomain reads a domain.
func (c *Client) GetDomain(ctx context.Context, id string) (api.DomainDTO, error) {
	return get[api.DomainDTO](ctx, c, "/domains/"+url.PathEscape(id), nil)
}

// DeleteDomain deletes a domain.
func (c *Client) DeleteDomain(ctx context.Context, id string) error {
	return c.del(ctx, "/domains/"+url.PathEscape(id), nil)
}

// StartVocabularyJob starts a job.
func (c *Client) StartVocabularyJob(ctx context.Context, domain string, body map[string]any) (api.JobDTO, error) {
	return post[api.JobDTO](ctx, c, "/domains/"+url.PathEscape(domain)+"/vocabulary/generate", body)
}

// GetJob reads a job.
func (c *Client) GetJob(ctx context.Context, id string) (api.JobDTO, error) {
	return get[api.JobDTO](ctx, c, "/vocabulary-jobs/"+url.PathEscape(id), nil)
}

// CancelJob cancels a job.
func (c *Client) CancelJob(ctx context.Context, id string) (api.JobDTO, error) {
	return post[api.JobDTO](ctx, c, "/vocabulary-jobs/"+url.PathEscape(id)+"/cancel", nil)
}

// Builtin is a catalog in the binary.
type Builtin struct {
	ID          string `json:"id"`
	Domain      string `json:"domain"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Groups      int    `json:"groups"`
	Values      int    `json:"values"`
}

// ListBuiltins lists the built-in catalogs.
func (c *Client) ListBuiltins(ctx context.Context) ([]Builtin, error) {
	out, err := get[Items[Builtin]](ctx, c, "/builtin-catalogs", nil)
	return out.Items, err
}

// CatalogContent is a version and its values.
type CatalogContent struct {
	Catalog api.CatalogDTO `json:"catalog"`
	Groups  []vocab.Group  `json:"groups"`
}

// CreateCatalog starts an empty draft.
func (c *Client) CreateCatalog(ctx context.Context, domain string) (api.CatalogDTO, error) {
	return post[api.CatalogDTO](ctx, c, "/catalogs", map[string]string{"domain_id": domain})
}

// ListCatalogs lists catalogs.
func (c *Client) ListCatalogs(ctx context.Context, domain string, o ListOptions) (api.PageDTO[api.CatalogDTO], error) {
	q := o.values()
	if domain != "" {
		q.Set("domain", domain)
	}
	return get[api.PageDTO[api.CatalogDTO]](ctx, c, "/catalogs", q)
}

// GetCatalog reads a version; 0 means the latest.
func (c *Client) GetCatalog(ctx context.Context, id string, version int) (CatalogContent, error) {
	var q url.Values
	if version > 0 {
		q = url.Values{"version": {strconv.Itoa(version)}}
	}
	return get[CatalogContent](ctx, c, "/catalogs/"+url.PathEscape(id), q)
}

// CatalogVersions lists versions.
func (c *Client) CatalogVersions(ctx context.Context, id string) ([]api.CatalogDTO, error) {
	out, err := get[Items[api.CatalogDTO]](ctx, c, "/catalogs/"+url.PathEscape(id)+"/versions", nil)
	return out.Items, err
}

// PublishCatalog freezes a draft.
func (c *Client) PublishCatalog(ctx context.Context, id string, version int) (api.CatalogDTO, error) {
	return post[api.CatalogDTO](ctx, c, "/catalogs/"+url.PathEscape(id)+"/versions/"+strconv.Itoa(version)+"/publish", nil)
}

// NewCatalogVersion starts a draft from a version; 0 means the highest.
func (c *Client) NewCatalogVersion(ctx context.Context, id string, from int) (api.CatalogDTO, error) {
	return post[api.CatalogDTO](ctx, c, "/catalogs/"+url.PathEscape(id)+"/versions", map[string]int{"from": from})
}

// CloneCatalog copies a version, or a built-in, to a new catalog.
func (c *Client) CloneCatalog(ctx context.Context, id string, version int, domain string) (api.CatalogDTO, error) {
	return post[api.CatalogDTO](ctx, c, "/catalogs/"+url.PathEscape(id)+"/clone", map[string]any{"version": version, "domain_id": domain})
}

// ExportCatalog renders a version in the portable format.
func (c *Client) ExportCatalog(ctx context.Context, id string, version int) (vocab.Export, error) {
	return get[vocab.Export](ctx, c, "/catalogs/"+url.PathEscape(id)+"/versions/"+strconv.Itoa(version)+"/export", nil)
}

// ImportResult is what an import made.
type ImportResult struct {
	Catalog  api.CatalogDTO `json:"catalog"`
	Rejected map[string]int `json:"rejected"`
}

// ImportCatalog creates a draft from an exported document.
func (c *Client) ImportCatalog(ctx context.Context, domain string, doc json.RawMessage) (ImportResult, error) {
	return post[ImportResult](ctx, c, "/catalogs/import", map[string]any{"domain_id": domain, "document": doc})
}

// AddResult says what happened to submitted values.
type AddResult struct {
	Added    int            `json:"added"`
	Rejected map[string]int `json:"rejected"`
}

func groupPath(id string, version int, key string) string {
	return "/catalogs/" + url.PathEscape(id) + "/versions/" + strconv.Itoa(version) + "/groups/" + url.PathEscape(key)
}

// PutGroup adds or changes a draft group.
func (c *Client) PutGroup(ctx context.Context, id string, version int, g *vocab.Group) error {
	_, err := c.call(ctx, request{method: http.MethodPut, path: groupPath(id, version, g.Key), body: jsonBody(g)}, nil)
	return err
}

// DeleteGroup removes a draft group.
func (c *Client) DeleteGroup(ctx context.Context, id string, version int, key string) error {
	return c.del(ctx, groupPath(id, version, key), nil)
}

// AddValues adds values to a draft group.
func (c *Client) AddValues(ctx context.Context, id string, version int, key string, values []vocab.Entry) (AddResult, error) {
	return post[AddResult](ctx, c, groupPath(id, version, key)+"/values", map[string]any{"values": values})
}

// DeleteValue removes a value from a draft group.
func (c *Client) DeleteValue(ctx context.Context, id string, version int, key, value string) error {
	return c.del(ctx, groupPath(id, version, key)+"/values", url.Values{"value": {value}})
}

// Workers and tokens

// ListWorkers lists Workers.
func (c *Client) ListWorkers(ctx context.Context) ([]api.WorkerDTO, error) {
	out, err := get[Items[api.WorkerDTO]](ctx, c, "/workers", nil)
	return out.Items, err
}

// GetWorker reads a Worker.
func (c *Client) GetWorker(ctx context.Context, id string) (api.WorkerDTO, error) {
	return get[api.WorkerDTO](ctx, c, "/workers/"+url.PathEscape(id), nil)
}

// DrainWorker stops giving a Worker new units.
func (c *Client) DrainWorker(ctx context.Context, id string) error {
	_, err := c.call(ctx, request{method: http.MethodPost, path: "/workers/" + url.PathEscape(id) + "/drain"}, nil)
	return err
}

// RevokeWorker cuts a Worker off.
func (c *Client) RevokeWorker(ctx context.Context, id string) error {
	_, err := c.call(ctx, request{method: http.MethodPost, path: "/workers/" + url.PathEscape(id) + "/revoke"}, nil)
	return err
}

// JoinToken lets new Workers register.
type JoinToken struct {
	ID        string     `json:"id"`
	Token     string     `json:"token"`
	MaxUses   int        `json:"max_uses"`
	ExpiresAt *time.Time `json:"expires_at"`
}

// CreateJoinToken makes a join token.
func (c *Client) CreateJoinToken(ctx context.Context, maxUses int, ttl string) (JoinToken, error) {
	return post[JoinToken](ctx, c, "/worker-tokens", map[string]any{"max_uses": maxUses, "ttl": ttl})
}

// CreateToken makes an API token.
func (c *Client) CreateToken(ctx context.Context, name, role, ttl string) (api.TokenDTO, error) {
	return post[api.TokenDTO](ctx, c, "/tokens", map[string]string{"name": name, "role": role, "ttl": ttl})
}

// ListTokens lists API tokens.
func (c *Client) ListTokens(ctx context.Context) ([]api.TokenDTO, error) {
	out, err := get[Items[api.TokenDTO]](ctx, c, "/tokens", nil)
	return out.Items, err
}

// RevokeToken revokes an API token.
func (c *Client) RevokeToken(ctx context.Context, id string) error {
	return c.del(ctx, "/tokens/"+url.PathEscape(id), nil)
}
