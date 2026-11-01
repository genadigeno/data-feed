// Package protocol carries wire.Coordinator calls over HTTP: the server side wraps the
// Coordinator, the client side is what a remote Worker holds. All decisions are behind the
// interface; this package only moves them across the network.
// See docs/01-architecture/coordinator-worker-protocol.md.
package protocol

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/wire"
)

// Version is the protocol's major version. Peers with another major version refuse
// to work together.
const Version = wire.ProtocolVersion

const (
	pingPath         = "/internal/v1/ping"
	registerPath     = "/internal/v1/workers/register"
	heartbeatPath    = "/internal/v1/workers/heartbeat"
	deregisterPath   = "/internal/v1/workers/deregister"
	leasesPath       = "/internal/v1/leases"
	snapshotsPath    = "/internal/v1/snapshots/"
	unitsPath        = "/internal/v1/units/"
	maxRequestBytes  = 1 << 20
	maxResponseBytes = 1 << 20
	maxSnapshotBytes = 1 << 30
	callTimeout      = 30 * time.Second
	leaseSlack       = 15 * time.Second
	readTimeout      = 30 * time.Second
	// minUploadBytesPerSecond is deliberately low: it catches stalling, not slow links.
	minUploadBytesPerSecond = 128 << 10
)

// Part headers. Everything about a part except its bytes travels here, so the Coordinator
// can decide before reading the body.
const (
	headerLease     = "X-Lease-Id"
	headerToken     = "X-Fencing-Token"
	headerSHA       = "X-Content-SHA256"
	headerRecords   = "X-Records"
	headerFirst     = "X-First-Index"
	headerEnd       = "X-End-Index"
	headerHeaderLen = "X-Header-Len"
	headerFooterLen = "X-Footer-Len"
	headerExtension = "X-Extension"
)

// CodeVersionMismatch reports incompatible peers.
const CodeVersionMismatch = wire.CodeProtocolVersion

// PingResponse is the body of GET /internal/v1/ping. It holds nothing but version
// numbers, so it is the one internal endpoint open before authentication.
type PingResponse struct {
	ProtocolVersion int    `json:"protocol_version"`
	ServerVersion   string `json:"server_version"`
}

// CheckCompatible reports whether this binary can talk to a Coordinator.
func CheckCompatible(remote int) error {
	if remote != Version {
		return apperr.New(apperr.Conflict, CodeVersionMismatch,
			fmt.Sprintf("coordinator speaks protocol version %d, this binary speaks %d", remote, Version))
	}
	return nil
}

// Backend is what the server exposes.
type Backend interface {
	wire.Coordinator
	// Authenticate returns the Worker a credential belongs to.
	Authenticate(ctx context.Context, credential string) (string, error)
}

type errorBody struct {
	Kind      apperr.Kind    `json:"kind"`
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

type leaseRequest struct {
	Max    int   `json:"max"`
	WaitMS int64 `json:"wait_ms"`
}

type leaseResponse struct {
	Assignments []wire.Assignment `json:"assignments"`
}

type completeRequest struct {
	wire.LeaseRef
	End   uint64 `json:"end"`
	Parts int    `json:"parts"`
}

type failRequest struct {
	wire.LeaseRef
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type server struct {
	b   Backend
	log *slog.Logger
}

// NewHandler builds the handler for the internal listener.
func NewHandler(logger *slog.Logger, serverVersion string, b Backend) http.Handler {
	s := &server{b: b, log: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+pingPath, func(w http.ResponseWriter, _ *http.Request) {
		s.writeJSON(w, PingResponse{ProtocolVersion: Version, ServerVersion: serverVersion})
	})
	mux.HandleFunc("POST "+registerPath, s.register)
	mux.HandleFunc("POST "+heartbeatPath, s.authed(s.heartbeat))
	mux.HandleFunc("POST "+deregisterPath, s.authed(s.deregister))
	mux.HandleFunc("POST "+leasesPath, s.authed(s.lease))
	mux.HandleFunc("GET "+snapshotsPath+"{hash}", s.authed(s.snapshot))
	mux.HandleFunc("HEAD "+unitsPath+"{unit}/parts/{seq}", s.authed(s.partHead))
	mux.HandleFunc("PUT "+unitsPath+"{unit}/parts/{seq}", s.authed(s.partPut))
	mux.HandleFunc("POST "+unitsPath+"{unit}/complete", s.authed(s.complete))
	mux.HandleFunc("POST "+unitsPath+"{unit}/release", s.authed(s.release))
	mux.HandleFunc("POST "+unitsPath+"{unit}/fail", s.authed(s.fail))
	return mux
}

func (s *server) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.log.Debug("write response", "err", err)
	}
}

func (s *server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	e, ok := apperr.As(err)
	if !ok {
		if r.Context().Err() == nil {
			s.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		}
		e = apperr.New(apperr.Internal, "ERR_INTERNAL", "internal error")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(apperr.HTTPStatusFor(e))
	if r.Method == http.MethodHead {
		return
	}
	if err := json.NewEncoder(w).Encode(errorBody{e.Kind, e.Code, e.Message, e.Retryable, e.Details}); err != nil {
		s.log.Debug("write error response", "err", err)
	}
}

func badRequest(msg string) error { return apperr.New(apperr.InvalidArgument, "ERR_BAD_REQUEST", msg) }

func (s *server) readJSON(w http.ResponseWriter, r *http.Request, v any) error {
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(readTimeout)) // not every writer can; the limit is a safeguard
	// Cleared afterwards: a read deadline passing during a long poll would cancel the request.
	defer func() { _ = rc.SetReadDeadline(time.Time{}) }()
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		return badRequest("invalid request body: " + err.Error())
	}
	return nil
}

// UploadDeadline is how long a part of a size may take: slower than this, a client is
// holding one of the few upload slots for nothing.
func UploadDeadline(size int64) time.Duration {
	return readTimeout + time.Duration(size/minUploadBytesPerSecond)*time.Second
}

func (s *server) authed(next func(http.ResponseWriter, *http.Request, string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		credential, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			s.writeError(w, r, apperr.New(apperr.Unauthenticated, "ERR_UNAUTHENTICATED", "a Worker credential is required"))
			return
		}
		worker, err := s.b.Authenticate(r.Context(), credential)
		if err == nil {
			err = next(w, r, worker)
		}
		if err != nil {
			s.writeError(w, r, err)
		}
	}
}

func (s *server) register(w http.ResponseWriter, r *http.Request) {
	var req wire.RegisterRequest
	err := s.readJSON(w, r, &req)
	var resp wire.RegisterResponse
	if err == nil {
		resp, err = s.b.Register(r.Context(), req)
	}
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, resp)
}

func (s *server) heartbeat(w http.ResponseWriter, r *http.Request, worker string) error {
	var h wire.Heartbeat
	if err := s.readJSON(w, r, &h); err != nil {
		return err
	}
	resp, err := s.b.Heartbeat(r.Context(), worker, h)
	if err != nil {
		return err
	}
	s.writeJSON(w, resp)
	return nil
}

func (s *server) deregister(w http.ResponseWriter, r *http.Request, worker string) error {
	if err := s.b.Deregister(r.Context(), worker); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) lease(w http.ResponseWriter, r *http.Request, worker string) error {
	var req leaseRequest
	if err := s.readJSON(w, r, &req); err != nil {
		return err
	}
	assignments, err := s.b.Lease(r.Context(), worker, req.Max, time.Duration(req.WaitMS)*time.Millisecond)
	if err != nil {
		return err
	}
	s.writeJSON(w, leaseResponse{Assignments: assignments})
	return nil
}

func (s *server) snapshot(w http.ResponseWriter, r *http.Request, worker string) error {
	body, err := s.b.Snapshot(r.Context(), worker, r.PathValue("hash"))
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if _, err := w.Write(body); err != nil {
		s.log.Debug("write snapshot", "err", err)
	}
	return nil
}

func partSeq(r *http.Request) (int, error) {
	seq, err := strconv.Atoi(r.PathValue("seq"))
	if err != nil || seq < 0 {
		return 0, badRequest("invalid part number")
	}
	return seq, nil
}

func (s *server) partHead(w http.ResponseWriter, r *http.Request, worker string) error {
	seq, err := partSeq(r)
	if err != nil {
		return err
	}
	committed, sum, err := s.b.PartCommitted(r.Context(), worker, r.PathValue("unit"), seq)
	if err != nil {
		return err
	}
	if !committed {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	w.Header().Set(headerSHA, sum)
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *server) partPut(w http.ResponseWriter, r *http.Request, worker string) error {
	meta, err := parsePartHeaders(r)
	if err != nil {
		return err
	}
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(UploadDeadline(meta.Bytes)))
	defer func() { _ = rc.SetReadDeadline(time.Time{}) }()
	result, err := s.b.UploadPart(r.Context(), worker, meta, wire.PartBody{Reader: r.Body})
	if err != nil {
		return err
	}
	s.writeJSON(w, result)
	return nil
}

func parsePartHeaders(r *http.Request) (wire.PartMeta, error) {
	seq, err := partSeq(r)
	if err != nil {
		return wire.PartMeta{}, err
	}
	m := wire.PartMeta{
		LeaseRef: wire.LeaseRef{UnitID: r.PathValue("unit"), LeaseID: r.Header.Get(headerLease)},
		Seq:      seq, SHA256: r.Header.Get(headerSHA), Extension: r.Header.Get(headerExtension), Bytes: r.ContentLength,
	}
	if m.Bytes < 0 {
		return m, badRequest("Content-Length is required")
	}
	for _, f := range []struct {
		header string
		into   *uint64
	}{{headerToken, &m.FencingToken}, {headerRecords, &m.Records}, {headerFirst, &m.FirstIndex}, {headerEnd, &m.EndIndex}} {
		v, err := strconv.ParseUint(r.Header.Get(f.header), 10, 64)
		if err != nil {
			return m, badRequest("invalid " + f.header)
		}
		*f.into = v
	}
	for _, f := range []struct {
		header string
		into   *int
	}{{headerHeaderLen, &m.HeaderLen}, {headerFooterLen, &m.FooterLen}} {
		v, err := strconv.Atoi(r.Header.Get(f.header))
		if err != nil || v < 0 {
			return m, badRequest("invalid " + f.header)
		}
		*f.into = v
	}
	return m, nil
}

func (s *server) complete(w http.ResponseWriter, r *http.Request, worker string) error {
	var req completeRequest
	if err := s.readJSON(w, r, &req); err != nil {
		return err
	}
	req.UnitID = r.PathValue("unit")
	if err := s.b.Complete(r.Context(), worker, req.LeaseRef, req.End, req.Parts); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) release(w http.ResponseWriter, r *http.Request, worker string) error {
	var ref wire.LeaseRef
	if err := s.readJSON(w, r, &ref); err != nil {
		return err
	}
	ref.UnitID = r.PathValue("unit")
	if err := s.b.Release(r.Context(), worker, ref); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) fail(w http.ResponseWriter, r *http.Request, worker string) error {
	var req failRequest
	if err := s.readJSON(w, r, &req); err != nil {
		return err
	}
	req.UnitID = r.PathValue("unit")
	if err := s.b.Fail(r.Context(), worker, req.LeaseRef, req.Message, req.Retryable); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// Client is a remote Worker's connection to a Coordinator. It implements wire.Coordinator;
// the Worker it speaks for is whoever its credential says, so the worker arguments are unused.
type Client struct {
	base string
	http *http.Client

	mu         sync.Mutex
	credential string
}

var _ wire.Coordinator = (*Client)(nil)

// NewClient returns a client for baseURL; tlsConfig applies to https URLs.
func NewClient(baseURL string, tlsConfig *tls.Config) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &Client{base: strings.TrimRight(baseURL, "/"), http: &http.Client{Transport: transport}}
}

// Close releases idle connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }

func (c *Client) bearer() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.credential
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	if credential := c.bearer(); credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 400 {
		return resp, nil
	}
	defer resp.Body.Close()
	if req.Method == http.MethodHead && resp.StatusCode == http.StatusNotFound {
		return resp, nil
	}
	return nil, decodeError(resp)
}

// decodeError restores a Coordinator error, so a Worker can make the same decisions about
// it as if it had been a direct call. Anything else came from something in between.
func decodeError(resp *http.Response) error {
	var body errorBody
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("read error response: %w", err)
	}
	if json.Unmarshal(data, &body) != nil || body.Code == "" {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return apperr.New(apperr.Unauthenticated, "ERR_UNAUTHENTICATED", fmt.Sprintf("refused with status %d", resp.StatusCode))
		}
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return &apperr.Error{Kind: body.Kind, Code: body.Code, Message: body.Message, Retryable: body.Retryable, Details: body.Details}
}

func (c *Client) call(ctx context.Context, timeout time.Duration, path string, in, out any) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		_, err := io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		return err
	}
	return json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(out)
}

// Ping checks connectivity and returns the Coordinator's versions.
func (c *Client) Ping(ctx context.Context) (PingResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	var out PingResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+pingPath, nil)
	if err != nil {
		return out, fmt.Errorf("build ping request: %w", err)
	}
	resp, err := c.do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&out); err != nil {
		return out, fmt.Errorf("decode ping response: %w", err)
	}
	return out, nil
}

// Register implements wire.Coordinator. It checks versions first, so talking to an
// incompatible Coordinator gives that answer and not a confusing one.
func (c *Client) Register(ctx context.Context, r wire.RegisterRequest) (wire.RegisterResponse, error) {
	var out wire.RegisterResponse
	ping, err := c.Ping(ctx)
	if err != nil {
		return out, err
	}
	if err := CheckCompatible(ping.ProtocolVersion); err != nil {
		return out, err
	}
	if err := c.call(ctx, callTimeout, registerPath, r, &out); err != nil {
		return out, err
	}
	credential := out.Credential
	if credential == "" {
		credential = r.Credential
	}
	c.mu.Lock()
	c.credential = credential
	c.mu.Unlock()
	return out, nil
}

// Heartbeat implements wire.Coordinator.
func (c *Client) Heartbeat(ctx context.Context, _ string, h wire.Heartbeat) (wire.HeartbeatResponse, error) {
	var out wire.HeartbeatResponse
	err := c.call(ctx, callTimeout, heartbeatPath, h, &out)
	return out, err
}

// Lease implements wire.Coordinator.
func (c *Client) Lease(ctx context.Context, _ string, maxUnits int, wait time.Duration) ([]wire.Assignment, error) {
	var out leaseResponse
	err := c.call(ctx, wait+leaseSlack, leasesPath, leaseRequest{Max: maxUnits, WaitMS: wait.Milliseconds()}, &out)
	return out.Assignments, err
}

// Snapshot implements wire.Coordinator.
func (c *Client) Snapshot(ctx context.Context, _, hash string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+snapshotsPath+hash, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSnapshotBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxSnapshotBytes {
		return nil, errors.New("snapshot is too large")
	}
	return body, nil
}

func partURL(base, unit string, seq int) string {
	return base + unitsPath + unit + "/parts/" + strconv.Itoa(seq)
}

// PartCommitted implements wire.Coordinator.
func (c *Client) PartCommitted(ctx context.Context, _, unit string, seq int) (bool, string, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, partURL(c.base, unit, seq), nil)
	if err != nil {
		return false, "", err
	}
	resp, err := c.do(req)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, "", nil
	}
	return true, resp.Header.Get(headerSHA), nil
}

// UploadPart implements wire.Coordinator. It has no time limit of its own, as parts are
// large: the Worker cancels it when the lease stops being trustworthy.
func (c *Client) UploadPart(ctx context.Context, _ string, m wire.PartMeta, body wire.PartBody) (wire.UploadResult, error) {
	var out wire.UploadResult
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, partURL(c.base, m.UnitID, m.Seq), body.Reader)
	if err != nil {
		return out, err
	}
	req.ContentLength = m.Bytes
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(headerLease, m.LeaseID)
	req.Header.Set(headerToken, strconv.FormatUint(m.FencingToken, 10))
	req.Header.Set(headerSHA, m.SHA256)
	req.Header.Set(headerRecords, strconv.FormatUint(m.Records, 10))
	req.Header.Set(headerFirst, strconv.FormatUint(m.FirstIndex, 10))
	req.Header.Set(headerEnd, strconv.FormatUint(m.EndIndex, 10))
	req.Header.Set(headerHeaderLen, strconv.Itoa(m.HeaderLen))
	req.Header.Set(headerFooterLen, strconv.Itoa(m.FooterLen))
	req.Header.Set(headerExtension, m.Extension)
	resp, err := c.do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	return out, json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&out)
}

// Complete implements wire.Coordinator.
func (c *Client) Complete(ctx context.Context, _ string, ref wire.LeaseRef, end uint64, parts int) error {
	return c.call(ctx, callTimeout, unitsPath+ref.UnitID+"/complete", completeRequest{ref, end, parts}, nil)
}

// Release implements wire.Coordinator.
func (c *Client) Release(ctx context.Context, _ string, ref wire.LeaseRef) error {
	return c.call(ctx, callTimeout, unitsPath+ref.UnitID+"/release", ref, nil)
}

// Fail implements wire.Coordinator.
func (c *Client) Fail(ctx context.Context, _ string, ref wire.LeaseRef, message string, retryable bool) error {
	return c.call(ctx, callTimeout, unitsPath+ref.UnitID+"/fail", failRequest{ref, message, retryable}, nil)
}

// Deregister implements wire.Coordinator.
func (c *Client) Deregister(ctx context.Context, _ string) error {
	return c.call(ctx, callTimeout, deregisterPath, struct{}{}, nil)
}
