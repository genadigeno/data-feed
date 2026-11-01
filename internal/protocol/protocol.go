// Package protocol implements the internal Coordinator–Worker protocol. Phase 2
// provides the versioned connectivity check; registration, leases, heartbeats and
// part uploads are added in Phase 6. See docs/01-architecture/coordinator-worker-protocol.md.
package protocol

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
)

// Version is the protocol's major version. Peers with another major version refuse
// to work together.
const Version = 1

const (
	pingPath         = "/internal/v1/ping"
	maxResponseBytes = 1 << 20
	requestTimeout   = 10 * time.Second
)

// CodeVersionMismatch reports incompatible peers.
const CodeVersionMismatch = "ERR_PROTOCOL_VERSION"

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

// NewHandler builds the handler for the internal listener.
func NewHandler(logger *slog.Logger, serverVersion string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+pingPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(PingResponse{ProtocolVersion: Version, ServerVersion: serverVersion}); err != nil {
			logger.Debug("write ping response", "err", err)
		}
	})
	return mux
}

// Client is the Worker's HTTP connection to a Coordinator.
type Client struct {
	base string
	http *http.Client
}

// NewClient returns a client for baseURL; tlsConfig applies to https URLs.
func NewClient(baseURL string, tlsConfig *tls.Config) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &Client{
		base: strings.TrimRight(baseURL, "/"),
		http: &http.Client{Transport: transport, Timeout: requestTimeout},
	}
}

// Ping checks connectivity and returns the Coordinator's protocol version.
func (c *Client) Ping(ctx context.Context) (PingResponse, error) {
	var out PingResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+pingPath, nil)
	if err != nil {
		return out, fmt.Errorf("build ping request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("ping: unexpected status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&out); err != nil {
		return out, fmt.Errorf("decode ping response: %w", err)
	}
	return out, nil
}

// Close releases idle connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }
