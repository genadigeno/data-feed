// Package apiclient is the typed Go client for the public API, used by the CLI in
// remote mode.
package apiclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/genadigeno/data-feed/internal/api"
	"github.com/genadigeno/data-feed/internal/apperr"
)

const maxResponseBytes = 1 << 20

// Client calls one Coordinator.
type Client struct {
	base string
	http *http.Client
}

// New returns a client for baseURL such as http://127.0.0.1:8080.
func New(baseURL string, timeout time.Duration) *Client {
	return &Client{base: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: timeout}}
}

// Health calls GET /api/v1/health.
func (c *Client) Health(ctx context.Context) (api.HealthResponse, error) {
	var out api.HealthResponse
	status, err := c.get(ctx, "/api/v1/health", &out)
	if err != nil {
		return out, err
	}
	if status != http.StatusOK {
		return out, unexpected(status)
	}
	return out, nil
}

// Ready calls GET /api/v1/ready. Not being ready is a normal answer, not an error.
func (c *Client) Ready(ctx context.Context) (api.ReadyResponse, error) {
	var out api.ReadyResponse
	status, err := c.get(ctx, "/api/v1/ready", &out)
	if err != nil {
		return out, err
	}
	if status != http.StatusOK && status != http.StatusServiceUnavailable {
		return out, unexpected(status)
	}
	return out, nil
}

func (c *Client) get(ctx context.Context, path string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return 0, apperr.Wrap(err, apperr.InvalidArgument, "ERR_SERVER_URL", "invalid server URL "+c.base)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, apperr.Wrap(err, apperr.Unavailable, "ERR_SERVER_UNAVAILABLE", "cannot reach the server at "+c.base)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(out); err != nil {
		return resp.StatusCode, apperr.Wrap(err, apperr.Unavailable, "ERR_SERVER_RESPONSE", "unreadable response from "+c.base)
	}
	return resp.StatusCode, nil
}

func unexpected(status int) error {
	return apperr.New(apperr.Unavailable, "ERR_SERVER_RESPONSE", fmt.Sprintf("unexpected status %d", status))
}
