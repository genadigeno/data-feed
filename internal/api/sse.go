package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/hub"
)

// WriteSSE renders one Server-Sent Event. Data is JSON, which has no raw newlines, so it
// always fits on one data line.
func WriteSSE(w io.Writer, event string, data any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, body)
	return err
}

// events streams live updates, for one session or all of them. A client that falls behind
// gets a resync event and refetches; it can't slow anything down.
func (s *server) events(w http.ResponseWriter, r *http.Request) error {
	limit := int64(s.o.Config.API.MaxSSEClients)
	if s.sseClients.Add(1) > limit {
		s.sseClients.Add(-1)
		w.Header().Set("Retry-After", "5")
		return apperr.New(apperr.Unavailable, "ERR_TOO_MANY_STREAMS", "too many live update streams are open")
	}
	defer s.sseClients.Add(-1)

	sub := s.o.Hub.Subscribe(r.URL.Query().Get("session"))
	defer sub.Close()

	rc := http.NewResponseController(w)
	noWriteDeadline(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	flush := func() bool { return rc.Flush() == nil }
	// The client should start from a fresh read: nothing before this moment will arrive.
	if WriteSSE(w, hub.TypeResync, map[string]any{}) != nil || !flush() {
		return nil
	}
	heartbeat := s.o.Clock.NewTicker(s.o.Config.API.SSEHeartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return nil
		case <-s.o.Done:
			return nil
		case <-heartbeat.C():
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil || !flush() {
				return nil
			}
		case e := <-sub.Events():
			data := e.Data
			if data == nil {
				data = map[string]any{}
			}
			if WriteSSE(w, e.Type, data) != nil || !flush() {
				return nil
			}
		}
	}
}

// statusCounts counts responses by status class.
type statusCounts struct {
	mu sync.Mutex
	m  map[int]int64
}

func (c *statusCounts) add(status int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[int]int64{}
	}
	c.m[status/100]++
}

func (c *statusCounts) snapshot() map[int]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[int]int64, len(c.m))
	for k, v := range c.m {
		out[k] = v
	}
	return out
}
