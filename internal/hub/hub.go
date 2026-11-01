// Package hub fans events out to subscribers, for live updates. A slow subscriber never
// slows anything else: its buffer fills, it loses events, and it is told to refetch.
package hub

import "sync"

// Event types.
const (
	TypeState    = "state"
	TypeProgress = "progress"
	TypeArtifact = "artifact"
	TypeLog      = "log"
	TypeJob      = "job"
	// TypeResync tells a subscriber it missed events and should refetch what it shows.
	TypeResync = "resync"
)

// BufferSize is how many events a subscriber can fall behind by.
const BufferSize = 64

// Event is one update. An empty SessionID is not about any one session.
type Event struct {
	Type      string
	SessionID string
	Data      any
}

// Hub is the fan-out. Publishing never blocks.
type Hub struct {
	mu   sync.Mutex
	subs map[*Subscription]struct{}
}

// New returns a Hub.
func New() *Hub { return &Hub{subs: map[*Subscription]struct{}{}} }

// Subscription receives events.
type Subscription struct {
	h       *Hub
	session string // "" receives everything
	events  chan Event
	missed  bool
}

// Subscribe starts receiving events, for one session or, with "", for all.
func (h *Hub) Subscribe(session string) *Subscription {
	s := &Subscription{h: h, session: session, events: make(chan Event, BufferSize)}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subs[s] = struct{}{}
	return s
}

// Events is the channel to read.
func (s *Subscription) Events() <-chan Event { return s.events }

// Close stops the subscription.
func (s *Subscription) Close() {
	s.h.mu.Lock()
	defer s.h.mu.Unlock()
	delete(s.h.subs, s)
}

// Count returns the number of subscribers.
func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// Publish delivers an event to everyone interested, dropping it for whoever has no room.
func (h *Hub) Publish(e Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs {
		if s.session != "" && e.SessionID != "" && s.session != e.SessionID {
			continue
		}
		s.deliver(e)
	}
}

func (s *Subscription) deliver(e Event) {
	if s.missed {
		// First get the news of the loss through, as soon as there is room.
		select {
		case s.events <- Event{Type: TypeResync, SessionID: s.session}:
			s.missed = false
		default:
			return
		}
	}
	select {
	case s.events <- e:
	default:
		s.missed = true
	}
}
