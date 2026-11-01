package hub

import "testing"

func TestDeliversBySession(t *testing.T) {
	h := New()
	all, one := h.Subscribe(""), h.Subscribe("a")
	defer all.Close()
	defer one.Close()

	h.Publish(Event{Type: TypeState, SessionID: "b"})
	h.Publish(Event{Type: TypeState, SessionID: "a"})
	h.Publish(Event{Type: TypeJob}) // about no session in particular

	if got := len(all.Events()); got != 3 {
		t.Fatalf("all got %d", got)
	}
	if got := len(one.Events()); got != 2 {
		t.Fatalf("one got %d", got)
	}
}

func TestSlowSubscriberIsToldToResync(t *testing.T) {
	h := New()
	s := h.Subscribe("")
	defer s.Close()
	for range BufferSize + 10 {
		h.Publish(Event{Type: TypeProgress})
	}
	if got := len(s.Events()); got != BufferSize {
		t.Fatalf("buffered %d", got)
	}
	for range 3 {
		<-s.Events()
	}
	h.Publish(Event{Type: TypeArtifact})

	var seen []string
	for len(s.Events()) > 0 {
		seen = append(seen, (<-s.Events()).Type)
	}
	n := len(seen)
	if n < 2 || seen[n-2] != TypeResync || seen[n-1] != TypeArtifact {
		t.Fatalf("tail: %v", seen[max(n-3, 0):])
	}
}

func TestPublishNeverBlocksAndCloseStops(t *testing.T) {
	h := New()
	s := h.Subscribe("")
	if h.Count() != 1 {
		t.Fatal(h.Count())
	}
	s.Close()
	if h.Count() != 0 {
		t.Fatal(h.Count())
	}
	h.Publish(Event{Type: TypeState})
	if len(s.Events()) != 0 {
		t.Fatal("closed subscription received an event")
	}
}
