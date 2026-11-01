package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/genadigeno/data-feed/internal/clock/clocktest"
)

var epoch = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

// scriptedTransport answers pings from a script, then signals each call.
type scriptedTransport struct {
	results chan error
	version int
	calls   chan struct{}
}

func newTransport(version int, results ...error) *scriptedTransport {
	s := &scriptedTransport{results: make(chan error, len(results)), version: version, calls: make(chan struct{}, 64)}
	for _, r := range results {
		s.results <- r
	}
	return s
}

func (s *scriptedTransport) Ping(ctx context.Context) (int, error) {
	s.calls <- struct{}{}
	select {
	case err := <-s.results:
		return s.version, err
	default:
		return s.version, nil
	}
}

func options(tr Transport, f *clocktest.Fake) Options {
	return Options{
		Name:       "w1",
		Transport:  tr,
		Compatible: func(int) error { return nil },
		Clock:      f,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Rand:       func() float64 { return 1 },
	}
}

func TestBackoffGrowsAndStaysCapped(t *testing.T) {
	want := map[int]time.Duration{
		0: 250 * time.Millisecond, 1: 500 * time.Millisecond, 2: time.Second, 3: 2 * time.Second,
		7: 30 * time.Second, 16: 30 * time.Second, 100: 30 * time.Second, 10000: 30 * time.Second,
	}
	for attempt, w := range want {
		if got := Backoff(attempt, 1); got != w {
			t.Errorf("Backoff(%d) = %v, want %v", attempt, got, w)
		}
	}
	if got := Backoff(3, 0); got != 0 {
		t.Errorf("zero jitter = %v", got)
	}
	if got := Backoff(1, 0.5); got != 250*time.Millisecond {
		t.Errorf("half jitter = %v", got)
	}
}

func TestRunKeepsRetryingThroughFailures(t *testing.T) {
	f := clocktest.NewFake(epoch)
	boom := errors.New("connection refused")
	tr := newTransport(1, boom, boom, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- New(options(tr, f)).Run(ctx) }()

	// Two failures use backoff, then a success uses the regular interval.
	for _, wait := range []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, checkInterval} {
		<-tr.calls
		f.BlockUntil(1)
		f.Advance(wait)
	}
	<-tr.calls // the call after the regular interval proves the loop kept going

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("clean stop returned %v", err)
	}
}

func TestRunStopsOnIncompatibleCoordinator(t *testing.T) {
	f := clocktest.NewFake(epoch)
	incompatible := errors.New("wrong version")
	o := options(newTransport(9), f)
	o.Compatible = func(v int) error {
		if v != 9 {
			t.Errorf("version = %d", v)
		}
		return incompatible
	}
	if err := New(o).Run(context.Background()); !errors.Is(err, incompatible) {
		t.Fatalf("err = %v", err)
	}
}

func TestRunReturnsPromptlyWhenCancelledWhileWaiting(t *testing.T) {
	f := clocktest.NewFake(epoch)
	tr := newTransport(1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(options(tr, f)).Run(ctx) }()

	<-tr.calls
	f.BlockUntil(1)
	cancel() // time never advances: only cancellation can end the wait

	if err := <-done; err != nil {
		t.Fatalf("err = %v", err)
	}
}
