// Package clocktest provides a deterministic clock for tests.
package clocktest

import (
	"sync"
	"time"

	"github.com/genadigeno/data-feed/internal/clock"
)

// Fake is a clock that moves only when Advance is called.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
	changed chan struct{}
}

type waiter struct {
	at     time.Time
	period time.Duration // > 0 for tickers
	ch     chan time.Time
	done   bool
}

// NewFake returns a Fake starting at start.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start, changed: make(chan struct{})}
}

// Now implements clock.Clock.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After implements clock.Clock.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.add(d, 0).ch
}

// NewTicker implements clock.Clock.
func (f *Fake) NewTicker(d time.Duration) clock.Ticker {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &fakeTicker{f: f, w: f.add(d, d)}
}

func (f *Fake) add(d, period time.Duration) *waiter {
	w := &waiter{at: f.now.Add(d), period: period, ch: make(chan time.Time, 1)}
	f.waiters = append(f.waiters, w)
	if d <= 0 && period == 0 {
		f.fire()
	}
	close(f.changed)
	f.changed = make(chan struct{})
	return w
}

// Advance moves time forward and fires everything due. A slow ticker consumer
// misses ticks, matching time.Ticker.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	f.fire()
}

// BlockUntil returns once at least n timers or tickers are waiting, so a test can
// advance time without racing the goroutine that is about to start waiting.
func (f *Fake) BlockUntil(n int) {
	for {
		f.mu.Lock()
		if len(f.waiters) >= n {
			f.mu.Unlock()
			return
		}
		ch := f.changed
		f.mu.Unlock()
		<-ch
	}
}

func (f *Fake) fire() {
	kept := f.waiters[:0]
	for _, w := range f.waiters {
		for !w.done && !w.at.After(f.now) {
			select {
			case w.ch <- f.now:
			default:
			}
			if w.period == 0 {
				w.done = true
				break
			}
			w.at = w.at.Add(w.period)
		}
		if !w.done {
			kept = append(kept, w)
		}
	}
	f.waiters = kept
}

type fakeTicker struct {
	f *Fake
	w *waiter
}

func (t *fakeTicker) C() <-chan time.Time { return t.w.ch }

func (t *fakeTicker) Stop() {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.w.done = true
	kept := t.f.waiters[:0]
	for _, w := range t.f.waiters {
		if w != t.w {
			kept = append(kept, w)
		}
	}
	t.f.waiters = kept
}
