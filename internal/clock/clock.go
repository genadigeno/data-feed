// Package clock abstracts time so timeouts, backoff and lease logic can be tested
// without sleeping. Only clock.Real calls the time package's clock functions.
package clock

import "time"

// Clock is the source of time.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) Ticker
}

// Ticker mirrors time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Real returns the system clock.
func Real() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (realClock) NewTicker(d time.Duration) Ticker       { return realTicker{time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }
