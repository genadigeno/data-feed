package clocktest

import (
	"testing"
	"time"
)

var epoch = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func TestAfterFiresOnlyWhenDue(t *testing.T) {
	f := NewFake(epoch)
	ch := f.After(10 * time.Second)

	f.Advance(9 * time.Second)
	select {
	case <-ch:
		t.Fatal("fired before it was due")
	default:
	}

	f.Advance(time.Second)
	select {
	case got := <-ch:
		if want := epoch.Add(10 * time.Second); !got.Equal(want) {
			t.Fatalf("fired at %v, want %v", got, want)
		}
	default:
		t.Fatal("did not fire when due")
	}
}

func TestAfterWithZeroDurationFiresImmediately(t *testing.T) {
	f := NewFake(epoch)
	select {
	case <-f.After(0):
	default:
		t.Fatal("After(0) did not fire")
	}
}

func TestTickerRepeatsAndStops(t *testing.T) {
	f := NewFake(epoch)
	tk := f.NewTicker(time.Second)

	for i := range 3 {
		f.Advance(time.Second)
		select {
		case <-tk.C():
		default:
			t.Fatalf("tick %d missing", i)
		}
	}

	tk.Stop()
	f.Advance(time.Hour)
	select {
	case <-tk.C():
		t.Fatal("ticked after Stop")
	default:
	}
}

func TestBlockUntilSeesWaiter(t *testing.T) {
	f := NewFake(epoch)
	got := make(chan struct{})
	go func() {
		<-f.After(time.Minute)
		close(got)
	}()

	f.BlockUntil(1)
	f.Advance(time.Minute)
	<-got
}
