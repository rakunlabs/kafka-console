package script

import (
	"context"
	"testing"
	"time"
)

func TestWatchdog(t *testing.T) {
	s, err := Compile(`if (spin) { while (true) {} } return true`, "spin")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	w := Watch(ctx, s, 100*time.Millisecond, "timed out")
	defer w.Stop()

	start := time.Now()
	if _, err := w.Call(s.Value(true)); err == nil {
		t.Fatal("an endless loop returned")
	}

	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("interrupted after %v, want about the budget", d)
	}

	// The interrupt of the last call must not stop this one.
	if ok, err := w.Call(s.Value(false)); err != nil || !ok {
		t.Fatalf("call after an interrupt: %v %v", ok, err)
	}

	cancel()

	if _, err := w.Call(s.Value(false)); err == nil {
		t.Fatal("a call after the context ended ran")
	}
}
