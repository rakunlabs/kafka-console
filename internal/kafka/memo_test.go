package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoKeepsResult(t *testing.T) {
	m := newMemo[int](time.Minute, time.Minute, 0)
	defer m.close()
	var reads atomic.Int32
	read := func(context.Context) (int, error) { return int(reads.Add(1)), nil }

	for range 3 {
		v, err := m.get(t.Context(), false, read)
		if err != nil || v != 1 {
			t.Fatalf("got %d, %v; want 1, nil", v, err)
		}
	}
	if n := reads.Load(); n != 1 {
		t.Fatalf("read %d times, want 1", n)
	}
}

func TestMemoFreshPassesTTLButNotFloor(t *testing.T) {
	m := newMemo[int](time.Minute, time.Minute, 0)
	defer m.close()
	var reads atomic.Int32
	read := func(context.Context) (int, error) { return int(reads.Add(1)), nil }

	_, _ = m.get(t.Context(), false, read)
	if v, _ := m.get(t.Context(), true, read); v != 1 {
		t.Fatalf("fresh inside the floor read again: got %d", v)
	}

	m.floor = 0
	if v, _ := m.get(t.Context(), true, read); v != 2 {
		t.Fatalf("fresh past the floor got %d, want a new read", v)
	}
	if v, _ := m.get(t.Context(), false, read); v != 2 {
		t.Fatalf("the fresh result was not kept: got %d", v)
	}
}

func TestMemoSharesRunningRead(t *testing.T) {
	m := newMemo[int](time.Minute, 0, 0)
	defer m.close()
	var reads atomic.Int32
	release := make(chan struct{})
	read := func(context.Context) (int, error) {
		reads.Add(1)
		<-release

		return 7, nil
	}

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if v, err := m.get(t.Context(), false, read); v != 7 || err != nil {
				t.Errorf("got %d, %v", v, err)
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	if n := reads.Load(); n != 1 {
		t.Fatalf("read %d times, want 1", n)
	}
}

func TestMemoDoesNotKeepErrors(t *testing.T) {
	m := newMemo[int](time.Minute, 0, 0)
	defer m.close()
	fail := true
	read := func(context.Context) (int, error) {
		if fail {
			return 0, errors.New("broker down")
		}

		return 3, nil
	}

	if _, err := m.get(t.Context(), false, read); err == nil {
		t.Fatal("want the error")
	}
	fail = false
	if v, err := m.get(t.Context(), false, read); v != 3 || err != nil {
		t.Fatalf("got %d, %v; want a new read after the error", v, err)
	}
}

// A caller that gives up does not cancel the read the others wait for.
func TestMemoCallerLeavingKeepsRead(t *testing.T) {
	m := newMemo[int](time.Minute, 0, 0)
	defer m.close()
	release := make(chan struct{})
	read := func(ctx context.Context) (int, error) {
		select {
		case <-release:
			return 5, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() {
		_, err := m.get(ctx, false, read)
		first <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller got %v, want canceled", err)
	}

	second := make(chan int, 1)
	go func() {
		v, _ := m.get(t.Context(), false, read)
		second <- v
	}()
	close(release)
	if v := <-second; v != 5 {
		t.Fatalf("second caller got %d, want 5", v)
	}
}

func TestMemoExpires(t *testing.T) {
	m := newMemo[int](30*time.Millisecond, 0, 0)
	defer m.close()
	var reads atomic.Int32
	read := func(context.Context) (int, error) { return int(reads.Add(1)), nil }

	_, _ = m.get(t.Context(), false, read)
	time.Sleep(50 * time.Millisecond)
	if v, _ := m.get(t.Context(), false, read); v != 2 {
		t.Fatalf("got %d after the ttl, want a new read", v)
	}
}

// The read's own error comes back unwrapped: it is shown to the user.
func TestMemoReturnsReadError(t *testing.T) {
	m := newMemo[int](time.Minute, 0, 0)
	defer m.close()
	want := errors.New("broker down")

	if _, err := m.get(t.Context(), false, func(context.Context) (int, error) { return 0, want }); err != want {
		t.Fatalf("got %v, want the read's error as is", err)
	}
}

// A refresh asked for while a read is running waits for that read rather than
// starting a second one, and both callers get its result.
func TestMemoFreshJoinsRunningRead(t *testing.T) {
	m := newMemo[int](time.Minute, 0, 0)
	defer m.close()
	var reads atomic.Int32
	release := make(chan struct{})
	read := func(context.Context) (int, error) {
		n := reads.Add(1)
		<-release

		return int(n), nil
	}

	results := make(chan int, 2)
	go func() { v, _ := m.get(t.Context(), false, read); results <- v }()
	time.Sleep(20 * time.Millisecond)
	go func() { v, _ := m.get(t.Context(), true, read); results <- v }()
	time.Sleep(20 * time.Millisecond)
	close(release)

	if a, b := <-results, <-results; a != 1 || b != 1 {
		t.Fatalf("got %d and %d, want both from the one read", a, b)
	}
	if n := reads.Load(); n != 1 {
		t.Fatalf("read %d times, want 1", n)
	}
}
