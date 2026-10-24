package kafka

import (
	"context"
	"time"

	"github.com/rakunlabs/cache"
	"github.com/rakunlabs/cache/store/memory"
)

// memo keeps the last result of an expensive cluster-wide read for a short
// while, so many people opening the same page do not each send the same
// requests to every broker. It holds one value in a rakunlabs/cache memory
// store, whose GetSet makes callers that arrive during a read wait for it
// instead of starting their own. Errors are not kept: the next call tries
// again.
type memo[T any] struct {
	// floor is how long a result is served even to a caller asking for a
	// fresh one, so repeated refresh clicks still share one read.
	floor time.Duration
	// timeout bounds a read. It does not end when the caller that started it
	// goes away, since others may be waiting for the same read.
	timeout time.Duration

	store *cache.Cache[struct{}, memoEntry[T]]
}

type memoEntry[T any] struct {
	val T
	at  time.Time
}

// newMemo serves a result as is for ttl.
func newMemo[T any](ttl, floor, timeout time.Duration) *memo[T] {
	// The memory store never fails to build; its janitor runs until Close.
	store, _ := cache.New(context.Background(), memory.Store[struct{}, memoEntry[T]],
		cache.WithStoreConfig(&memory.Config{MaxItems: 1, TTL: ttl}))

	return &memo[T]{floor: floor, timeout: timeout, store: store}
}

// get returns the kept result unless the caller asks for a fresh one and it
// is older than the floor; otherwise it waits for a read, the one already
// running or a new one.
func (m *memo[T]) get(ctx context.Context, fresh bool, read func(context.Context) (T, error)) (T, error) {
	e, ok, _ := m.store.Get(ctx, struct{}{})

	switch {
	case ok && (!fresh || time.Since(e.at) < m.floor):
		return e.val, nil
	case ok:
		_ = m.store.Delete(ctx, struct{}{})
	}

	// The read runs on its own context so a caller leaving does not cancel
	// it for the others; this caller stops waiting when its ctx ends.
	type result struct {
		val T
		err error
	}
	done := make(chan result, 1)

	go func() {
		readCtx := context.WithoutCancel(ctx)
		if m.timeout > 0 {
			var cancel context.CancelFunc
			readCtx, cancel = context.WithTimeout(readCtx, m.timeout)
			defer cancel()
		}

		// GetSet wraps a failed read in its own message; the read's error is
		// kept to return as is, since it is shown to the user.
		var readErr error
		e, err := m.store.GetSet(readCtx, struct{}{}, func() (memoEntry[T], error) {
			v, err := read(readCtx)
			readErr = err

			return memoEntry[T]{val: v, at: time.Now()}, err
		})
		if readErr != nil {
			err = readErr
		}

		done <- result{e.val, err}
	}()

	select {
	case r := <-done:
		return r.val, r.err
	case <-ctx.Done():
		var zero T

		return zero, ctx.Err()
	}
}

// close stops the store's janitor.
func (m *memo[T]) close() {
	_ = m.store.Close()
}
