// Package script runs user-supplied JavaScript predicates safely.
package script

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/dop251/goja"
	"github.com/dop251/goja/ast"
)

// maxCallStackSize turns runaway recursion into a catchable JavaScript error
// rather than letting it exhaust the host stack and take the server with it.
const maxCallStackSize = 2000

// reinterruptInterval is how often a guard past its deadline re-asserts the
// interrupt. goja clears the flag when an interrupted call returns, so without
// re-arming the next evaluation would run unbounded. It is short enough that a
// runaway predicate is stopped promptly and long enough to cost nothing.
const reinterruptInterval = 50 * time.Millisecond

// errNotSingleFunction is why a script that closes its wrapper is refused,
// by both Compile and Check.
const errNotSingleFunction = "script must be a single function body; it closes the function it is wrapped in"

// Script is a compiled user predicate.
//
// The program is compiled once and the runtime reused across a whole scan,
// because creating either per call would cost more than the filtering itself.
// A goja runtime is not safe for concurrent use, so each goroutine that
// evaluates predicates owns one of its own.
type Script struct {
	runtime *goja.Runtime
	fn      goja.Callable
}

// Compile prepares a predicate for evaluation.
//
// The body is wrapped in a function so that a bare "return" works at the top
// level, which is the form a caller naturally writes. The parameter names are
// the variables the predicate will see, and Call must supply them in the same
// order.
//
// Compiling here rather than per evaluation reports a malformed script before
// any data is read.
func Compile(source string, parameters ...string) (*Script, error) {
	wrapped := wrap(source, parameters)

	// The source is pasted into a wrapper, so a script containing `})` could
	// close it early and put statements at the top level. Those run when the
	// program loads, before any Guard exists, and an endless loop there could
	// never be stopped. Only a program that is exactly one function literal
	// is allowed to load.
	parsed, err := goja.Parse("filter.js", wrapped)
	if err != nil {
		return nil, fmt.Errorf("script does not compile: %w", err)
	}

	if !singleFunction(parsed) {
		return nil, errors.New(errNotSingleFunction)
	}

	program, err := goja.CompileAST(parsed, true)
	if err != nil {
		return nil, fmt.Errorf("script does not compile: %w", err)
	}

	runtime := goja.New()

	// Nothing from the host is injected: the runtime has no require, no
	// filesystem, no network and no clock of its own. A filter has no business
	// reaching any of them.
	runtime.SetMaxCallStackSize(maxCallStackSize)

	// A filter must give the same answer twice. Both sources of nondeterminism
	// in JavaScript are pinned, so Date.now() and Math.random() cannot make two
	// identical calls disagree.
	fixed := time.Unix(0, 0).UTC()
	runtime.SetTimeSource(func() time.Time { return fixed })

	seeded := rand.New(rand.NewSource(1))
	runtime.SetRandSource(func() float64 { return seeded.Float64() })

	value, err := runtime.RunProgram(program)
	if err != nil {
		return nil, fmt.Errorf("script does not load: %w", err)
	}

	fn, ok := goja.AssertFunction(value)
	if !ok {
		return nil, fmt.Errorf("script did not produce a function")
	}

	return &Script{runtime: runtime, fn: fn}, nil
}

// singleFunction reports whether a program is one expression statement whose
// expression is a function literal, which is the only shape that defines the
// predicate without running anything.
func singleFunction(program *ast.Program) bool {
	if len(program.Body) != 1 {
		return false
	}

	statement, ok := program.Body[0].(*ast.ExpressionStatement)
	if !ok {
		return false
	}

	_, ok = statement.Expression.(*ast.FunctionLiteral)

	return ok
}

// Runtime exposes the underlying runtime so a caller can build values that only
// make sense inside it, such as a real JavaScript Date.
func (s *Script) Runtime() *goja.Runtime {
	return s.runtime
}

// Value converts a Go value into one the predicate can read.
func (s *Script) Value(value any) goja.Value {
	return s.runtime.ToValue(value)
}

// Call evaluates the predicate and reports JavaScript truthiness, so a script
// may return a field directly rather than spelling out a comparison.
//
// An error means the script failed on this input, which is a different outcome
// from the input not matching, and callers count the two separately.
func (s *Script) Call(arguments ...goja.Value) (bool, error) {
	result, err := s.fn(goja.Undefined(), arguments...)
	if err != nil {
		return false, err
	}

	return result.ToBoolean(), nil
}

// Interrupt stops a script that is currently running. It is safe to call from
// another goroutine, and is what keeps an endless loop from hanging a caller.
func (s *Script) Interrupt(reason string) {
	s.runtime.Interrupt(reason)
}

// Close releases the interrupt flag. A runtime that was interrupted keeps the
// flag set, so reusing one without clearing it would kill the next script
// immediately.
func (s *Script) Close() {
	s.runtime.ClearInterrupt()
}

// Watchdog bounds every call of a script to a budget, and fails them all once
// ctx is done, with one goroutine for a whole read rather than one per call.
//
// goja does not yield, so a predicate that never returns is never preempted:
// the context is not observed until the call comes back, which for
// `while (true) {}` is never. Watching from another goroutine and
// interrupting the runtime is the only thing that can stop it.
type Watchdog struct {
	script *Script
	ctx    context.Context
	budget time.Duration
	reason string

	mu sync.Mutex
	// deadline is when the running call is interrupted; zero between calls,
	// so an interrupt never lands on the next one.
	deadline time.Time

	stop chan struct{}
	done chan struct{}
}

// Watch starts a watchdog for s. Stop must be called when evaluation ends.
func Watch(ctx context.Context, s *Script, budget time.Duration, reason string) *Watchdog {
	w := &Watchdog{
		script: s, ctx: ctx, budget: budget, reason: reason,
		stop: make(chan struct{}), done: make(chan struct{}),
	}

	go w.run()

	return w
}

func (w *Watchdog) run() {
	defer close(w.done)

	// The ticker also re-asserts an interrupt past ctx, which goja needs as
	// it clears the flag when an interrupted call returns.
	ticker := time.NewTicker(min(reinterruptInterval, w.budget))
	defer ticker.Stop()

	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
		}

		w.mu.Lock()
		if !w.deadline.IsZero() && (w.ctx.Err() != nil || time.Now().After(w.deadline)) {
			w.script.Interrupt(w.reason)
		}
		w.mu.Unlock()
	}
}

// Call evaluates the predicate within the budget. Once ctx is done every call
// fails with its error without running.
func (w *Watchdog) Call(arguments ...goja.Value) (bool, error) {
	if err := w.ctx.Err(); err != nil {
		return false, err
	}

	w.mu.Lock()
	w.deadline = time.Now().Add(w.budget)
	w.mu.Unlock()

	ok, err := w.script.Call(arguments...)

	w.mu.Lock()
	w.deadline = time.Time{}
	w.mu.Unlock()

	// An interrupt that arrived as the call returned would stop the next one.
	w.script.Close()

	return ok, err
}

// Script is the predicate the watchdog guards.
func (w *Watchdog) Script() *Script { return w.script }

// Stop ends the watchdog's goroutine.
func (w *Watchdog) Stop() {
	close(w.stop)
	<-w.done
}
