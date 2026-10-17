// Package script runs user-supplied JavaScript predicates safely.
package script

import (
	"context"
	"fmt"
	"math/rand"
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

// Script is a compiled user predicate.
//
// The program is compiled once and the runtime reused across a whole scan,
// because creating either per call would cost more than the filtering itself.
// A goja runtime is not safe for concurrent use, so each goroutine that
// evaluates predicates owns one of its own.
type Script struct {
	runtime *goja.Runtime
	fn      goja.Callable
	source  string
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
		return nil, fmt.Errorf("script must be a single function body; it closes the function it is wrapped in")
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

	return &Script{runtime: runtime, fn: fn, source: source}, nil
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

// Evaluate runs the script and returns what it returned, for callers that need
// a value rather than a match, such as a grouping key.
func (s *Script) Evaluate(arguments ...goja.Value) (goja.Value, error) {
	return s.fn(goja.Undefined(), arguments...)
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

// Guard interrupts a script when ctx is done, and keeps it interrupted.
//
// This is what makes a deadline real. goja does not yield, so a predicate that
// never returns is never preempted: the context is not observed until the call
// comes back, which for `while (true) {}` is never. Watching the context from
// another goroutine and interrupting the runtime is the only thing that can
// stop it.
//
// Re-arming matters as much as arming. When an interrupted call returns, goja
// clears the interrupt flag itself, so a guard that fired once and exited would
// leave the next evaluation unguarded — and a caller that evaluates a predicate
// per topic would hang on the second one. The watcher therefore keeps
// interrupting for as long as the context stays done.
//
// The returned function must be called when evaluation finishes, and stops the
// watcher. It does not clear the interrupt flag; use Close for that.
func Guard(ctx context.Context, s *Script, reason string) func() {
	if s == nil {
		return func() {}
	}

	stopped := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		select {
		case <-ctx.Done():
		case <-stopped:
			return
		}

		// Past the deadline. Re-assert the interrupt until the caller stops
		// evaluating, so every remaining call fails fast rather than running
		// without a limit.
		ticker := time.NewTicker(reinterruptInterval)
		defer ticker.Stop()

		for {
			s.Interrupt(reason)

			select {
			case <-stopped:
				return
			case <-ticker.C:
			}
		}
	}()

	return func() {
		close(stopped)
		<-done
	}
}
