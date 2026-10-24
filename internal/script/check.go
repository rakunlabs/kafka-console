package script

import (
	"errors"
	"strings"

	"github.com/dop251/goja"
	"github.com/dop251/goja/parser"
)

// Problem is one diagnostic for a predicate, positioned in the user's source.
type Problem struct {
	Line    int    `json:"line"`
	Column  int    `json:"column"`
	Message string `json:"message"`
}

// Check reports why a predicate would be rejected, using the same wrapper and
// the same engine as Compile, so an editor can flag exactly what the server
// refuses. Positions are translated back from the wrapped program: the
// wrapper adds one line before the user's first.
//
// It parses and compiles only; nothing is executed.
func Check(source string, parameters ...string) []Problem {
	wrapped := wrap(source, parameters)
	lines := strings.Count(source, "\n") + 1
	at := func(line, column int, message string) Problem { return position(source, lines, line, column, message) }

	program, err := parser.ParseFile(nil, "filter.js", wrapped, 0)
	if err != nil {
		var list parser.ErrorList
		if errors.As(err, &list) {
			out := make([]Problem, 0, len(list))
			seen := map[Problem]bool{}
			for _, e := range list {
				p := at(e.Position.Line, e.Position.Column, e.Message)
				if !seen[p] {
					seen[p] = true
					out = append(out, p)
				}
			}

			return out
		}

		return []Problem{{Line: 1, Column: 1, Message: err.Error()}}
	}

	if !singleFunction(program) {
		return []Problem{{Line: 1, Column: 1, Message: errNotSingleFunction}}
	}

	if _, err := goja.CompileAST(program, true); err != nil {
		var syntax *goja.CompilerSyntaxError
		if errors.As(err, &syntax) && syntax.File != nil {
			p := syntax.File.Position(syntax.Offset)
			return []Problem{at(p.Line, p.Column, syntax.Message)}
		}

		var reference *goja.CompilerReferenceError
		if errors.As(err, &reference) && reference.File != nil {
			p := reference.File.Position(reference.Offset)
			return []Problem{at(p.Line, p.Column, reference.Message)}
		}

		return []Problem{{Line: 1, Column: 1, Message: err.Error()}}
	}

	return nil
}

// position maps a location in the wrapped program onto the user's source.
// Line 1 of the wrapper is its header and the line after the source is its
// closing "})"; errors reported on either are pinned to the nearest edge of
// the user's text, which is where the mistake is.
func position(source string, lines, line, column int, message string) Problem {
	line--

	switch {
	case line < 1:
		line, column = 1, 1
	case line > lines:
		line = lines
		last := source[strings.LastIndex(source, "\n")+1:]
		column = len([]rune(last)) + 1
	}

	return Problem{Line: line, Column: column, Message: message}
}

func wrap(source string, parameters []string) string {
	wrapped := "(function ("
	for index, parameter := range parameters {
		if index > 0 {
			wrapped += ", "
		}

		wrapped += parameter
	}

	return wrapped + ") {\n" + source + "\n})"
}
