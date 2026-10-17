package script

import "testing"

func TestCheck(t *testing.T) {
	params := []string{"value", "key"}

	cases := []struct {
		name   string
		source string
		line   int
		ok     bool
	}{
		{name: "valid", source: `return value.status === "FAILED"`, ok: true},
		{name: "valid multiline", source: "const s = value.status\nreturn s === 'x'", ok: true},
		{name: "syntax on line 2", source: "const a = 1\nreturn value.(", line: 2},
		{name: "unclosed", source: "return value.status === \"x", line: 1},
		{name: "breakout", source: "}); while(true){} (function(){", line: 1},
		{name: "strict mode", source: "with (value) { return 1 }", line: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			problems := Check(tc.source, params...)
			if tc.ok {
				if len(problems) != 0 {
					t.Fatalf("expected no problems, got %+v", problems)
				}
				return
			}

			if len(problems) == 0 {
				t.Fatalf("expected a problem")
			}

			if problems[0].Line != tc.line {
				t.Fatalf("expected line %d, got %+v", tc.line, problems[0])
			}

			if _, err := Compile(tc.source, params...); err == nil {
				t.Fatalf("Check flagged a script that Compile accepts")
			}
		})
	}
}
