package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
)

func TestSplitSubjectContext(t *testing.T) {
	for _, tc := range []struct{ in, ctx, name string }{
		{"orders-value", ".", "orders-value"},
		{":.prod:orders-value", ".prod", "orders-value"},
		{":.broken", ".", ":.broken"},
	} {
		ctx, name := splitSubjectContext(tc.in)
		if ctx != tc.ctx || name != tc.name || subjectContext(tc.in) != tc.ctx {
			t.Errorf("%q = %q %q", tc.in, ctx, name)
		}
	}
}

func TestSoftThenHard(t *testing.T) {
	soft := errors.New("already soft-deleted")

	var calls []sr.DeleteHow
	del := func(how sr.DeleteHow) error {
		calls = append(calls, how)
		if how == sr.SoftDelete {
			return soft
		}

		return nil
	}

	if err := softThenHard("delete", true, del); err != nil || len(calls) != 2 || calls[1] != sr.HardDelete {
		t.Fatalf("permanent = %v %v", err, calls)
	}

	calls = nil
	if err := softThenHard("delete", false, del); !errors.Is(err, soft) || len(calls) != 1 {
		t.Fatalf("soft = %v %v", err, calls)
	}
}

func TestRequireRegistryWrite(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"t": 1})
	if err := c.requireRegistryWrite(); !errors.Is(err, ErrNoRegistry) {
		t.Fatalf("err = %v", err)
	}
}
