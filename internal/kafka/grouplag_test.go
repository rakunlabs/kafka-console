package kafka

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestAccessErr(t *testing.T) {
	if !errors.Is(accessErr(&kadm.AuthError{Err: kerr.TopicAuthorizationFailed}), ErrNoTopicAccess) {
		t.Fatal("kadm auth error on a topic should map to ErrNoTopicAccess")
	}

	wrapped := fmt.Errorf("list; %w", kerr.TopicAuthorizationFailed)
	if !errors.Is(accessErr(wrapped), ErrNoTopicAccess) {
		t.Fatal("wrapped topic auth error should map to ErrNoTopicAccess")
	}

	if errors.Is(accessErr(kerr.UnknownTopicOrPartition), ErrNoTopicAccess) {
		t.Fatal("other errors must pass through unchanged")
	}
}

func TestUnreadable(t *testing.T) {
	lag := kadm.GroupLag{
		"allowed": {0: {Topic: "allowed", Lag: 5}},
		"secret":  {0: {Topic: "secret", Lag: -1, Err: ErrNoTopicAccess}, 1: {Topic: "secret", Lag: -1, Err: ErrNoTopicAccess}},
		"gone":    {0: {Topic: "gone", Lag: -1, Err: kerr.UnknownTopicOrPartition}},
	}

	if got := unreadable(lag); !slices.Equal(got, []string{"secret"}) {
		t.Fatalf("unreadable = %v, want [secret]", got)
	}

	// Unknown lag (-1) never lowers the total of the readable topics.
	if got := positiveLag(lag); got != 5 {
		t.Fatalf("positiveLag = %d, want 5", got)
	}
}
