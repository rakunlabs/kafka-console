package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/rakunlabs/ada"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/rakunlabs/kafka-console/internal/kafka"
)

func TestFailStatus(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
	}{
		{fmt.Errorf("list; %w", kerr.TopicAuthorizationFailed), http.StatusForbidden},
		{fmt.Errorf("describe; %w", kerr.GroupAuthorizationFailed), http.StatusForbidden},
		{fmt.Errorf("lag; %w", kafka.ErrNoTopicAccess), http.StatusForbidden},
		{kafka.ErrConflict{Err: errors.New("group has 2 active members")}, http.StatusConflict},
		{fmt.Errorf("fetch; %w", context.DeadlineExceeded), http.StatusGatewayTimeout},
		{fmt.Errorf("topic; %w", kafka.ErrNotFound), http.StatusNotFound},
		{kafka.ErrInvalid{Err: errors.New("bad")}, http.StatusBadRequest},
		{kerr.LeaderNotAvailable, http.StatusBadGateway},
	} {
		var he *ada.HTTPError
		if !errors.As(fail(tc.err), &he) || he.Code != tc.want {
			t.Errorf("%v: got %v, want %d", tc.err, he, tc.want)
		}
	}

	// The message stays the cause, without a wrapper prefix.
	if got := fail(kafka.ErrConflict{Err: errors.New("group has 2 active members")}).Error(); got != "group has 2 active members" {
		t.Errorf("conflict message %q", got)
	}
}
