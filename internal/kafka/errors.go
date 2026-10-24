package kafka

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
)

// ErrInvalid marks a request the caller can fix.
type ErrInvalid struct{ Err error }

func (e ErrInvalid) Error() string { return e.Err.Error() }

func (e ErrInvalid) Unwrap() error { return e.Err }

var ErrNotFound = errors.New("not found")

// ErrConflict is a request the cluster's current state refuses, like
// resetting a group whose consumers are still running.
type ErrConflict struct{ Err error }

func (e ErrConflict) Error() string { return e.Err.Error() }

func (e ErrConflict) Unwrap() error { return e.Err }

var ErrReadOnly = errors.New("cluster is read-only")

// ErrNoRegistry means the cluster has no schema registry configured.
var ErrNoRegistry = errors.New("schema registry is not configured for this cluster")

// ErrBusy refuses work when a cluster already runs as much of it as allowed.
var ErrBusy = errors.New("too many live tails and replays are running on this cluster; stop one and retry")

// ErrUnsupported means the broker does not implement the request, for
// example ACLs on a cluster without an authorizer.
var ErrUnsupported = errors.New("not supported by this cluster")

// ErrNoTopicAccess marks lag that could not be computed because the console's
// credentials may not read the topic's offsets. The group itself is shown;
// only these partitions carry the error.
var ErrNoTopicAccess = errors.New("no access to this topic")

// ErrNoGroupAccess is the console's principal lacking Describe on a group.
var ErrNoGroupAccess = errors.New("no access to this group")

func accessErr(err error) error {
	if errors.Is(err, kerr.TopicAuthorizationFailed) {
		return ErrNoTopicAccess
	}

	return err
}

// ErrNoDocs means the cluster has no docs path configured.
var ErrNoDocs = errors.New("topic documentation is not configured for this cluster")

// respErr is a failed request's error with the broker's message, when it
// sent one, which usually says why.
func respErr(op string, err error, msg string) error {
	return fmt.Errorf("%s; %w", op, withMessage(err, msg))
}

func withMessage(err error, msg string) error {
	if msg == "" {
		return err
	}

	return fmt.Errorf("%w: %s", err, msg)
}

// message is an optional broker error message.
func message(msg *string) string {
	if msg == nil {
		return ""
	}

	return *msg
}
