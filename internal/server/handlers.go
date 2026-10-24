package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/rakunlabs/ada"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/rakunlabs/kafka-console/internal/kafka"
)

// handle runs fn with the request's cluster and replies with its result, or
// 204 when the result is nil. Handlers parse and bind their input before
// calling it, so a bad request is a 400 whichever cluster it names.
func (s *Server) handle(c *ada.Context, fn func(cl *kafka.Client) (any, error)) error {
	return s.reply(c, http.StatusOK, fn)
}

// created is handle answering 201 for what fn made.
func (s *Server) created(c *ada.Context, fn func(cl *kafka.Client) (any, error)) error {
	return s.reply(c, http.StatusCreated, fn)
}

func (s *Server) reply(c *ada.Context, status int, fn func(cl *kafka.Client) (any, error)) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	out, err := fn(cl)
	if err != nil {
		return fail(err)
	}

	if out == nil {
		return c.SendNoContent()
	}

	return c.SetStatus(status).SendJSON(out)
}

func (s *Server) client(c *ada.Context) (*kafka.Client, error) {
	name := c.Request.PathValue("cluster")

	cl, ok := s.registry.Get(name)
	if !ok {
		return nil, ada.NewHTTPError(http.StatusNotFound, fmt.Sprintf("cluster %q not found", name))
	}

	return cl, nil
}

func bind[T any](c *ada.Context) (T, error) {
	var v T

	return v, bindErr(c.Bind(&v))
}

// bindErr makes a body that could not be read a 400, except one over the
// size limit: ada answers that with 413 itself.
func bindErr(err error) error {
	if err == nil {
		return nil
	}

	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return err
	}

	return ada.WrapHTTPError(http.StatusBadRequest, err)
}

// writeJSONError answers outside a handler (middleware, unknown routes) in
// the same {"message": …} shape ada gives handler errors.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	b, _ := json.Marshal(map[string]string{"message": msg})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// maxPage caps one page so a single request cannot ask for stats, lag or
// versions of every item in a large cluster.
const maxPage = 500

// pageParams reads offset and limit; a missing or too large limit is maxPage.
func pageParams(q url.Values) (offset, limit int, err error) {
	for _, p := range []struct {
		key string
		dst *int
	}{{"offset", &offset}, {"limit", &limit}} {
		if v := q.Get(p.key); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return 0, 0, ada.WrapHTTPError(http.StatusBadRequest, fmt.Errorf("%s must be a non-negative integer", p.key))
			}
			*p.dst = n
		}
	}

	if limit == 0 || limit > maxPage {
		limit = maxPage
	}

	return offset, limit, nil
}

// fail maps domain errors to HTTP errors whose text is safe to show.
func fail(err error) error {
	var (
		invalid  kafka.ErrInvalid
		conflict kafka.ErrConflict
	)

	switch {
	case errors.Is(err, kafka.ErrReadOnly), denied(err):
		return ada.WrapHTTPError(http.StatusForbidden, err)
	case errors.Is(err, kafka.ErrNotFound):
		return ada.WrapHTTPError(http.StatusNotFound, err)
	case errors.As(err, &invalid):
		return ada.WrapHTTPError(http.StatusBadRequest, err)
	case errors.As(err, &conflict):
		return ada.WrapHTTPError(http.StatusConflict, err)
	case errors.Is(err, kafka.ErrBusy):
		return ada.WrapHTTPError(http.StatusTooManyRequests, err)
	case errors.Is(err, kafka.ErrNoRegistry), errors.Is(err, kafka.ErrUnsupported), errors.Is(err, kafka.ErrNoDocs):
		return ada.WrapHTTPError(http.StatusNotImplemented, err)
	case errors.Is(err, context.DeadlineExceeded):
		return ada.WrapHTTPError(http.StatusGatewayTimeout, err)
	case errors.Is(err, context.Canceled):
		// The caller went away; nobody reads this answer.
		return ada.WrapHTTPError(statusClientClosed, err)
	case kafkaIs(err, kafkaNotFound):
		return ada.WrapHTTPError(http.StatusNotFound, err)
	case kafkaIs(err, kafkaConflict):
		return ada.WrapHTTPError(http.StatusConflict, err)
	case kafkaIs(err, kafkaInvalid):
		return ada.WrapHTTPError(http.StatusBadRequest, err)
	default:
		// Kafka errors carry no secrets and the operator needs them.
		if hint := kafka.ExplainConnErr(err); hint != "" {
			return ada.WrapHTTPError(http.StatusBadGateway, fmt.Errorf("%w; %s", err, hint))
		}

		return ada.WrapHTTPError(http.StatusBadGateway, err)
	}
}

// statusClientClosed is nginx's code for a request its client abandoned.
const statusClientClosed = 499

// Broker refusals the caller can act on, by what they mean; anything else
// stays a 502, the broker's own failure.
var (
	kafkaNotFound = []error{kerr.UnknownTopicOrPartition, kerr.GroupIDNotFound, kerr.UnknownTopicID}
	kafkaConflict = []error{
		kerr.TopicAlreadyExists, kerr.NonEmptyGroup, kerr.GroupSubscribedToTopic,
		kerr.ReassignmentInProgress, kerr.ConcurrentTransactions,
	}
	kafkaInvalid = []error{
		kerr.InvalidPartitions, kerr.InvalidReplicationFactor, kerr.InvalidReplicaAssignment,
		kerr.InvalidConfig, kerr.InvalidTopicException, kerr.PolicyViolation, kerr.InvalidRequest,
		kerr.InvalidGroupID, kerr.MessageTooLarge, kerr.RecordListTooLarge, kerr.InvalidRecord,
		kerr.UnsupportedCompressionType, kerr.InvalidTimestamp,
	}
)

func kafkaIs(err error, kinds []error) bool {
	for _, k := range kinds {
		if errors.Is(err, k) {
			return true
		}
	}

	return false
}

// denied reports Kafka refusing the console's credentials an operation:
// the request was understood, the principal may not do it.
func denied(err error) bool {
	if _, ok := errors.AsType[*kadm.AuthError](err); ok {
		return true
	}

	for _, e := range []error{
		kerr.TopicAuthorizationFailed, kerr.GroupAuthorizationFailed, kerr.ClusterAuthorizationFailed,
		kerr.TransactionalIDAuthorizationFailed, kerr.DelegationTokenAuthorizationFailed,
		kerr.SaslAuthenticationFailed, kafka.ErrNoTopicAccess, kafka.ErrNoGroupAccess,
	} {
		if errors.Is(err, e) {
			return true
		}
	}

	return false
}
