package kafka

import (
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// listenerHook logs which broker answered in a way that means the console
// and that listener disagree about TLS. The client's own error names no
// address, and the culprit is often not a seed broker but an address the
// brokers advertise in metadata, so the error only shows "sometimes".
type listenerHook struct {
	cluster string
	tls     bool
}

func (h listenerHook) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, _ int, _, _ time.Duration, err error) {
	if err == nil {
		return
	}

	if hint := listenerHint(err.Error()); hint != "" {
		slog.Warn("broker listener mismatch",
			"cluster", h.cluster, "broker", meta.NodeID, "addr", meta.Host, "port", meta.Port,
			"console_tls", h.tls, "hint", hint, "error", err)
	}
}

// listenerHint explains franz-go's size-check errors in terms of the config.
func listenerHint(msg string) string {
	switch {
	case strings.Contains(msg, "tls alert record"):
		return "this broker address expects TLS but the console connected without it: set tls.enabled for the cluster, " +
			"or check the brokers' advertised listeners point at a listener of the same kind as the bootstrap address"
	case strings.Contains(msg, "'HTTP' in ascii"):
		return "this address answers HTTP, not Kafka: check the broker port"
	case strings.Contains(msg, "invalid large response size"):
		return "this broker address answered with something that is not a Kafka response: " +
			"usually a TLS or SASL listener the console's settings do not match, or a proxy in between"
	}

	return ""
}

// ExplainConnErr adds what to check to a listener-mismatch error.
func ExplainConnErr(err error) string {
	if err == nil {
		return ""
	}

	return listenerHint(err.Error())
}
