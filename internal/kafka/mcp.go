package kafka

import (
	"github.com/denizgursoy/kafka-mcp/pkg/kafkamcp"
	"github.com/twmb/franz-go/pkg/kgo"
)

// MCPConnection lends our live clients and factories, not our credentials.
// MCP owns only the short-lived consumers returned by newReader; the console
// keeps ownership of the shared client, registry and cached manual producer.
func (c *Client) MCPConnection() kafkamcp.Connection {
	principal := ""
	if c.cfg.SASL != nil {
		principal = c.cfg.SASL.User
	}
	return kafkamcp.Connection{
		Name:  c.Name,
		Kafka: c.client, Admin: c.admin, NewReader: c.newReader,
		ManualProducer: func() (*kgo.Client, error) { return c.producer(true, "") },
		SchemaRegistry: c.registry, ReadOnly: c.ReadOnly(),
		Brokers: c.Brokers(), Principal: principal,
	}
}
