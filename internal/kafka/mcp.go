package kafka

import (
	"github.com/denizgursoy/kafka-mcp/pkg/kafkamcp"
	"github.com/twmb/franz-go/pkg/kgo"
)

// MCPConnection lends our live clients and factories, not our credentials.
// Scan readers come from the console's own pool (limits.mcp_reads) and are
// given back, not closed; the console keeps ownership of the shared client,
// registry and cached producers.
func (c *Client) MCPConnection() kafkamcp.Connection {
	principal := ""
	if c.cfg.SASL != nil {
		principal = c.cfg.SASL.User
	}

	conn := kafkamcp.Connection{
		Name:           c.Name,
		Kafka:          c.client,
		Admin:          c.admin,
		AcquireReader:  c.mcpReader,
		ManualProducer: func() (*kgo.Client, error) { return c.producer(true, "") },
		BulkProducer:   func(manual bool) (*kgo.Client, error) { return c.bulkProducer(manual, "") },
		SchemaRegistry: c.registry,
		ReadOnly:       c.ReadOnly(),
		Brokers:        c.Brokers(),
		Principal:      principal,
	}

	if c.protos != nil {
		conn.ProtoMessage = c.protos.message
	}

	return conn
}
