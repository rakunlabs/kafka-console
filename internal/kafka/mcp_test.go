package kafka

import (
	"testing"

	"github.com/denizgursoy/kafka-mcp/pkg/kafkamcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/rakunlabs/kafka-console/internal/config"
)

func TestMCPBorrowsExistingClientsAndFactories(t *testing.T) {
	c, err := New("local", &config.Cluster{Brokers: []string{"localhost:1"}, ReadOnly: true,
		SchemaRegistry: &config.SchemaRegistry{URLs: []string{"http://localhost:2"}, Token: "host-secret"},
	}, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	connection := c.MCPConnection()
	if connection.Kafka != c.client || connection.Admin != c.admin || connection.SchemaRegistry != c.registry || !connection.ReadOnly {
		t.Fatal("MCP must receive the exact host client pointers, not reconstructed connections")
	}
	manual, err := connection.ManualProducer()
	if err != nil {
		t.Fatal(err)
	}
	existing, err := c.producer(true, "")
	if err != nil || manual != existing {
		t.Fatal("MCP must reuse the console's cached manual producer")
	}
	if connection.NewReader != nil || connection.AcquireReader == nil {
		t.Fatal("MCP must take readers from the console's bounded pool, not open its own")
	}
	reader, release, err := connection.AcquireReader(t.Context(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if reader == c.client || reader == manual {
		t.Fatal("each scan needs an isolated consumer, not shared consuming state")
	}
	release()
	release() // a second release must not free a slot twice
	bulk, err := connection.BulkProducer(false)
	if err != nil || bulk == c.client {
		t.Fatal("MCP batch writes must use the console's bulk producer, not the shared client")
	}
	if connection.Name != "local" {
		t.Fatalf("connection must carry the console's cluster name, got %q", connection.Name)
	}
	clusters, err := kafkamcp.NewClusters(connection)
	if err != nil {
		t.Fatal(err)
	}
	if err := clusters.Register(mcp.NewServer(&mcp.Implementation{Name: "t", Version: "1"}, nil), "local", kafkamcp.Options{}); err != nil {
		t.Fatal(err)
	}
	// Registering must not clear/replace the console-owned manual producer.
	again, err := connection.ManualProducer()
	if err != nil || again != manual {
		t.Fatal("MCP cleanup must preserve console-owned cached producers")
	}
}
