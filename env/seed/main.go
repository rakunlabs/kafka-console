// Command seed fills the local Redpanda from env/docker-compose.yaml with
// sample topics, records, schemas and consumer groups, so every page of the
// console has something to show. It is safe to run more than once: topics and
// schemas that exist are reused and new records are appended.
//
//	go run ./env/seed
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

var (
	brokers  = flag.String("brokers", "localhost:19092", "Kafka bootstrap servers")
	registry = flag.String("registry", "http://localhost:18081", "Schema Registry URL, empty to skip schemas")
	count    = flag.Int("n", 500, "records per JSON topic")
)

// topic is one sample topic and what fills it: records JSON records from
// gen, or none when another step (text, Avro) produces them.
type topic struct {
	name       string
	partitions int32
	configs    map[string]*string
	records    func() int
	gen        gen
}

func ptr(s string) *string { return &s }

func fixed(n int) func() int { return func() int { return n } }

// times scales the -n flag, read once flags are parsed.
func times(f float64) func() int { return func() int { return int(float64(*count) * f) } }

var topics = []topic{
	{"orders.v1", 6, map[string]*string{"retention.ms": ptr("604800000")}, times(1), orderRecord},
	{"payments.v1", 3, nil, times(0.5), paymentRecord},
	{"inventory.stock-levels", 4, map[string]*string{"cleanup.policy": ptr("compact")}, fixed(200), stockRecord},
	{"users.profile-updates", 3, map[string]*string{"cleanup.policy": ptr("compact")}, fixed(150), userRecord},
	{"shipments.avro", 3, nil, nil, nil},
	{"clickstream.raw", 12, map[string]*string{"retention.ms": ptr("86400000"), "compression.type": ptr("zstd")}, times(4), clickRecord},
	{"audit.log", 1, map[string]*string{"retention.ms": ptr("-1")}, fixed(120), auditRecord},
	{"notifications.email", 2, nil, fixed(80), emailRecord},
	{"orders.v1.dlq", 1, nil, fixed(12), dlqRecord},
	{"legacy.events", 2, nil, nil, nil},
}

const shipmentSchema = `{
  "type": "record",
  "name": "Shipment",
  "namespace": "com.example.logistics",
  "fields": [
    {"name": "shipment_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "carrier", "type": {"type": "enum", "name": "Carrier", "symbols": ["DHL", "UPS", "FEDEX", "PTT"]}},
    {"name": "status", "type": "string"},
    {"name": "weight_kg", "type": "double"},
    {"name": "eta", "type": ["null", "string"], "default": null}
  ]
}`

const paymentSchema = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Payment",
  "type": "object",
  "required": ["payment_id", "order_id", "amount", "currency"],
  "properties": {
    "payment_id": {"type": "string"},
    "order_id": {"type": "string"},
    "amount": {"type": "number"},
    "currency": {"type": "string", "enum": ["EUR", "USD", "TRY"]},
    "method": {"type": "string"},
    "status": {"type": "string"}
  }
}`

var (
	cities    = []string{"Istanbul", "Ankara", "Izmir", "Berlin", "Amsterdam", "London", "Madrid", "Paris"}
	products  = []string{"SKU-1001", "SKU-1002", "SKU-1017", "SKU-2040", "SKU-3300", "SKU-4108", "SKU-5555"}
	statuses  = []string{"created", "paid", "packed", "shipped", "delivered", "cancelled"}
	carriers  = []string{"DHL", "UPS", "FEDEX", "PTT"}
	currency  = []string{"EUR", "USD", "TRY"}
	methods   = []string{"card", "iban", "wallet"}
	pages     = []string{"/", "/search", "/product/1001", "/product/2040", "/cart", "/checkout", "/account"}
	agents    = []string{"Mozilla/5.0 (Macintosh)", "Mozilla/5.0 (iPhone)", "Mozilla/5.0 (Windows NT 10.0)"}
	actors    = []string{"alice@example.com", "bob@example.com", "ops-bot", "ci-pipeline"}
	auditActs = []string{"topic.create", "topic.config.update", "group.offset.reset", "acl.create", "user.login"}
)

func pick[T any](s []T) T { return s[rand.IntN(len(s))] }

func main() {
	flag.Parse()

	if err := run(); err != nil {
		log.Fatal(err)
	}

	log.Println("done: open http://localhost:8080")
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(*brokers), kgo.ProducerLinger(20*time.Millisecond))
	if err != nil {
		return err
	}
	defer cl.Close()

	if err := createTopics(ctx, kadm.NewClient(cl)); err != nil {
		return err
	}

	var shipID int
	if *registry != "" {
		if shipID, err = registerSchemas(ctx); err != nil {
			return err
		}
	}

	now := time.Now()
	for _, t := range topics {
		if t.gen == nil {
			continue
		}

		if err := produceJSON(ctx, cl, t.name, t.records(), now, t.gen); err != nil {
			return err
		}
	}

	if err := produceText(ctx, cl, "legacy.events", 60, now); err != nil {
		return err
	}

	if shipID != 0 {
		if err := produceAvro(ctx, cl, shipID, 150, now); err != nil {
			return err
		}
	}

	return consumeGroups(ctx)
}

func createTopics(ctx context.Context, adm *kadm.Client) error {
	for _, t := range topics {
		res, err := adm.CreateTopic(ctx, t.partitions, 1, t.configs, t.name)
		switch {
		case err == nil && res.Err == nil:
			log.Printf("topic %s created (%d partitions)", t.name, t.partitions)
		case errors.Is(err, kerr.TopicAlreadyExists) || errors.Is(res.Err, kerr.TopicAlreadyExists):
			log.Printf("topic %s exists", t.name)
		case err != nil:
			return fmt.Errorf("create %s: %w", t.name, err)
		default:
			return fmt.Errorf("create %s: %w", t.name, res.Err)
		}
	}

	return nil
}

func registerSchemas(ctx context.Context) (int, error) {
	rcl, err := sr.NewClient(sr.URLs(*registry))
	if err != nil {
		return 0, err
	}
	ship, err := rcl.CreateSchema(ctx, "shipments.avro-value", sr.Schema{Schema: shipmentSchema, Type: sr.TypeAvro})
	if err != nil {
		return 0, fmt.Errorf("register shipment schema: %w", err)
	}
	if _, err := rcl.CreateSchema(ctx, "payments.v1-value", sr.Schema{Schema: paymentSchema, Type: sr.TypeJSON}); err != nil {
		return 0, fmt.Errorf("register payment schema: %w", err)
	}
	log.Printf("schemas registered (shipment id %d)", ship.ID)
	return ship.ID, nil
}

type gen func(i int, ts time.Time) (key string, value any, headers []kgo.RecordHeader)

func produceJSON(ctx context.Context, cl *kgo.Client, name string, n int, now time.Time, g gen) error {
	recs := make([]*kgo.Record, 0, n)
	for i := range n {
		ts := now.Add(-time.Duration(n-i) * 7 * time.Second)
		k, v, h := g(i, ts)
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		r := &kgo.Record{Topic: name, Value: b, Timestamp: ts, Headers: h}
		if k != "" {
			r.Key = []byte(k)
		}
		recs = append(recs, r)
	}
	return produce(ctx, cl, name, recs)
}

func produceText(ctx context.Context, cl *kgo.Client, name string, n int, now time.Time) error {
	recs := make([]*kgo.Record, 0, n)
	for i := range n {
		ts := now.Add(-time.Duration(n-i) * time.Minute)
		line := fmt.Sprintf("%s INFO legacy-svc event=%d host=app-%02d msg=%q", ts.UTC().Format(time.RFC3339), i, rand.IntN(8), "heartbeat ok")
		if i%9 == 0 {
			line = fmt.Sprintf("%s WARN legacy-svc event=%d slow query took %dms", ts.UTC().Format(time.RFC3339), i, 800+rand.IntN(4000))
		}
		recs = append(recs, &kgo.Record{Topic: name, Value: []byte(line), Timestamp: ts})
	}
	return produce(ctx, cl, name, recs)
}

func produceAvro(ctx context.Context, cl *kgo.Client, id, n int, now time.Time) error {
	schema, err := avro.Parse(shipmentSchema)
	if err != nil {
		return err
	}
	var hdr sr.ConfluentHeader
	recs := make([]*kgo.Record, 0, n)
	for i := range n {
		ts := now.Add(-time.Duration(n-i) * 40 * time.Second)
		var eta any
		if i%3 != 0 {
			eta = ts.Add(48 * time.Hour).UTC().Format(time.RFC3339)
		}
		v := map[string]any{
			"shipment_id": fmt.Sprintf("shp_%06d", i),
			"order_id":    orderID(),
			"carrier":     pick(carriers),
			"status":      pick([]string{"label_created", "in_transit", "out_for_delivery", "delivered"}),
			"weight_kg":   float64(rand.IntN(3000)) / 100,
			"eta":         eta,
		}
		b, err := hdr.AppendEncode(nil, id, nil)
		if err != nil {
			return err
		}
		if b, err = schema.AppendEncode(b, v); err != nil {
			return fmt.Errorf("avro encode: %w", err)
		}
		recs = append(recs, &kgo.Record{Topic: "shipments.avro", Key: []byte(v["shipment_id"].(string)), Value: b, Timestamp: ts})
	}
	return produce(ctx, cl, "shipments.avro", recs)
}

func produce(ctx context.Context, cl *kgo.Client, name string, recs []*kgo.Record) error {
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("produce %s: %w", name, err)
	}
	log.Printf("topic %s: +%d records", name, len(recs))
	return nil
}

// orderID is a random order that orders.v1 may hold, for records that
// refer to one.
func orderID() string { return fmt.Sprintf("ord_%06d", rand.IntN(*count)) }

func hdr(k, v string) kgo.RecordHeader { return kgo.RecordHeader{Key: k, Value: []byte(v)} }

func traceHeaders() []kgo.RecordHeader {
	return []kgo.RecordHeader{
		hdr("trace-id", fmt.Sprintf("%016x%016x", rand.Uint64(), rand.Uint64())),
		hdr("producer", "order-service/2.14.0"),
	}
}

func orderRecord(i int, ts time.Time) (string, any, []kgo.RecordHeader) {
	id := fmt.Sprintf("ord_%06d", i)
	items := make([]map[string]any, 1+rand.IntN(3))
	total := 0.0
	for j := range items {
		price := float64(500+rand.IntN(20000)) / 100
		qty := 1 + rand.IntN(4)
		total += price * float64(qty)
		items[j] = map[string]any{"sku": pick(products), "qty": qty, "unit_price": price}
	}
	return id, map[string]any{
		"order_id":    id,
		"customer_id": fmt.Sprintf("cus_%04d", rand.IntN(400)),
		"status":      pick(statuses),
		"items":       items,
		"total":       float64(int(total*100)) / 100,
		"currency":    pick(currency),
		"shipping":    map[string]any{"city": pick(cities), "express": rand.IntN(4) == 0},
		"created_at":  ts.UTC().Format(time.RFC3339Nano),
	}, append(traceHeaders(), hdr("content-type", "application/json"))
}

func paymentRecord(i int, ts time.Time) (string, any, []kgo.RecordHeader) {
	id := fmt.Sprintf("pay_%06d", i)
	status := "captured"
	if i%11 == 0 {
		status = "declined"
	}
	return id, map[string]any{
		"payment_id": id,
		"order_id":   orderID(),
		"amount":     float64(1000+rand.IntN(90000)) / 100,
		"currency":   pick(currency),
		"method":     pick(methods),
		"status":     status,
	}, []kgo.RecordHeader{hdr("idempotency-key", strconv.FormatUint(rand.Uint64(), 36))}
}

func stockRecord(i int, ts time.Time) (string, any, []kgo.RecordHeader) {
	sku := pick(products)
	return sku, map[string]any{
		"sku":        sku,
		"warehouse":  pick([]string{"IST-1", "AMS-2", "BER-1"}),
		"available":  rand.IntN(900),
		"reserved":   rand.IntN(40),
		"updated_at": ts.UTC().Format(time.RFC3339),
	}, nil
}

func userRecord(i int, ts time.Time) (string, any, []kgo.RecordHeader) {
	id := fmt.Sprintf("cus_%04d", rand.IntN(400))
	if i%17 == 0 {
		// Some updates mark the customer deleted.
		return id, map[string]any{"customer_id": id, "deleted": true}, nil
	}
	return id, map[string]any{
		"customer_id": id,
		"name":        pick([]string{"Ayşe Yılmaz", "Mehmet Kaya", "Elif Demir", "Jan de Vries", "Sophie Martin", "Lukas Müller"}),
		"email":       fmt.Sprintf("user%d@example.com", rand.IntN(400)),
		"locale":      pick([]string{"tr-TR", "en-GB", "de-DE", "nl-NL", "fr-FR"}),
		"marketing":   rand.IntN(2) == 0,
		"updated_at":  ts.UTC().Format(time.RFC3339),
	}, nil
}

func clickRecord(i int, ts time.Time) (string, any, []kgo.RecordHeader) {
	sess := fmt.Sprintf("s_%05d", rand.IntN(900))
	return sess, map[string]any{
		"session": sess,
		"page":    pick(pages),
		"ref":     pick([]string{"google", "direct", "newsletter", "instagram"}),
		"ua":      pick(agents),
		"ms":      40 + rand.IntN(2400),
		"ts":      ts.UnixMilli(),
	}, nil
}

func auditRecord(i int, ts time.Time) (string, any, []kgo.RecordHeader) {
	return "", map[string]any{
		"actor":  pick(actors),
		"action": pick(auditActs),
		"target": pick([]string{"orders.v1", "payments.v1", "billing-service", "analytics"}),
		"ok":     i%13 != 0,
		"at":     ts.UTC().Format(time.RFC3339),
	}, []kgo.RecordHeader{hdr("source", "kafka-console")}
}

func emailRecord(i int, ts time.Time) (string, any, []kgo.RecordHeader) {
	return fmt.Sprintf("mail_%05d", i), map[string]any{
		"to":       fmt.Sprintf("user%d@example.com", rand.IntN(400)),
		"template": pick([]string{"order-confirmation", "shipment-update", "password-reset", "weekly-digest"}),
		"vars":     map[string]any{"order_id": orderID()},
	}, nil
}

func dlqRecord(i int, ts time.Time) (string, any, []kgo.RecordHeader) {
	return orderID(), map[string]any{
		"order_id": orderID(),
		"status":   "UNKNOWN_STATE",
	}, []kgo.RecordHeader{
		hdr("dlq.reason", pick([]string{"schema validation failed: total < 0", "unknown status UNKNOWN_STATE", "timeout calling inventory-service"})),
		hdr("dlq.original-topic", "orders.v1"),
		hdr("dlq.attempts", strconv.Itoa(1+rand.IntN(5))),
	}
}

// consumeGroups commits offsets for a few groups at different positions, so
// the consumer-group pages show caught-up, lagging and stale groups.
func consumeGroups(ctx context.Context) error {
	groups := []struct {
		name   string
		topics []string
		share  float64 // fraction of each partition to commit
	}{
		{"billing-service", []string{"orders.v1", "payments.v1"}, 1},
		{"analytics-etl", []string{"clickstream.raw", "orders.v1"}, 0.6},
		{"shipment-tracker", []string{"shipments.avro"}, 0.92},
		{"email-sender", []string{"notifications.email"}, 0.3},
		{"search-indexer", []string{"users.profile-updates", "inventory.stock-levels"}, 0.8},
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(*brokers))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	for _, g := range groups {
		ends, err := adm.ListEndOffsets(ctx, g.topics...)
		if err != nil {
			return fmt.Errorf("end offsets %v: %w", g.topics, err)
		}
		offs := kadm.Offsets{}
		ends.Each(func(o kadm.ListedOffset) {
			at := int64(float64(o.Offset) * g.share)
			offs.Add(kadm.Offset{Topic: o.Topic, Partition: o.Partition, At: at, LeaderEpoch: -1})
		})
		if err := adm.CommitAllOffsets(ctx, g.name, offs); err != nil {
			return fmt.Errorf("commit %s: %w", g.name, err)
		}
		log.Printf("group %s committed at %.0f%%", g.name, g.share*100)
	}
	return nil
}
