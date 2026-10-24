package kafka

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type ProduceRequest struct {
	Key       *string  `json:"key"`
	Value     *string  `json:"value"`
	Headers   []Header `json:"headers"`
	Partition *int32   `json:"partition"`
	// KeySchema / ValueSchema serialize the JSON text through the registry.
	KeySchema   *EncodeRequest `json:"key_schema"`
	ValueSchema *EncodeRequest `json:"value_schema"`
	// Encoding of value: utf8 (default) or base64 for binary payloads.
	ValueEncoding string `json:"value_encoding"`
	KeyEncoding   string `json:"key_encoding"`
	// Compression for the produced batch: none, gzip, snappy, lz4, zstd.
	Compression string `json:"compression"`
}

type ProduceResult struct {
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

func (c *Client) payload(ctx context.Context, text *string, encoding string, schema *EncodeRequest, what string) ([]byte, error) {
	if text == nil {
		return nil, nil
	}

	if schema != nil && (schema.Subject != "" || schema.ID > 0) {
		b, err := c.encodeRegistry(ctx, schema, []byte(*text))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", what, err)
		}

		return b, nil
	}

	if encoding == "base64" {
		b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(*text))
		if err != nil {
			return nil, fmt.Errorf("%s: invalid base64; %w", what, err)
		}

		return b, nil
	}

	return []byte(*text), nil
}

// produceTimeout bounds one produced record.
const produceTimeout = 15 * time.Second

// errBadPartitionText is how kgo's manual partitioner says a record names a
// partition its topic lacks.
const errBadPartitionText = "invalid record partitioning choice"

// checkPartition confirms topic has partition p; a missing topic is not found.
func (c *Client) checkPartition(ctx context.Context, topic string, p int32) error {
	_, td, err := c.topicDetail(ctx, topic)
	if err != nil {
		return err
	}

	if _, ok := td.Partitions[p]; !ok {
		return noPartition(topic, p)
	}

	return nil
}

func (c *Client) Produce(ctx context.Context, topic string, req ProduceRequest) (*ProduceResult, error) {
	if err := c.requireWritable(); err != nil {
		return nil, err
	}

	rec, err := c.produceRecord(ctx, topic, req)
	if err != nil {
		return nil, err
	}

	codec, err := compression(req.Compression)
	if err != nil {
		return nil, ErrInvalid{err}
	}

	producer, err := c.producer(req.Partition != nil, codec)
	if err != nil {
		return nil, fmt.Errorf("producer; %w", err)
	}

	// The manual partitioner refuses a partition the topic lacks; that is a
	// bad request, not a broker failure, so it is checked first.
	if req.Partition != nil {
		if err := c.checkPartition(ctx, topic, *req.Partition); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithTimeout(ctx, produceTimeout)
	defer cancel()

	out, err := producer.ProduceSync(ctx, rec).First()
	if err != nil {
		// The partition may have gone since the check; kgo has no sentinel.
		if req.Partition != nil && strings.Contains(err.Error(), errBadPartitionText) {
			return nil, noPartition(topic, *req.Partition)
		}

		return nil, fmt.Errorf("produce; %w", err)
	}

	return &ProduceResult{Partition: out.Partition, Offset: out.Offset, Timestamp: out.Timestamp}, nil
}

// produceRecord is shared by ordinary produce and drafts seeded from a message.
func (c *Client) produceRecord(ctx context.Context, topic string, req ProduceRequest) (*kgo.Record, error) {
	rec := &kgo.Record{Topic: topic}

	var err error

	if rec.Key, err = c.payload(ctx, req.Key, req.KeyEncoding, req.KeySchema, "key"); err != nil {
		return nil, ErrInvalid{err}
	}

	if rec.Value, err = c.payload(ctx, req.Value, req.ValueEncoding, req.ValueSchema, "value"); err != nil {
		return nil, ErrInvalid{err}
	}

	for _, h := range req.Headers {
		value := []byte(h.Value)
		if h.Null {
			value = nil
		} else if h.Encoding == "base64" {
			if value, err = base64.StdEncoding.DecodeString(h.Value); err != nil {
				return nil, ErrInvalid{fmt.Errorf("header %s: invalid base64; %w", h.Key, err)}
			}
		}

		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.Key, Value: value})
	}

	if req.Partition != nil {
		if *req.Partition < 0 {
			return nil, ErrInvalid{fmt.Errorf("partition %d is not valid", *req.Partition)}
		}

		rec.Partition = *req.Partition
	}

	return rec, nil
}

// compressionCodecs are the batch compressions a produce may ask for.
var compressionCodecs = map[string]kgo.CompressionCodec{
	"gzip":   kgo.GzipCompression(),
	"snappy": kgo.SnappyCompression(),
	"lz4":    kgo.Lz4Compression(),
	"zstd":   kgo.ZstdCompression(),
}

// compression checks a requested codec name; "" means none.
func compression(name string) (string, error) {
	lower := strings.ToLower(name)
	if lower == "" || lower == "none" {
		return "", nil
	}

	if _, ok := compressionCodecs[lower]; ok {
		return lower, nil
	}

	return "", fmt.Errorf("unknown compression %q", name)
}
