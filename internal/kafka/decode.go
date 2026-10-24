package kafka

import (
	"context"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// recordView is a record with key and value decoded once, shared by the JS
// filter and the renderer.
type recordView struct {
	key, value *decoded
	err        string
}

func (c *Client) decodeRecord(ctx context.Context, rec *kgo.Record, raw bool, as DecodeAs) recordView {
	var (
		v    recordView
		errs []string
	)

	// Registry framing first, then a local .proto type configured for the
	// topic, then the requested format. A chosen format skips both: the
	// user says what the bytes are, and binary data can start like framing
	// by chance.
	read := func(b []byte, format, side string) *decoded {
		if !raw && format == AsAuto && b != nil {
			if d, ok, err := c.decodeRegistry(ctx, b); ok {
				if err != nil {
					errs = append(errs, side+": "+err.Error())

					return nil
				}

				return d
			}

			if md := c.protos.message(rec.Topic, side == "key"); md != nil {
				d, err := decodeProto(md, b)
				if err != nil {
					errs = append(errs, side+": "+err.Error())
				}

				return d
			}
		}

		d, ok, err := decodeAs(format, b)
		if ok && err != nil {
			errs = append(errs, side+": "+err.Error())
		}

		return d
	}

	v.key = read(rec.Key, as.Key, "key")
	v.value = read(rec.Value, as.Value, "value")
	v.err = strings.Join(errs, "; ")

	return v
}
