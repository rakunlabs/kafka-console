package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/rakunlabs/kafka-console/internal/config"
)

// localProto decodes plain protobuf records (no registry framing) with
// message types compiled from local .proto files, chosen by topic.
type localProto struct {
	rules []protoRule
}

type protoRule struct {
	topic      *regexp.Regexp
	key, value protoreflect.MessageDescriptor
}

// newLocalProto compiles every .proto file under the configured paths once,
// at startup, and resolves each topic rule's message names.
func newLocalProto(cfg *config.Protobuf) (*localProto, error) {
	if cfg == nil {
		return nil, nil
	}

	var files []string

	for _, root := range cfg.Paths {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if !d.IsDir() && strings.HasSuffix(path, ".proto") {
				rel, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}

				files = append(files, filepath.ToSlash(rel))
			}

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("protobuf paths; %w", err)
		}
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("protobuf: no .proto files under %v", cfg.Paths)
	}

	slices.Sort(files)
	files = slices.Compact(files)

	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{ImportPaths: cfg.Paths}),
	}

	compiled, err := compiler.Compile(context.Background(), files...)
	if err != nil {
		return nil, fmt.Errorf("protobuf: compile; %w", err)
	}

	messages := map[protoreflect.FullName]protoreflect.MessageDescriptor{}

	for _, fd := range compiled {
		walkMessages(fd.Messages(), func(m protoreflect.MessageDescriptor) bool {
			messages[m.FullName()] = m

			return true
		})
	}

	find := func(name string) (protoreflect.MessageDescriptor, error) {
		if name == "" {
			return nil, nil
		}

		if m, ok := messages[protoreflect.FullName(strings.TrimPrefix(name, "."))]; ok {
			return m, nil
		}

		return nil, fmt.Errorf("protobuf: message %q is not in the .proto files", name)
	}

	out := &localProto{}

	for _, t := range cfg.Topics {
		re, err := regexp.Compile("^(?:" + t.Topic + ")$")
		if err != nil {
			return nil, fmt.Errorf("protobuf: topic %q; %w", t.Topic, err)
		}

		r := protoRule{topic: re}
		if r.key, err = find(t.Key); err != nil {
			return nil, err
		}

		if r.value, err = find(t.Value); err != nil {
			return nil, err
		}

		out.rules = append(out.rules, r)
	}

	return out, nil
}

// message is the type configured for a topic's key or value, if any.
func (p *localProto) message(topic string, key bool) protoreflect.MessageDescriptor {
	if p == nil {
		return nil
	}

	for _, r := range p.rules {
		if r.topic.MatchString(topic) {
			if key {
				return r.key
			}

			return r.value
		}
	}

	return nil
}

// decode reads b as md and gives it as JSON, the same shape the registry
// protobuf decoding produces.
func decodeProto(md protoreflect.MessageDescriptor, b []byte) (*decoded, error) {
	msg := dynamicpb.NewMessage(md)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(b, msg); err != nil {
		return nil, fmt.Errorf("protobuf %s; %w", md.FullName(), err)
	}

	raw, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(msg)
	if err != nil {
		return nil, err
	}

	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}

	if doc == nil {
		return nil, errors.New("protobuf: empty message")
	}

	return &decoded{Text: string(raw), Doc: doc, Format: "protobuf", Message: string(md.FullName())}, nil
}
