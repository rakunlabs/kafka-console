package kafka

import (
	"context"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// metadata is kadm's Metadata without its all-or-nothing rule. kadm fails
// the whole call when a single topic answers TOPIC_AUTHORIZATION_FAILED,
// which happens whenever the console's principal may describe some topics
// but not others, and an overview or topic list then shows nothing at all.
// Here such a topic keeps its error in TopicDetail.Err like any other
// per-topic error, and callers decide what to show.
func (c *Client) metadata(ctx context.Context, topics ...string) (kadm.Metadata, error) {
	req := kmsg.NewPtrMetadataRequest()
	for _, t := range topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(t)
		req.Topics = append(req.Topics, rt)
	}

	resp, err := c.client.RequestCachedMetadata(ctx, req, 0)
	if err != nil {
		return kadm.Metadata{}, err
	}

	// A cluster-wide refusal is still an error for the whole call.
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return kadm.Metadata{}, err
	}

	m := kadm.Metadata{
		Controller: resp.ControllerID,
		Topics:     make(kadm.TopicDetails, len(resp.Topics)),
	}
	if resp.ClusterID != nil {
		m.Cluster = *resp.ClusterID
	}

	for _, b := range resp.Brokers {
		m.Brokers = append(m.Brokers, kgo.BrokerMetadata{NodeID: b.NodeID, Host: b.Host, Port: b.Port, Rack: b.Rack})
	}

	sort.Slice(m.Brokers, func(i, j int) bool { return m.Brokers[i].NodeID < m.Brokers[j].NodeID })

	for _, t := range resp.Topics {
		td := kadm.TopicDetail{
			ID:         t.TopicID,
			IsInternal: t.IsInternal,
			Partitions: make(map[int32]kadm.PartitionDetail, len(t.Partitions)),
			Err:        accessErr(kerr.ErrorForCode(t.ErrorCode)),
		}
		if t.Topic != nil {
			td.Topic = *t.Topic
		}

		for _, p := range t.Partitions {
			td.Partitions[p.Partition] = kadm.PartitionDetail{
				Topic:           td.Topic,
				Partition:       p.Partition,
				Leader:          p.Leader,
				LeaderEpoch:     p.LeaderEpoch,
				Replicas:        p.Replicas,
				ISR:             p.ISR,
				OfflineReplicas: p.OfflineReplicas,
				Err:             kerr.ErrorForCode(p.ErrorCode),
			}
		}

		// A topic refused by name comes back without one; it cannot be
		// keyed, and it is reported through the topic it was asked for.
		if td.Topic == "" {
			continue
		}

		m.Topics[td.Topic] = td
	}

	for _, t := range topics {
		if _, ok := m.Topics[t]; !ok {
			m.Topics[t] = kadm.TopicDetail{Topic: t, Err: kerr.UnknownTopicOrPartition}
		}
	}

	return m, nil
}

// listTopics is metadata's topics. With no names it lists every topic the
// console may describe; those it may not are left out, with their count.
func (c *Client) listTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, int, error) {
	m, err := c.metadata(ctx, topics...)
	if err != nil {
		return nil, 0, err
	}

	if len(topics) > 0 {
		return m.Topics, 0, nil
	}

	hidden := 0

	for name, td := range m.Topics {
		if td.Err != nil {
			delete(m.Topics, name)
			hidden++
		}
	}

	return m.Topics, hidden, nil
}
