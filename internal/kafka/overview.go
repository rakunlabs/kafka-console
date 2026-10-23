package kafka

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

type Broker struct {
	ID         int32   `json:"id"`
	Host       string  `json:"host"`
	Port       int32   `json:"port"`
	Rack       *string `json:"rack,omitempty"`
	Controller bool    `json:"controller"`
	Leaders    int     `json:"leaders"`
	Replicas   int     `json:"replicas"`
	Version    string  `json:"version,omitempty"`
}

type Overview struct {
	Cluster           string   `json:"cluster"`
	ClusterID         string   `json:"cluster_id"`
	Brokers           []Broker `json:"brokers"`
	Controller        int32    `json:"controller"`
	Topics            int      `json:"topics"`
	InternalTopics    int      `json:"internal_topics"`
	Partitions        int      `json:"partitions"`
	UnderReplicated   int      `json:"under_replicated"`
	OfflinePartitions int      `json:"offline_partitions"`
	Groups            int      `json:"groups"`
	HasRegistry       bool     `json:"has_registry"`
	// Problems lists only the topics with an unhealthy partition, sorted by
	// name, so the payload stays small however many topics the cluster has.
	// Partitions has one character per partition in id order: "o" ok,
	// "u" under-replicated, "x" offline.
	Problems []TopicHealth `json:"problems"`
	// At is when the numbers were read; they may be served from memory for
	// a few seconds.
	At time.Time `json:"at"`
}

type TopicHealth struct {
	Topic      string `json:"topic"`
	Partitions string `json:"partitions"`
}

// DiskUsage is the cluster's on-disk size across every replica. It is read
// apart from the overview: it asks every broker for the size of every
// partition it holds, which on a large cluster takes far longer than the rest.
type DiskUsage struct {
	Size int64     `json:"size"`
	At   time.Time `json:"at"`
}

// Overview and disk usage are the first page everyone opens, and both are
// cluster-wide reads. They are kept in memory for config.Cache and shared
// between callers. A read gives up after readTimeout.
const readTimeout = 30 * time.Second

// Overview returns the cluster summary, from memory when a recent one is
// kept; fresh asks for a new read unless one finished moments ago.
func (c *Client) Overview(ctx context.Context, fresh bool) (*Overview, error) {
	return c.overviews.get(ctx, fresh, c.readOverview)
}

// DiskUsage returns the cluster's total size on disk, kept like Overview.
func (c *Client) DiskUsage(ctx context.Context, fresh bool) (*DiskUsage, error) {
	return c.diskUsage.get(ctx, fresh, c.readDiskUsage)
}

func (c *Client) readDiskUsage(ctx context.Context) (*DiskUsage, error) {
	dirs, err := c.admin.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("describe log dirs; %w", err)
	}

	var size int64
	dirs.Each(func(d kadm.DescribedLogDir) { size += d.Size() })

	return &DiskUsage{Size: size, At: time.Now()}, nil
}

func (c *Client) readOverview(ctx context.Context) (*Overview, error) {
	meta, err := c.metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("metadata; %w", err)
	}

	o := &Overview{
		Cluster:     c.Name,
		ClusterID:   meta.Cluster,
		Controller:  meta.Controller,
		Brokers:     brokers(meta),
		HasRegistry: c.registry != nil,
		Problems:    []TopicHealth{},
		At:          time.Now(),
	}

	index := map[int32]int{}
	for i, b := range o.Brokers {
		index[b.ID] = i
	}

	// These only need the metadata above, so they run together; one after
	// another each waits its own round trip.
	var (
		wg       sync.WaitGroup
		versions kadm.BrokersApiVersions
		verErr   error
		groups   = -1
	)
	wg.Go(func() { versions, verErr = c.admin.ApiVersions(ctx) })
	wg.Go(func() {
		if listed, err := c.admin.ListGroups(ctx); err == nil {
			groups = len(listed)
		}
	})

	o.countTopics(meta, index)

	wg.Wait()

	if verErr == nil {
		versions.Each(func(v kadm.BrokerApiVersions) {
			if i, ok := index[v.NodeID]; ok && v.Err == nil {
				o.Brokers[i].Version = v.VersionGuess()
			}
		})
	}

	o.Groups = max(groups, 0)

	return o, nil
}

// countTopics fills topic, partition, leader and health counts from metadata.
func (o *Overview) countTopics(meta kadm.Metadata, index map[int32]int) {
	for _, t := range meta.Topics.Sorted() {
		// A topic the console may not describe is left out. Brokers already
		// omit such topics from a full listing, so no count of them would be
		// reliable; one answered from the client cache still carries its
		// refusal here.
		if t.Err != nil {
			continue
		}

		if t.IsInternal {
			o.InternalTopics++
		} else {
			o.Topics++
		}

		marks := make([]byte, 0, len(t.Partitions))
		unhealthy := false

		for _, p := range t.Partitions.Sorted() {
			switch {
			case p.Leader < 0:
				marks = append(marks, 'x')
				unhealthy = true
			case len(p.ISR) < len(p.Replicas):
				marks = append(marks, 'u')
				unhealthy = true
			default:
				marks = append(marks, 'o')
			}

			o.Partitions++

			if i, ok := index[p.Leader]; ok {
				o.Brokers[i].Leaders++
			}

			for _, r := range p.Replicas {
				if i, ok := index[r]; ok {
					o.Brokers[i].Replicas++
				}
			}

			if p.Leader < 0 {
				o.OfflinePartitions++
			}

			if len(p.ISR) < len(p.Replicas) {
				o.UnderReplicated++
			}
		}

		if unhealthy {
			o.Problems = append(o.Problems, TopicHealth{Topic: t.Topic, Partitions: string(marks)})
		}
	}
}

func brokers(meta kadm.Metadata) []Broker {
	out := make([]Broker, 0, len(meta.Brokers))
	for _, b := range meta.Brokers {
		out = append(out, Broker{
			ID:         b.NodeID,
			Host:       b.Host,
			Port:       b.Port,
			Rack:       b.Rack,
			Controller: b.NodeID == meta.Controller,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}
