package kafka

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
)

// ─── Topic configuration ──────────────────────────────────

type ConfigChange struct {
	Key string `json:"key"`
	// Value nil with Op "delete" resets the key to its default.
	Value *string `json:"value"`
	// Op is set (default) or delete.
	Op string `json:"op"`
}

func alterConfigs(changes []ConfigChange) ([]kadm.AlterConfig, error) {
	out := make([]kadm.AlterConfig, 0, len(changes))

	for _, ch := range changes {
		if ch.Key == "" {
			return nil, ErrInvalid{fmt.Errorf("config key is required")}
		}

		ac := kadm.AlterConfig{Name: ch.Key, Value: ch.Value}

		switch ch.Op {
		case "", "set":
			if ch.Value == nil {
				return nil, ErrInvalid{fmt.Errorf("config %s: value is required", ch.Key)}
			}

			ac.Op = kadm.SetConfig
		case "delete":
			ac.Op = kadm.DeleteConfig
			ac.Value = nil
		default:
			return nil, ErrInvalid{fmt.Errorf("config %s: unknown op %q", ch.Key, ch.Op)}
		}

		out = append(out, ac)
	}

	return out, nil
}

func (c *Client) AlterTopicConfigs(ctx context.Context, topic string, changes []ConfigChange) error {
	if err := c.requireWritable(); err != nil {
		return err
	}

	acs, err := alterConfigs(changes)
	if err != nil {
		return err
	}

	resp, err := c.admin.AlterTopicConfigs(ctx, acs, topic)
	if err != nil {
		return fmt.Errorf("alter topic configs; %w", err)
	}

	r, err := resp.On(topic, nil)
	if err != nil {
		return fmt.Errorf("alter topic configs; %w", err)
	}

	if r.Err != nil {
		return fmt.Errorf("alter topic configs; %w %s", r.Err, r.ErrMessage)
	}

	return nil
}

// ─── Partitions ───────────────────────────────────────────

func (c *Client) AddPartitions(ctx context.Context, topic string, total int) error {
	if err := c.requireWritable(); err != nil {
		return err
	}

	resp, err := c.admin.UpdatePartitions(ctx, total, topic)
	if err != nil {
		return fmt.Errorf("update partitions; %w", err)
	}

	r, err := resp.On(topic, nil)
	if err != nil {
		return fmt.Errorf("update partitions; %w", err)
	}

	if r.Err != nil {
		return fmt.Errorf("update partitions; %w %s", r.Err, r.ErrMessage)
	}

	return nil
}

// DeleteRecordsRequest truncates partitions up to (excluding) an offset.
type DeleteRecordsRequest struct {
	// Partitions empty means all.
	Partitions []int32 `json:"partitions"`
	// Strategy is all (up to the high watermark), offset or timestamp.
	Strategy  string `json:"strategy"`
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
}

type DeletedRecords struct {
	Partition    int32  `json:"partition"`
	LowWatermark int64  `json:"low_watermark"`
	Error        string `json:"error,omitempty"`
}

func (c *Client) DeleteRecords(ctx context.Context, topic string, req DeleteRecordsRequest) ([]DeletedRecords, error) {
	if err := c.requireWritable(); err != nil {
		return nil, err
	}

	ends, err := c.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets; %w", err)
	}

	var atTime kadm.ListedOffsets
	if req.Strategy == "timestamp" {
		if atTime, err = c.admin.ListOffsetsAfterMilli(ctx, req.Timestamp, topic); err != nil {
			return nil, fmt.Errorf("list offsets by time; %w", err)
		}
	}

	wanted := map[int32]bool{}
	for _, p := range req.Partitions {
		wanted[p] = true
	}

	offsets := kadm.Offsets{}

	ends.Each(func(e kadm.ListedOffset) {
		if e.Topic != topic || e.Err != nil || (len(wanted) > 0 && !wanted[e.Partition]) {
			return
		}

		target := e.Offset

		switch req.Strategy {
		case "offset":
			target = min(req.Offset, e.Offset)
		case "timestamp":
			if t, ok := atTime.Lookup(topic, e.Partition); ok && t.Err == nil && t.Offset >= 0 {
				target = t.Offset
			}
		}

		offsets.AddOffset(topic, e.Partition, target, -1)
	})

	if len(offsets) == 0 {
		return nil, ErrInvalid{fmt.Errorf("no partitions selected")}
	}

	switch req.Strategy {
	case "", "all", "offset", "timestamp":
	default:
		return nil, ErrInvalid{fmt.Errorf("unknown strategy %q", req.Strategy)}
	}

	resp, err := c.admin.DeleteRecords(ctx, offsets)
	if err != nil {
		return nil, fmt.Errorf("delete records; %w", err)
	}

	var out []DeletedRecords

	resp.Each(func(r kadm.DeleteRecordsResponse) {
		d := DeletedRecords{Partition: r.Partition, LowWatermark: r.LowWatermark}
		if r.Err != nil {
			d.Error = r.Err.Error()
		}

		out = append(out, d)
	})

	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })

	return out, nil
}

// ─── Brokers ──────────────────────────────────────────────

type LogDir struct {
	Broker      int32  `json:"broker"`
	Dir         string `json:"dir"`
	Size        int64  `json:"size"`
	TotalBytes  int64  `json:"total_bytes"`
	UsableBytes int64  `json:"usable_bytes"`
	Partitions  int    `json:"partitions"`
	Error       string `json:"error,omitempty"`
}

type BrokerDetail struct {
	Broker
	Configs []ConfigEntry `json:"configs"`
	LogDirs []LogDir      `json:"log_dirs"`
	// Leaders and Replicas count the partitions this broker hosts.
	Leaders  int `json:"leaders"`
	Replicas int `json:"replicas"`
}

func (c *Client) Broker(ctx context.Context, id int32) (*BrokerDetail, error) {
	meta, err := c.admin.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("metadata; %w", err)
	}

	var out *BrokerDetail

	for _, b := range brokers(meta) {
		if b.ID == id {
			out = &BrokerDetail{Broker: b, Configs: []ConfigEntry{}, LogDirs: []LogDir{}}
		}
	}

	if out == nil {
		return nil, fmt.Errorf("broker %d; %w", id, ErrNotFound)
	}

	for _, t := range meta.Topics {
		for _, p := range t.Partitions {
			if p.Leader == id {
				out.Leaders++
			}

			for _, r := range p.Replicas {
				if r == id {
					out.Replicas++
				}
			}
		}
	}

	var (
		configs    kadm.ResourceConfigs
		configsErr error
		dirs       kadm.DescribedLogDirs
		dirsErr    error
		wg         sync.WaitGroup
	)

	wg.Go(func() { configs, configsErr = c.admin.DescribeBrokerConfigs(ctx, id) })
	wg.Go(func() { dirs, dirsErr = c.admin.DescribeBrokerLogDirs(ctx, id, nil) })
	wg.Wait()

	if configsErr == nil {
		if rc, err := configs.On(strconv.Itoa(int(id)), nil); err == nil {
			out.Configs = configEntries(rc.Configs)
		}
	}

	if dirsErr == nil {
		for _, d := range dirs.Sorted() {
			ld := LogDir{
				Broker: id, Dir: d.Dir, Size: d.Size(),
				TotalBytes: d.TotalBytes, UsableBytes: d.UsableBytes,
			}

			for _, ps := range d.Topics {
				ld.Partitions += len(ps)
			}

			if d.Err != nil {
				ld.Error = d.Err.Error()
			}

			out.LogDirs = append(out.LogDirs, ld)
		}
	}

	return out, nil
}

func (c *Client) AlterBrokerConfigs(ctx context.Context, id int32, changes []ConfigChange) error {
	if err := c.requireWritable(); err != nil {
		return err
	}

	acs, err := alterConfigs(changes)
	if err != nil {
		return err
	}

	resp, err := c.admin.AlterBrokerConfigs(ctx, acs, id)
	if err != nil {
		return fmt.Errorf("alter broker configs; %w", err)
	}

	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("alter broker configs; %w %s", r.Err, r.ErrMessage)
		}
	}

	return nil
}

func configEntries(cfgs []kadm.Config) []ConfigEntry {
	out := make([]ConfigEntry, 0, len(cfgs))
	for _, cfg := range cfgs {
		out = append(out, ConfigEntry{
			Key:       cfg.Key,
			Value:     cfg.Value,
			Sensitive: cfg.Sensitive,
			Source:    cfg.Source.String(),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	return out
}

// topicSizes returns the on-disk size of the given topics across leader
// replicas and across all replicas. Errors are swallowed: sizes are best
// effort. Leaders come from topics, the metadata the caller already holds.
func (c *Client) topicSizes(ctx context.Context, topics kadm.TopicDetails) map[string][2]int64 {
	dirs, err := c.admin.DescribeAllLogDirs(ctx, topics.TopicsSet())
	if err != nil {
		return nil
	}

	out := map[string][2]int64{}

	dirs.Each(func(d kadm.DescribedLogDir) {
		for t, ps := range d.Topics {
			for _, p := range ps {
				if p.IsFuture {
					continue
				}

				s := out[t]
				s[1] += p.Size
				out[t] = s
			}
		}
	})

	// Leader-only sizes approximate the logical topic size.
	leaderSize := map[string]int64{}

	dirs.Each(func(d kadm.DescribedLogDir) {
		for t, ps := range d.Topics {
			mt, ok := topics[t]
			if !ok {
				continue
			}

			for p, part := range ps {
				if mp, ok := mt.Partitions[p]; ok && mp.Leader == d.Broker && !part.IsFuture {
					leaderSize[t] += part.Size
				}
			}
		}
	})

	for t, v := range leaderSize {
		s := out[t]
		s[0] = v
		out[t] = s
	}

	return out
}

// ─── Consumer group offsets ───────────────────────────────

// DeleteGroupOffsets removes committed offsets of a group for a topic; empty
// partitions means every partition the group committed for.
func (c *Client) DeleteGroupOffsets(ctx context.Context, group, topic string, partitions []int32) error {
	if err := c.requireWritable(); err != nil {
		return err
	}

	if len(partitions) == 0 {
		current, err := c.admin.FetchOffsetsForTopics(ctx, group, topic)
		if err != nil {
			return fmt.Errorf("fetch offsets; %w", err)
		}

		current.Each(func(o kadm.OffsetResponse) {
			if o.Topic == topic && o.Err == nil && o.At >= 0 {
				partitions = append(partitions, o.Partition)
			}
		})
	}

	set := kadm.TopicsSet{}
	set.Add(topic, partitions...)

	resp, err := c.admin.DeleteOffsets(ctx, group, set)
	if err != nil {
		return fmt.Errorf("delete offsets; %w", err)
	}

	if err := resp.Error(); err != nil {
		return fmt.Errorf("delete offsets; %w", err)
	}

	return nil
}
