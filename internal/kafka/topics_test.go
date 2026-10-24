package kafka

import (
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

func TestSortTopics(t *testing.T) {
	base := []TopicSummary{
		{Name: "c", Partitions: 3, Messages: 10, Size: 5},
		{Name: "a", Partitions: 3, Messages: 30, Size: -1},
		{Name: "b", Partitions: 1, Messages: 10, Size: 7},
		{Name: "d", Partitions: 6, Messages: 0, Size: 0},
	}

	tests := []struct {
		key  string
		desc bool
		want []string
	}{
		{"name", false, []string{"a", "b", "c", "d"}},
		{"name", true, []string{"d", "c", "b", "a"}},
		// Ties keep name order in both directions, so pages never shuffle.
		{"partitions", false, []string{"b", "a", "c", "d"}},
		{"partitions", true, []string{"d", "a", "c", "b"}},
		{"messages", true, []string{"a", "b", "c", "d"}},
		{"size", true, []string{"b", "c", "d", "a"}},
	}

	for _, tt := range tests {
		ts := slices.Clone(base)
		sortTopics(ts, tt.key, tt.desc)

		got := make([]string, len(ts))
		for i, s := range ts {
			got[i] = s.Name
		}

		if !slices.Equal(got, tt.want) {
			t.Errorf("sort %s desc=%v = %v, want %v", tt.key, tt.desc, got, tt.want)
		}
	}
}

func TestPageOf(t *testing.T) {
	s := []int{0, 1, 2, 3, 4}

	tests := []struct {
		offset, limit int
		want          []int
	}{
		{0, 2, []int{0, 1}},
		{3, 2, []int{3, 4}},
		{4, 10, []int{4}},
		{5, 2, []int{}},
		{99, 2, []int{}},
		{-1, 2, []int{0, 1}},
		{2, 0, []int{2, 3, 4}},
	}

	for _, tt := range tests {
		if got := pageOf(s, tt.offset, tt.limit); !slices.Equal(got, tt.want) {
			t.Errorf("pageOf(offset=%d, limit=%d) = %v, want %v", tt.offset, tt.limit, got, tt.want)
		}
	}
}

func TestLogSizes(t *testing.T) {
	part := func(p int32, size int64, future bool) kadm.DescribedLogDirPartition {
		return kadm.DescribedLogDirPartition{Topic: "t", Partition: p, Size: size, IsFuture: future}
	}

	dirs := kadm.DescribedAllLogDirs{
		1: {"/a": {Broker: 1, Dir: "/a", Topics: kadm.DescribedLogDirTopics{"t": {0: part(0, 10, false), 1: part(1, 20, false)}}}},
		2: {"/b": {Broker: 2, Dir: "/b", Topics: kadm.DescribedLogDirTopics{"t": {0: part(0, 11, false), 1: part(1, 99, true)}}}},
	}
	topics := kadm.TopicDetails{"t": {Topic: "t", Partitions: kadm.PartitionDetails{
		0: {Partition: 0, Leader: 1},
		1: {Partition: 1, Leader: 2},
	}}}

	got := logSizes(dirs, topics)["t"]
	if got.leader != 10 || got.replicated != 41 || got.partitions[0] != 10 || len(got.partitions) != 1 {
		t.Fatalf("sizes = %+v", got)
	}
}
