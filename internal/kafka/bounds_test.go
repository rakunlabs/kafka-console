package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func listed(topic string, offsets map[int32]int64, errs map[int32]error) kadm.ListedOffsets {
	ps := map[int32]kadm.ListedOffset{}
	for p, o := range offsets {
		ps[p] = kadm.ListedOffset{Topic: topic, Partition: p, Offset: o}
	}

	for p, err := range errs {
		ps[p] = kadm.ListedOffset{Topic: topic, Partition: p, Offset: -1, Err: err}
	}

	return kadm.ListedOffsets{topic: ps}
}

func TestOffsetBounds(t *testing.T) {
	b := offsetBounds{
		topic:  "t",
		starts: listed("t", map[int32]int64{0: 5, 2: 0, 1: 3}, map[int32]error{3: kerr.NotLeaderForPartition}),
		ends:   listed("t", map[int32]int64{0: 10, 1: 3}, map[int32]error{2: kerr.TopicAuthorizationFailed}),
		atTime: listed("t", map[int32]int64{0: 7, 1: -1}, nil),
	}

	if got := b.partitions(); len(got) != 4 || got[0] != 0 || got[3] != 3 {
		t.Fatalf("partitions = %v", got)
	}

	if lo, hi, err := b.partition(0); err != nil || lo != 5 || hi != 10 {
		t.Fatalf("partition 0 = %d %d %v", lo, hi, err)
	}

	if _, _, err := b.partition(9); !errors.As(err, new(ErrInvalid)) || !b.has(0) || b.has(9) || b.has(-1) {
		t.Fatalf("missing partition: %v", err)
	}

	if _, _, err := b.partition(2); !errors.Is(err, ErrNoTopicAccess) {
		t.Fatalf("end unknown: %v", err)
	}

	if _, _, err := b.partition(3); !errors.Is(err, kerr.NotLeaderForPartition) {
		t.Fatalf("start unknown: %v", err)
	}

	if at, err := b.at(0); err != nil || at != 7 {
		t.Fatalf("at 0 = %d %v", at, err)
	}

	if _, err := b.at(2); err == nil {
		t.Fatal("at of an unlisted partition must fail")
	}

	if got := timeOffset(b.atTime, "t", 1, 3); got != 3 {
		t.Fatalf("timeOffset with nothing that late = %d", got)
	}

	if s := partitionSet([]int32{1, 1, 4}); len(s) != 2 || !s[4] {
		t.Fatalf("partitionSet = %v", s)
	}
}
