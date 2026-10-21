package kafka

import (
	"errors"
	"testing"
	"time"
)

// The offsets strategy moves only the listed partitions, each to its own
// target clamped to its range: what a restore or a copy from another
// group needs.
func TestResetToPerPartitionOffsets(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"orders": 3})
	ctx := t.Context()

	for p := range int32(3) {
		seed(t, c, "orders", p, 10, time.Now())
	}

	if _, err := c.CreateGroup(ctx, CreateGroupRequest{Group: "billing", Topic: "orders", Offsets: []PartitionOffset{{Partition: 1, Offset: 3}}}); err != nil {
		t.Fatal(err)
	}

	plan, err := c.ResetOffsets(ctx, "billing", ResetRequest{
		Topic:    "orders",
		Strategy: "offsets",
		Offsets:  []PartitionOffset{{Partition: 0, Offset: 4}, {Partition: 2, Offset: 99}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(plan) != 2 || plan[0].Partition != 0 || plan[0].To != 4 || plan[1].Partition != 2 || plan[1].To != 10 {
		t.Fatalf("plan = %+v, want p0→4 and p2 clamped to 10", plan)
	}

	committed, err := c.admin.FetchOffsets(ctx, "billing")
	if err != nil {
		t.Fatal(err)
	}

	if o, _ := committed.Lookup("orders", 0); o.At != 4 {
		t.Fatalf("p0 committed %d, want 4", o.At)
	}

	if o, _ := committed.Lookup("orders", 1); o.At != 3 {
		t.Fatalf("p1 committed %d; it was not listed and must keep 3", o.At)
	}

	bad := map[string][]PartitionOffset{
		"empty":     nil,
		"twice":     {{Partition: 0, Offset: 1}, {Partition: 0, Offset: 2}},
		"negative":  {{Partition: 0, Offset: -1}},
		"no such p": {{Partition: 7, Offset: 1}},
	}
	for name, offs := range bad {
		_, err := c.ResetOffsets(ctx, "billing", ResetRequest{Topic: "orders", Strategy: "offsets", Offsets: offs, DryRun: true})
		if !errors.As(err, &ErrInvalid{}) {
			t.Errorf("%s: %v, want invalid", name, err)
		}
	}
}
