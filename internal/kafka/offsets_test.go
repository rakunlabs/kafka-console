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

// A reset names an existing group; committing to a mistyped name would
// create a new group instead.
func TestResetOffsetsRefusesAnUnknownGroup(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"orders": 1})
	seed(t, c, "orders", 0, 5, time.Now())

	for _, dry := range []bool{true, false} {
		_, err := c.ResetOffsets(t.Context(), "biling", ResetRequest{Topic: "orders", Strategy: "earliest", DryRun: dry})
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("dry run %v: %v, want not found", dry, err)
		}
	}

	groups, err := c.admin.ListGroups(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if _, created := groups["biling"]; created {
		t.Fatal("a reset created the group it was given")
	}

	_, err = c.AddGroupTopic(t.Context(), CreateGroupRequest{Group: "biling", Topic: "orders", Offsets: []PartitionOffset{{}}})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("add topic: %v, want not found", err)
	}
}

func TestResetAppliesReviewedTargets(t *testing.T) {
	for _, strategy := range []string{"latest", "shift", "offsets"} {
		t.Run(strategy, func(t *testing.T) {
			c := fakeCluster(t, map[string]int32{"orders": 1})
			seed(t, c, "orders", 0, 5, time.Now())
			if _, err := c.CreateGroup(t.Context(), CreateGroupRequest{Group: "billing", Topic: "orders", Offsets: []PartitionOffset{{Partition: 0, Offset: 1}}}); err != nil {
				t.Fatal(err)
			}
			preview, err := c.ResetOffsets(t.Context(), "billing", ResetRequest{Topic: "orders", Strategy: strategy, Shift: 2, Offsets: []PartitionOffset{{Partition: 0, Offset: 100}}, DryRun: true})
			if err != nil {
				t.Fatal(err)
			}
			seed(t, c, "orders", 0, 3, time.Now())
			actual, err := c.ResetOffsets(t.Context(), "billing", ResetRequest{Topic: "orders", Plan: preview})
			if err != nil {
				t.Fatal(err)
			}
			if actual[0].To != preview[0].To {
				t.Fatalf("preview=%+v actual=%+v", preview, actual)
			}
			commits, err := c.admin.FetchOffsets(t.Context(), "billing")
			if err != nil {
				t.Fatal(err)
			}
			if o, _ := commits.Lookup("orders", 0); o.At != preview[0].To {
				t.Fatalf("committed %d, reviewed %d", o.At, preview[0].To)
			}
		})
	}
}

func TestResetRefusesChangedReviewedState(t *testing.T) {
	for _, change := range []string{"commit", "retention"} {
		t.Run(change, func(t *testing.T) {
			c := fakeCluster(t, map[string]int32{"orders": 2})
			for p := range int32(2) {
				seed(t, c, "orders", p, 10, time.Now())
			}
			if _, err := c.CreateGroup(t.Context(), CreateGroupRequest{Group: "billing", Topic: "orders", Offsets: []PartitionOffset{{Partition: 0, Offset: 1}, {Partition: 1, Offset: 1}}}); err != nil {
				t.Fatal(err)
			}
			preview, err := c.ResetOffsets(t.Context(), "billing", ResetRequest{Topic: "orders", Strategy: "offset", Offset: 3, DryRun: true})
			if err != nil {
				t.Fatal(err)
			}
			if change == "commit" {
				_, err = c.ResetOffsets(t.Context(), "billing", ResetRequest{Topic: "orders", Strategy: "offsets", Offsets: []PartitionOffset{{Partition: 1, Offset: 2}}})
			} else {
				_, err = c.DeleteRecords(t.Context(), "orders", DeleteRecordsRequest{Partitions: []int32{1}, Strategy: "offset", Offset: 5})
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.ResetOffsets(t.Context(), "billing", ResetRequest{Topic: "orders", Plan: preview})
			var conflict ErrConflict
			if !errors.As(err, &conflict) {
				t.Fatalf("wanted re-preview conflict, got %v", err)
			}
			commits, err := c.admin.FetchOffsets(t.Context(), "billing")
			if err != nil {
				t.Fatal(err)
			}
			if o, _ := commits.Lookup("orders", 0); o.At != 1 {
				t.Fatalf("partial commit despite refusal: %d", o.At)
			}
		})
	}
}

func TestResetRefusesMalformedReviewedPlan(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"orders": 1})
	for _, plan := range [][]ResetPlan{
		{}, {{Partition: -1}}, {{From: -2}}, {{To: 3, Low: 4, High: 5}}, {{To: 6, High: 5}},
		{{High: 5}, {High: 5}},
	} {
		_, err := c.ResetOffsets(t.Context(), "billing", ResetRequest{Topic: "orders", Plan: plan})
		var invalid ErrInvalid
		if !errors.As(err, &invalid) {
			t.Fatalf("accepted malformed plan %+v: %v", plan, err)
		}
	}
}

func TestResetTarget(t *testing.T) {
	b := offsetBounds{topic: "t", atTime: listed("t", map[int32]int64{0: 7, 1: -1}, nil)}

	cases := []struct {
		req       ResetRequest
		p         int32
		from, own int64
		want      int64
		wantErr   bool
	}{
		{req: ResetRequest{Strategy: "earliest"}, want: 2},
		{req: ResetRequest{Strategy: "latest"}, want: 10},
		{req: ResetRequest{Strategy: "offset", Offset: 50}, want: 10},
		{req: ResetRequest{Strategy: "offsets"}, own: 1, want: 2},
		{req: ResetRequest{Strategy: "timestamp"}, want: 7},
		{req: ResetRequest{Strategy: "timestamp"}, p: 1, want: 10},
		{req: ResetRequest{Strategy: "timestamp"}, p: 2, wantErr: true},
		{req: ResetRequest{Strategy: "shift", Shift: -3}, from: 6, want: 3},
		{req: ResetRequest{Strategy: "shift", Shift: 3}, from: -1, want: 5},
		{req: ResetRequest{Strategy: "sideways"}, wantErr: true},
	}

	for _, tc := range cases {
		got, err := resetTarget(tc.req, b, tc.p, 2, 10, tc.from, tc.own)
		if (err != nil) != tc.wantErr || err == nil && got != tc.want {
			t.Errorf("%+v p%d: got %d, %v; want %d", tc.req, tc.p, got, err, tc.want)
		}
	}
}
