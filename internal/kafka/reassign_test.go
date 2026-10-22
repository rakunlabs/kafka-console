package kafka

import (
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/rakunlabs/kafka-console/internal/config"
)

func TestBalance(t *testing.T) {
	// Six partitions, RF 2, everything piled on brokers 1 and 2; broker 3
	// was added and holds nothing.
	current := map[int32][]int32{}
	for p := range int32(6) {
		current[p] = []int32{1, 2}
	}

	plan := balance([]int32{1, 2, 3}, current)
	if len(plan) != 6 {
		t.Fatalf("plan has %d partitions", len(plan))
	}

	replicas, leaders := map[int32]int{}, map[int32]int{}
	moved := 0

	for _, r := range plan {
		if len(r.Replicas) != 2 {
			t.Fatalf("partition %d: RF changed to %d", r.Partition, len(r.Replicas))
		}

		if r.Replicas[0] == r.Replicas[1] {
			t.Fatalf("partition %d: broker twice", r.Partition)
		}

		for _, b := range r.Replicas {
			replicas[b]++
			if !contains(current[r.Partition], b) {
				moved++
			}
		}

		leaders[r.Replicas[0]]++
	}

	for _, b := range []int32{1, 2, 3} {
		if replicas[b] != 4 {
			t.Errorf("broker %d holds %d replicas, want 4", b, replicas[b])
		}

		if leaders[b] != 2 {
			t.Errorf("broker %d leads %d partitions, want 2", b, leaders[b])
		}
	}

	// 12 replicas over 3 brokers: only broker 3's share (4) has to move.
	if moved != 4 {
		t.Errorf("moved %d replicas, want 4", moved)
	}

	// A balanced topic stays as it is.
	again := map[int32][]int32{}
	for _, r := range plan {
		again[r.Partition] = r.Replicas
	}

	for _, r := range balance([]int32{1, 2, 3}, again) {
		if !equalReplicas(r.Replicas, again[r.Partition]) {
			t.Errorf("partition %d: balanced %v proposed again as %v", r.Partition, again[r.Partition], r.Replicas)
		}
	}

	// Replicas on a broker that is gone are moved to a live one.
	gone := balance([]int32{1, 2}, map[int32][]int32{0: {3, 1}})
	if r := gone[0].Replicas; contains(r, 3) || len(r) != 2 {
		t.Errorf("replica left on a missing broker: %v", r)
	}
}

func TestBalanceInvariants(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))

	for range 2000 {
		nb := 1 + rng.IntN(6)
		brokers := make([]int32, nb)
		for i := range brokers {
			brokers[i] = int32(i + 1)
		}

		rf := 1 + rng.IntN(nb)
		current := map[int32][]int32{}

		for p := range int32(1 + rng.IntN(60)) {
			// Start anywhere, including brokers that no longer exist (0, 9).
			pool := append([]int32{0, 9}, brokers...)
			rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
			current[p] = pool[:rf]
		}

		plan := balance(brokers, current)
		total := len(current) * rf
		limit := (total + nb - 1) / nb
		share := (len(current) + nb - 1) / nb
		load, leads := map[int32]int{}, map[int32]int{}

		for _, r := range plan {
			if len(r.Replicas) != rf {
				t.Fatalf("partition %d: rf %d, want %d", r.Partition, len(r.Replicas), rf)
			}

			seen := map[int32]bool{}
			for _, b := range r.Replicas {
				if b < 1 || int(b) > nb || seen[b] {
					t.Fatalf("partition %d: bad replicas %v (brokers %v)", r.Partition, r.Replicas, brokers)
				}

				seen[b] = true
				load[b]++
			}

			leads[r.Replicas[0]]++
		}

		for _, b := range brokers {
			if load[b] > limit {
				t.Fatalf("broker %d holds %d replicas, limit %d (brokers %d rf %d parts %d load %v)", b, load[b], limit, nb, rf, len(current), load)
			}

			if leads[b] > share {
				t.Fatalf("broker %d leads %d, share %d (rf %d, brokers %d, plan %v)", b, leads[b], share, rf, nb, plan)
			}
		}
	}
}

func TestReassign(t *testing.T) {
	fake, err := kfake.NewCluster(kfake.NumBrokers(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fake.Close)

	c, err := New("test", &config.Cluster{Brokers: fake.ListenAddrs()}, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	if _, err := c.admin.CreateTopic(t.Context(), 3, 2, nil, "moves"); err != nil {
		t.Fatal(err)
	}

	plan, err := c.BalancePlan(t.Context(), "moves")
	if err != nil || len(plan) != 3 {
		t.Fatalf("plan %v %v", plan, err)
	}

	// Checks refuse a changed replication factor, unknown brokers and
	// duplicates before anything is sent.
	for _, bad := range [][]int32{{0}, {0, 7}, {1, 1}} {
		if _, err := c.Reassign(t.Context(), "moves", ReassignRequest{Partitions: []Reassignment{{Partition: 0, Replicas: bad}}}); err == nil {
			t.Fatalf("replicas %v accepted", bad)
		}
	}

	meta, err := c.admin.Metadata(t.Context(), "moves")
	if err != nil {
		t.Fatal(err)
	}

	from := meta.Topics["moves"].Partitions[0].Replicas
	to := []int32{}
	for _, b := range []int32{0, 1, 2} {
		if !contains(from, b) {
			to = append(to, b)
		}
	}
	to = append(to, from[0])

	dry, err := c.Reassign(t.Context(), "moves", ReassignRequest{DryRun: true, Partitions: []Reassignment{{Partition: 0, Replicas: to}}})
	if err != nil || len(dry) != 1 || !equalReplicas(dry[0].To, to) {
		t.Fatalf("dry run %v %v", dry, err)
	}

	res, err := c.Reassign(t.Context(), "moves", ReassignRequest{Partitions: []Reassignment{{Partition: 0, Replicas: to}}})
	if err != nil || res[0].Error != "" {
		t.Fatalf("reassign %v %v", res, err)
	}

	// kfake accepts moves without carrying them out, so the replicas
	// themselves are not checked here.

	if _, err := c.Reassignments(t.Context(), "moves"); err != nil {
		t.Fatal(err)
	}

	if _, err := c.ElectPreferredLeaders(t.Context(), "moves", nil); err != nil {
		t.Fatal(err)
	}

	ro, err := New("ro", &config.Cluster{Brokers: fake.ListenAddrs(), ReadOnly: true}, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ro.Close)

	if _, err := ro.Reassign(t.Context(), "moves", ReassignRequest{Partitions: []Reassignment{{Partition: 0, Replicas: from}}}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only cluster: %v", err)
	}
}
