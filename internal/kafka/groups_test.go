package kafka

import (
	"slices"
	"testing"
	"time"
)

// Groups pages in the order the list asks for: by name either way, or by
// lag, with the page cut after sorting so later pages continue the order.
func TestGroupsSortAndPage(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"orders": 1})
	ctx := t.Context()
	seed(t, c, "orders", 0, 10, time.Now())

	// Each group's lag is 10 minus its committed offset.
	for group, offset := range map[string]int64{"a": 9, "b": 2, "c": 5, "d": 0} {
		if _, err := c.CreateGroup(ctx, CreateGroupRequest{Group: group, Topic: "orders", Offsets: []PartitionOffset{{Partition: 0, Offset: offset}}}); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		query GroupQuery
		want  []string
	}{
		{GroupQuery{Sort: "name"}, []string{"a", "b", "c", "d"}},
		{GroupQuery{Sort: "name", Desc: true}, []string{"d", "c", "b", "a"}},
		{GroupQuery{Sort: "name", Desc: true, Offset: 1, Limit: 2}, []string{"c", "b"}},
		{GroupQuery{Sort: "lag", Desc: true}, []string{"d", "b", "c", "a"}},
		{GroupQuery{Sort: "lag"}, []string{"a", "c", "b", "d"}},
		{GroupQuery{Sort: "lag", Desc: true, Offset: 2, Limit: 2}, []string{"c", "a"}},
	}

	for _, tt := range tests {
		page, err := c.Groups(ctx, tt.query)
		if err != nil {
			t.Fatalf("%+v: %v", tt.query, err)
		}

		got := make([]string, len(page.Groups))
		for i, g := range page.Groups {
			got[i] = g.Name
		}

		if !slices.Equal(got, tt.want) || page.Total != 4 {
			t.Errorf("%+v = %v (total %d), want %v (total 4)", tt.query, got, page.Total, tt.want)
		}
	}
}
