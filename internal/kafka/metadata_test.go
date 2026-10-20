package kafka

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/rakunlabs/kafka-console/internal/config"
)

// One topic the console may not describe must not take the overview, topic
// list or name index down with it.
func TestOverviewSurvivesTopicAuthFailure(t *testing.T) {
	fake, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fake.Close)

	c, err := New("test", &config.Cluster{Brokers: fake.ListenAddrs()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	for _, name := range []string{"open", "secret"} {
		if _, err := c.admin.CreateTopic(t.Context(), 2, 1, nil, name); err != nil {
			t.Fatal(err)
		}
	}

	// Metadata answered by hand: "open" is readable, "secret" is refused.
	addr := fake.ListenAddrs()[0]
	host, portStr, _ := strings.Cut(addr, ":")
	port, _ := strconv.Atoi(portStr)

	fake.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		fake.KeepControl()

		req := kreq.(*kmsg.MetadataRequest)
		resp := req.ResponseKind().(*kmsg.MetadataResponse)
		resp.ControllerID = 0

		b := kmsg.NewMetadataResponseBroker()
		b.NodeID, b.Host, b.Port = 0, host, int32(port)
		resp.Brokers = append(resp.Brokers, b)

		topic := func(name string, refused bool) {
			rt := kmsg.NewMetadataResponseTopic()
			rt.Topic = kmsg.StringPtr(name)
			if refused {
				rt.ErrorCode = kerr.TopicAuthorizationFailed.Code
			} else {
				for p := range int32(2) {
					rp := kmsg.NewMetadataResponseTopicPartition()
					rp.Partition, rp.Leader, rp.Replicas, rp.ISR = p, 0, []int32{0}, []int32{0}
					rt.Partitions = append(rt.Partitions, rp)
				}
			}
			resp.Topics = append(resp.Topics, rt)
		}

		if req.Topics == nil {
			topic("open", false)
			topic("secret", true)
		} else {
			for _, t := range req.Topics {
				switch *t.Topic {
				case "open":
					topic("open", false)
				case "secret":
					topic("secret", true)
				default:
					rt := kmsg.NewMetadataResponseTopic()
					rt.Topic = t.Topic
					rt.ErrorCode = kerr.UnknownTopicOrPartition.Code
					resp.Topics = append(resp.Topics, rt)
				}
			}
		}

		return resp, nil, true
	})

	o, err := c.Overview(t.Context())
	if err != nil {
		t.Fatalf("overview failed: %v", err)
	}

	if o.Topics != 1 || o.HiddenTopics != 1 || o.Partitions != 2 {
		t.Fatalf("topics=%d hidden=%d partitions=%d, want 1, 1, 2", o.Topics, o.HiddenTopics, o.Partitions)
	}

	page, err := c.Topics(t.Context(), TopicQuery{Sort: "name"})
	if err != nil {
		t.Fatalf("topics failed: %v", err)
	}

	if page.Total != 1 || page.Topics[0].Name != "open" {
		t.Fatalf("topics = %+v", page.Topics)
	}

	if _, err := c.Topic(t.Context(), "secret"); !errors.Is(err, ErrNoTopicAccess) {
		t.Fatalf("topic secret: %v, want no access (not not-found)", err)
	}

	if _, err := c.Topic(t.Context(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("topic missing: %v, want not found", err)
	}
}

func TestDeleteRecordsGuards(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"t": 2})
	seed(t, c, "t", 0, 10, time.Now())
	seed(t, c, "t", 1, 10, time.Now())

	low := func(p int32) int64 {
		starts, err := c.admin.ListStartOffsets(t.Context(), "t")
		if err != nil {
			t.Fatal(err)
		}

		o, _ := starts.Lookup("t", p)

		return o.Offset
	}

	var invalid ErrInvalid

	for name, req := range map[string]DeleteRecordsRequest{
		"empty strategy":    {},
		"negative offset":   {Strategy: "offset", Offset: -1},
		"unknown strategy":  {Strategy: "everything"},
		"missing partition": {Strategy: "all", Partitions: []int32{7}},
	} {
		if _, err := c.DeleteRecords(t.Context(), "t", req); !errors.As(err, &invalid) {
			t.Errorf("%s: err = %v, want invalid", name, err)
		}
	}

	if low(0) != 0 || low(1) != 0 {
		t.Fatal("a refused request deleted records")
	}

	if _, err := c.DeleteRecords(t.Context(), "t", DeleteRecordsRequest{Strategy: "offset", Offset: 4, Partitions: []int32{0}}); err != nil {
		t.Fatal(err)
	}

	if low(0) != 4 || low(1) != 0 {
		t.Fatalf("after offset 4 on p0: lows %d, %d", low(0), low(1))
	}

	if _, err := c.DeleteRecords(t.Context(), "t", DeleteRecordsRequest{Strategy: "all"}); err != nil {
		t.Fatal(err)
	}

	if low(0) != 10 || low(1) != 10 {
		t.Fatalf("after all: lows %d, %d", low(0), low(1))
	}
}
