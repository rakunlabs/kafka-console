package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/rakunlabs/kafka-console/internal/config"
)

// restrictedCluster is a cluster where the console's principal may see
// everything except one topic ("secret") and one group ("hidden-group"),
// the way a shared production cluster often looks. An admin seeds data.
func restrictedCluster(t *testing.T) (console, admin *Client) {
	t.Helper()

	allow := func(res kmsg.ACLResourceType, name string, pattern kmsg.ACLResourcePatternType, op kmsg.ACLOperation) kfake.ACL {
		return kfake.ACL{Resource: res, Name: name, Pattern: pattern, Operation: op, Allow: true}
	}
	deny := func(res kmsg.ACLResourceType, name string, op kmsg.ACLOperation) kfake.ACL {
		return kfake.ACL{Resource: res, Name: name, Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: op}
	}

	lit, all := kmsg.ACLResourcePatternTypeLiteral, kmsg.ACLOperationAll
	fake, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.EnableACLs(),
		kfake.EnableSASL(),
		kfake.Superuser("PLAIN", "admin", "admin"),
		kfake.User("PLAIN", "console", "console",
			allow(kmsg.ACLResourceTypeTopic, "*", lit, all),
			allow(kmsg.ACLResourceTypeGroup, "*", lit, all),
			allow(kmsg.ACLResourceTypeCluster, "kafka-cluster", lit, all),
			deny(kmsg.ACLResourceTypeTopic, "secret", all),
			deny(kmsg.ACLResourceTypeGroup, "hidden-group", all),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fake.Close)

	connect := func(user string) *Client {
		c, err := New("test", &config.Cluster{
			Brokers: fake.ListenAddrs(),
			SASL:    &config.SASL{Mechanism: config.MechanismPlain, User: user, Password: user},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)

		return c
	}

	admin, console = connect("admin"), connect("console")

	ctx := t.Context()
	for _, name := range []string{"open", "secret"} {
		if _, err := admin.admin.CreateTopic(ctx, 2, 1, nil, name); err != nil {
			t.Fatal(err)
		}
		seed(t, admin, name, 0, 3, time.Now())
	}

	// A group reading both topics, and a group the console may not see.
	for _, g := range []string{"mixed", "hidden-group"} {
		offs := kadm.Offsets{}
		offs.AddOffset("open", 0, 1, -1)
		offs.AddOffset("secret", 0, 1, -1)
		if _, err := admin.admin.CommitOffsets(ctx, g, offs); err != nil {
			t.Fatal(err)
		}
	}

	return console, admin
}

// Every read the UI makes, as a principal that may not see one topic and
// one group. None may fail outright; each must leave the forbidden parts
// out or mark them.
func TestReadsWithPartialAccess(t *testing.T) {
	c, _ := restrictedCluster(t)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	check := func(name string, err error) {
		t.Helper()
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}

	// Touching the forbidden topic by name first puts its refusal in the
	// client's metadata cache, which later all-topic reads are answered
	// from. That is the order that made the overview fail "sometimes":
	// kadm then fails the whole listing on that one refused topic.
	if _, err := c.Topic(ctx, "secret"); !errors.Is(err, ErrNoTopicAccess) {
		t.Errorf("topic secret: %v, want no access", err)
	}

	// One full listing marks the cache as covering every topic; the next
	// is answered from it, refusal included, and kadm fails on it.
	if _, err := c.admin.Metadata(ctx); err != nil {
		t.Fatalf("setup: first full listing: %v", err)
	}

	if _, err := c.admin.ListTopicsWithInternal(ctx); err == nil {
		t.Fatal("setup: the cached refusal should fail kadm's own listing; the test no longer covers the cache path")
	}

	g, err := c.Group(ctx, "mixed")
	check("group mixed", err)
	if err == nil {
		var secretRows, deniedRows int
		for _, p := range g.Partitions {
			if p.Topic == "secret" {
				secretRows++
				if p.NoAccess {
					deniedRows++
				}
			}
		}
		if secretRows == 0 || deniedRows != secretRows {
			t.Errorf("group mixed: secret rows %d, marked no access %d", secretRows, deniedRows)
		}
	}

	// Brokers leave forbidden topics out of a full listing, or report them
	// refused when the answer comes from the cache; either way they are
	// not counted.
	o, err := c.Overview(ctx)
	check("overview", err)
	if err == nil && o.Topics != 1 {
		t.Errorf("overview topics=%d, want 1", o.Topics)
	}

	page, err := c.Topics(ctx, TopicQuery{Sort: "name"})
	check("topics", err)
	if err == nil && (page.Total != 1 || page.Topics[0].Name != "open") {
		t.Errorf("topics = %+v", page.Topics)
	}

	for _, sort := range []string{"messages", "size"} {
		_, err := c.Topics(ctx, TopicQuery{Sort: sort})
		check("topics sorted by "+sort, err)
	}

	_, err = c.Names(ctx)
	check("names", err)

	_, err = c.TopicConsumers(ctx, "open")
	check("topic consumers", err)

	groups, err := c.Groups(ctx, GroupQuery{})
	check("groups", err)
	if err == nil {
		for _, g := range groups.Groups {
			if g.Name == "hidden-group" {
				t.Errorf("groups lists hidden-group: %+v", g)
			}
		}
	}

	_, err = c.Groups(ctx, GroupQuery{SortByLag: true})
	check("groups by lag", err)

	_, err = c.Topic(ctx, "open")
	check("topic open", err)

	_, err = c.TopicConsumers(ctx, "secret")
	check("topic consumers of a forbidden topic", err)

	_, err = c.Consume(ctx, "open", ConsumeRequest{Start: "newest", Limit: 10, MaxScan: 100})
	check("consume open", err)

	_, err = c.Broker(ctx, 0)
	check("broker", err)

	_, err = c.Reassignments(ctx, "open")
	check("reassignments", err)
}

// Group actions on a group that also reads a forbidden topic: working on
// the readable topic must not fail because of the other one, and acting
// on the forbidden one must say it is refused, not "not found".
func TestGroupActionsWithPartialAccess(t *testing.T) {
	c, _ := restrictedCluster(t)
	ctx := t.Context()

	// Warm the cache the way a session does before the actions.
	_, _ = c.Topic(ctx, "secret")
	_, _ = c.Overview(ctx)

	plan, err := c.ResetOffsets(ctx, "mixed", ResetRequest{Topic: "open", Strategy: "earliest", DryRun: true})
	if err != nil || len(plan) != 2 {
		t.Fatalf("reset preview on open: %v %+v", err, plan)
	}

	if _, err := c.ResetOffsets(ctx, "mixed", ResetRequest{Topic: "open", Strategy: "latest"}); err != nil {
		t.Fatalf("reset open: %v", err)
	}

	if _, err := c.ResetOffsets(ctx, "mixed", ResetRequest{Topic: "secret", Strategy: "latest", DryRun: true}); !errors.Is(err, ErrNoTopicAccess) {
		t.Fatalf("reset secret: %v, want no access", err)
	}

	if err := c.DeleteGroupOffsets(ctx, "mixed", "open", nil); err != nil {
		t.Fatalf("delete offsets of open: %v", err)
	}

	if _, err := c.Group(ctx, "hidden-group"); !errors.Is(err, ErrNoGroupAccess) {
		t.Fatalf("hidden-group: %v, want no access", err)
	}
}

// Reading or writing a forbidden topic is refused with a no-access error
// the server turns into 403, and does not hang until a timeout.
func TestForbiddenTopicIsRefusedQuickly(t *testing.T) {
	c, _ := restrictedCluster(t)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	for name, run := range map[string]func() error{
		"consume": func() error {
			_, err := c.Consume(ctx, "secret", ConsumeRequest{Start: "newest", Limit: 10, MaxScan: 100})
			return err
		},
		"message": func() error {
			_, err := c.Message(ctx, "secret", RecordRef{Partition: 0, Offset: 0})
			return err
		},
		"consumers": func() error {
			_, err := c.TopicConsumers(ctx, "secret")
			return err
		},
	} {
		start := time.Now()
		err := run()
		took := time.Since(start)

		switch {
		case name == "consumers" && err == nil:
			// No group is visible as reading it; an empty list is fine.
		case err == nil:
			t.Errorf("%s: no error", name)
		case !isDenied(err):
			t.Errorf("%s: %v, want an authorization error", name, err)
		}

		if took > 3*time.Second {
			t.Errorf("%s took %v", name, took)
		}
	}
}

func isDenied(err error) bool {
	var auth *kadm.AuthError

	return errors.Is(err, ErrNoTopicAccess) || errors.As(err, &auth) ||
		errors.Is(err, kerr.TopicAuthorizationFailed)
}
