package config

import (
	"strings"
	"testing"
)

func TestFeaturesDefaultOn(t *testing.T) {
	on, off := true, false

	tests := []struct {
		name string
		in   *bool
		want bool
	}{
		{"unset", nil, true},
		{"true", &on, true},
		{"false", &off, false},
	}

	checks := map[string]func(*bool) bool{
		"security":       func(b *bool) bool { return Features{Security: b}.SecurityEnabled() },
		"create_topic":   func(b *bool) bool { return Features{CreateTopic: b}.CreateTopicEnabled() },
		"delete_topic":   func(b *bool) bool { return Features{DeleteTopic: b}.DeleteTopicEnabled() },
		"alter_topic":    func(b *bool) bool { return Features{AlterTopic: b}.AlterTopicEnabled() },
		"delete_records": func(b *bool) bool { return Features{DeleteRecords: b}.DeleteRecordsEnabled() },
		"delete_group":   func(b *bool) bool { return Features{DeleteGroup: b}.DeleteGroupEnabled() },
		"alter_broker":   func(b *bool) bool { return Features{AlterBroker: b}.AlterBrokerEnabled() },
	}

	for feature, enabled := range checks {
		for _, tt := range tests {
			t.Run(feature+"/"+tt.name, func(t *testing.T) {
				if got := enabled(tt.in); got != tt.want {
					t.Fatalf("enabled = %v, want %v", got, tt.want)
				}
			})
		}
	}
}

func TestClusterFeaturesOverrideGlobal(t *testing.T) {
	on, off := true, false
	cfg := &Config{
		Features: Features{DeleteTopic: &off, CreateTopic: &off},
		Clusters: map[string]*Cluster{
			"dev":  {Features: Features{DeleteTopic: &on, Security: &off}},
			"prod": {},
		},
	}

	dev := cfg.ClusterFeatures("dev")
	if !dev.DeleteTopicEnabled() || dev.SecurityEnabled() || dev.CreateTopicEnabled() || !dev.AlterTopicEnabled() {
		t.Fatalf("dev: cluster keys must win and unset ones follow global, got %+v", dev)
	}

	prod := cfg.ClusterFeatures("prod")
	if prod.DeleteTopicEnabled() || prod.CreateTopicEnabled() || !prod.SecurityEnabled() {
		t.Fatalf("prod: must follow global, got %+v", prod)
	}
}

func TestValidateProtobufAndDocs(t *testing.T) {
	dir := t.TempDir()
	check := func(c *Cluster) error {
		c.Brokers = []string{"localhost:9092"}

		return (&Config{Clusters: map[string]*Cluster{"c": c}}).validate()
	}

	for name, tc := range map[string]struct {
		c  *Cluster
		ok bool
	}{
		"docs dir":            {&Cluster{Docs: &Docs{Path: dir}}, true},
		"docs path unset":     {&Cluster{Docs: &Docs{}}, true},
		"docs missing dir":    {&Cluster{Docs: &Docs{Path: dir + "/nope"}}, false},
		"protobuf":            {&Cluster{Protobuf: &Protobuf{Paths: []string{dir}, Topics: []ProtobufTopic{{Topic: "t", Value: "a.B"}}}}, true},
		"protobuf no paths":   {&Cluster{Protobuf: &Protobuf{Topics: []ProtobufTopic{{Topic: "t", Value: "a.B"}}}}, false},
		"protobuf no message": {&Cluster{Protobuf: &Protobuf{Paths: []string{dir}, Topics: []ProtobufTopic{{Topic: "t"}}}}, false},
		"protobuf no topic":   {&Cluster{Protobuf: &Protobuf{Paths: []string{dir}, Topics: []ProtobufTopic{{Value: "a.B"}}}}, false},
	} {
		if err := check(tc.c); (err == nil) != tc.ok {
			t.Errorf("%s: err %v, want ok=%v", name, err, tc.ok)
		}
	}
}

func TestValidateColor(t *testing.T) {
	for in, want := range map[string]string{
		"#E5484D": "#e5484d",
		"e5484d":  "#e5484d",
		"#f80":    "#ff8800",
		"Blue":    "blue",
		"tomato":  "tomato",
		"bleu":    "",
		"#12345":  "",
		"#ggg":    "",
	} {
		c := &Cluster{Brokers: []string{"localhost:9092"}, Color: in}
		err := (&Config{Clusters: map[string]*Cluster{"c": c}}).validate()

		switch {
		case want == "" && err == nil:
			t.Errorf("%q: want an error", in)
		case want != "" && (err != nil || c.Color != want):
			t.Errorf("%q: got %q, %v; want %q", in, c.Color, err, want)
		}
	}
}

func TestClusterNamesOrder(t *testing.T) {
	at := func(n int) *int { return &n }

	c := &Config{Clusters: map[string]*Cluster{
		"local":   {},
		"prod":    {Order: at(1)},
		"staging": {Order: at(2)},
		"dev":     {},
		"perf":    {Order: at(2)},
		"neg":     {Order: at(-1)},
	}}

	got := strings.Join(c.ClusterNames(), ",")
	if want := "neg,prod,perf,staging,dev,local"; got != want {
		t.Fatalf("order %s, want %s", got, want)
	}

	plain := &Config{Clusters: map[string]*Cluster{"b": {}, "a": {}, "c": {}}}
	if got := strings.Join(plain.ClusterNames(), ","); got != "a,b,c" {
		t.Fatalf("without order %s, want a,b,c", got)
	}
}

func TestValidateRefusesTheRemovedMCPOutputDir(t *testing.T) {
	c := &Config{Clusters: map[string]*Cluster{"local": {Brokers: []string{"localhost:9092"}}}, MCP: MCP{OutputDir: "/tmp/exports"}}
	if err := c.validate(); err == nil || !strings.Contains(err.Error(), "mcp.output_dir is no longer supported") {
		t.Fatalf("an operator who sets the old directory must be told files no longer land there: %v", err)
	}
}
