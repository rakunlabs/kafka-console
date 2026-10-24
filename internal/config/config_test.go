package config

import (
	"os"
	"reflect"
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

	for _, feature := range FeatureKeys() {
		for _, tt := range tests {
			t.Run(feature+"/"+tt.name, func(t *testing.T) {
				var f Features
				for _, ff := range featureFields {
					if ff.key == feature {
						*ff.field(&f) = tt.in
					}
				}

				if got := f.Enabled(feature); got != tt.want {
					t.Fatalf("enabled = %v, want %v", got, tt.want)
				}
			})
		}
	}
}

// Every Features field must be in the table, under its config key, or Over
// and Enabled would silently skip it.
func TestFeatureTableCoversEveryField(t *testing.T) {
	typ := reflect.TypeFor[Features]()
	if typ.NumField() != len(featureFields) {
		t.Fatalf("Features has %d fields, the table %d", typ.NumField(), len(featureFields))
	}

	for i := range typ.NumField() {
		field := typ.Field(i)

		var f Features
		on := true
		reflect.ValueOf(&f).Elem().Field(i).Set(reflect.ValueOf(&on))

		key := field.Tag.Get("cfg")
		found := false
		for _, ff := range featureFields {
			if ff.key == key {
				found = *ff.field(&f) == &on
			}
		}

		if !found {
			t.Errorf("field %s (%s) is not in the table under its key", field.Name, key)
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
	if !dev.Enabled(FeatureDeleteTopic) || dev.Enabled(FeatureSecurity) || dev.Enabled(FeatureCreateTopic) || !dev.Enabled(FeatureAlterTopic) {
		t.Fatalf("dev: cluster keys must win and unset ones follow global, got %+v", dev)
	}

	prod := cfg.ClusterFeatures("prod")
	if prod.Enabled(FeatureDeleteTopic) || prod.Enabled(FeatureCreateTopic) || !prod.Enabled(FeatureSecurity) {
		t.Fatalf("prod: must follow global, got %+v", prod)
	}
}

func TestValidateProtobufAndDocs(t *testing.T) {
	dir := t.TempDir()
	check := func(c *Cluster) error {
		c.Brokers = []string{"localhost:9092"}

		return (&Config{Clusters: map[string]*Cluster{"c": c}}).prepare()
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
		err := (&Config{Clusters: map[string]*Cluster{"c": c}}).prepare()

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
	if err := c.prepare(); err == nil || !strings.Contains(err.Error(), "mcp.output_dir is no longer supported") {
		t.Fatalf("an operator who sets the old directory must be told files no longer land there: %v", err)
	}
}

func TestValidateDurationsAndOrder(t *testing.T) {
	valid := func() *Config {
		return &Config{Clusters: map[string]*Cluster{
			"a": {Brokers: []string{"localhost:9092"}},
			"b": {Brokers: []string{"localhost:9092"}},
		}}
	}

	for name, tc := range map[string]struct {
		change func(*Config)
		want   string
	}{
		"idle timeout":  {func(c *Config) { c.Server.IdleTimeout = -1 }, "server.idle_timeout"},
		"global cache":  {func(c *Config) { c.Cache.Refresh = -1 }, "cache.refresh"},
		"cluster cache": {func(c *Config) { c.Clusters["b"].Cache.DiskUsage = -1 }, "clusters.b.cache.disk_usage"},
		// Several problems: the first by a fixed order, not map order.
		"limits order":   {func(c *Config) { c.Limits = Limits{Reads: -1, IdleReaders: -1, Streams: -1, MCPReads: -1} }, "limits.reads"},
		"clusters order": {func(c *Config) { c.Clusters["a"].Brokers, c.Clusters["b"].Brokers = nil, nil }, `cluster "a"`},
	} {
		for range 20 {
			c := valid()
			tc.change(c)

			if err := c.validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s: got %v, want %q", name, err, tc.want)
			}
		}
	}
}

func TestPrepareNormalizesAndReadsSecrets(t *testing.T) {
	dir := t.TempDir()
	secret := dir + "/password"
	if err := os.WriteFile(secret, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	c := &Config{
		Server: Server{BasePath: " /kafka/ "},
		Clusters: map[string]*Cluster{"c": {
			Brokers:        []string{"localhost:9092"},
			SchemaRegistry: &SchemaRegistry{URLs: []string{"http://sr"}, PasswordFile: secret},
			SASL:           &SASL{Mechanism: "SCRAM-SHA-512", PasswordFile: secret},
		}, "empty": {
			Brokers:        []string{"localhost:9092"},
			SchemaRegistry: &SchemaRegistry{},
			Docs:           &Docs{},
			SASL:           &SASL{},
		}},
	}
	if err := c.prepare(); err != nil {
		t.Fatal(err)
	}

	cl := c.Clusters["c"]
	if c.Server.BasePath != "/kafka" || cl.SASL.Mechanism != MechanismScramSHA512 || cl.SASL.Password != "s3cret" || cl.SchemaRegistry.Password != "s3cret" {
		t.Fatalf("base %q, mechanism %q, passwords %q %q", c.Server.BasePath, cl.SASL.Mechanism, cl.SASL.Password, cl.SchemaRegistry.Password)
	}

	if e := c.Clusters["empty"]; e.SchemaRegistry != nil || e.Docs != nil || e.SASL != nil {
		t.Fatalf("empty blocks kept: %+v", e)
	}

	cl.SASL.PasswordFile = dir + "/missing"
	if err := c.prepare(); err == nil || !strings.Contains(err.Error(), "read password file") {
		t.Fatalf("a missing password file must be reported: %v", err)
	}
}
