package config

import (
	"strings"
	"testing"
	"time"
)

func TestCacheForPrefersCluster(t *testing.T) {
	c := &Config{
		Cache: Cache{Overview: 15 * time.Second, DiskUsage: time.Minute, Refresh: 30 * time.Second},
		Clusters: map[string]*Cluster{
			"prod":  {Cache: ClusterCache{DiskUsage: 5 * time.Minute}},
			"local": {},
		},
	}

	if got := c.CacheFor("prod"); got.Overview != 15*time.Second || got.DiskUsage != 5*time.Minute || got.Refresh != 30*time.Second {
		t.Errorf("prod = %+v, want its disk usage and the global rest", got)
	}
	if got := c.CacheFor("local"); got != c.Cache {
		t.Errorf("local = %+v, want the global settings", got)
	}
}

func TestLimitsForPrefersCluster(t *testing.T) {
	c := &Config{
		Limits: Limits{Reads: 16, IdleReaders: 4, Streams: 32, MCPReads: 8},
		Clusters: map[string]*Cluster{
			"prod":  {Brokers: []string{"b:9092"}, Limits: ClusterLimits{Reads: 4, MCPReads: 2}},
			"local": {Brokers: []string{"b:9092"}},
		},
	}

	if got := c.LimitsFor("prod"); got != (Limits{Reads: 4, IdleReaders: 4, Streams: 32, MCPReads: 2}) {
		t.Errorf("prod = %+v, want its reads and MCP reads and the global rest", got)
	}
	if got := c.LimitsFor("local"); got != c.Limits {
		t.Errorf("local = %+v, want the global limits", got)
	}

	if err := c.validate(); err != nil {
		t.Fatalf("valid limits refused: %v", err)
	}

	c.Clusters["prod"].Limits.Streams = -1
	if err := c.validate(); err == nil || !strings.Contains(err.Error(), "clusters.prod.limits.streams") {
		t.Errorf("a negative cluster limit must be refused by name, got %v", err)
	}
}
