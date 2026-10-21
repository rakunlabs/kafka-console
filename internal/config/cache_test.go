package config

import (
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
