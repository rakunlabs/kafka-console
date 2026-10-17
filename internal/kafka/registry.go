package kafka

import (
	"fmt"

	"github.com/rakunlabs/kafka-console/internal/config"
)

// Registry holds one Client per configured cluster.
type Registry struct {
	names   []string
	clients map[string]*Client
}

func NewRegistry(cfg *config.Config) (*Registry, error) {
	r := &Registry{clients: make(map[string]*Client, len(cfg.Clusters))}

	for _, name := range cfg.ClusterNames() {
		client, err := New(name, cfg.Clusters[name])
		if err != nil {
			r.Close()

			return nil, fmt.Errorf("cluster %q; %w", name, err)
		}

		r.names = append(r.names, name)
		r.clients[name] = client
	}

	return r, nil
}

func (r *Registry) Names() []string { return r.names }

func (r *Registry) Get(name string) (*Client, bool) {
	c, ok := r.clients[name]

	return c, ok
}

func (r *Registry) Close() {
	for _, c := range r.clients {
		c.Close()
	}
}
