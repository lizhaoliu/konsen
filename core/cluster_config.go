package core

// ClusterConfig is the configuration of a cluster.
type ClusterConfig struct {
	Endpoints     []string `yaml:"endpoints"`               // All endpoints/addresses of servers in this cluster.
	LocalEndpoint string   `yaml:"localEndpoint,omitempty"` // Endpoint/address of local server, should be "host:port".
}
