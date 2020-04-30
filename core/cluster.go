package core

import (
	"fmt"
	"io/ioutil"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// ClusterConfig is the configuration of a cluster.
type ClusterConfig struct {
	Servers         map[string]string `yaml:"servers"`                   // All servers in the cluster, a map of "serverName": "serverEndpoint".
	LocalServerName string            `yaml:"localServerName,omitempty"` // Local server name.
}

// ParseClusterConfig parses given config YAML file.
func ParseClusterConfig(cfgFilePath string) (*ClusterConfig, error) {
	buf, err := ioutil.ReadFile(cfgFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster config file: %v", err)
	}

	cluster := &ClusterConfig{}
	if err := yaml.Unmarshal(buf, cluster); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cluster config file: %v", err)
	}

	if cluster.LocalServerName == "" {
		return nil, fmt.Errorf("local server is unspecified")
	}

	numNodes := len(cluster.Servers)
	if numNodes%2 != 1 {
		return nil, fmt.Errorf("number of nodes in a cluster must be odd, got: %d", numNodes)
	}

	logrus.Infof("Cluster endpoints: %q", cluster.Servers)

	for server := range cluster.Servers {
		if cluster.LocalServerName == server {
			return cluster, nil
		}
	}

	return nil, fmt.Errorf("local server %q is not defined in cluster", cluster.LocalServerName)
}
