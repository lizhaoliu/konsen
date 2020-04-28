package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/lizhaoliu/konsen/v2/core"
	"github.com/lizhaoliu/konsen/v2/net"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

var (
	clusterConfigPath string
	dbFilePath        string
	pprofPort         int
)

func init() {
	flag.StringVar(&clusterConfigPath, "cluster_config_path", "", "Cluster configuration file path.")
	flag.StringVar(&dbFilePath, "db_file_path", "", "Local DB file path.")
	flag.IntVar(&pprofPort, "pprof_port", 0, "pprof HTTP server port, 0 means no pprof.")
	flag.Parse()

	if clusterConfigPath == "" {
		logrus.Fatalf("cluster_config_path is unspecified.")
	}
	if dbFilePath == "" {
		logrus.Fatalf("db_file_path is unspecified.")
	}

	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logrus.SetLevel(logrus.InfoLevel)
}

func parseClusterConfig(configFilePath string) (*core.ClusterConfig, error) {
	buf, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster config file: %v", err)
	}
	cluster := &core.ClusterConfig{}
	if err := yaml.Unmarshal(buf, cluster); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cluster config file: %v", err)
	}

	if cluster.LocalEndpoint == "" {
		return nil, fmt.Errorf("local endpoint/address is unspecified")
	}

	numNodes := len(cluster.Endpoints)
	if numNodes%2 != 1 {
		return nil, fmt.Errorf("number of nodes in a cluster must be odd, got: %d", numNodes)
	}

	logrus.Infof("Cluster endpoints: %q", cluster.Endpoints)

	for _, endpoint := range cluster.Endpoints {
		if cluster.LocalEndpoint == endpoint {
			return cluster, nil
		}
	}

	return nil, fmt.Errorf("local server endpoint %q is not in cluster", cluster.LocalEndpoint)
}

func createClients(cluster *core.ClusterConfig) (map[string]core.RaftService, error) {
	clients := make(map[string]core.RaftService)
	for _, endpoint := range cluster.Endpoints {
		if endpoint != cluster.LocalEndpoint {
			c, err := net.NewRaftGRPCClient(net.RaftGRPCClientConfig{
				Endpoint: endpoint,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create GRPC client: %v", err)
			}
			clients[endpoint] = c
		}
	}
	return clients, nil
}

func main() {
	cluster, err := parseClusterConfig(clusterConfigPath)
	if err != nil {
		logrus.Fatalf("%v", err)
	}

	storage, err := core.NewBoltDB(core.BoltDBConfig{FilePath: dbFilePath})
	if err != nil {
		logrus.Fatalf("%v", err)
	}

	clients, err := createClients(cluster)
	if err != nil {
		logrus.Fatalf("%v", err)
	}

	sm, err := core.NewStateMachine(core.StateMachineConfig{
		Storage: storage,
		Cluster: cluster,
		Clients: clients,
	})
	if err != nil {
		logrus.Fatalf("Failed to create state machine: %v", err)
	}

	server := net.NewRaftGRPCServer(net.RaftGRPCServerConfig{
		Endpoint:     cluster.LocalEndpoint,
		StateMachine: sm,
	})
	go func() {
		server.Serve()
	}()
	go func() {
		sm.Run(context.Background())
	}()

	if pprofPort > 0 {
		go func() {
			if err := http.ListenAndServe(fmt.Sprintf(":%d", pprofPort), nil); err != nil {
				logrus.Errorf("Failed to start pprof server: %v", err)
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	sig := <-sigCh
	logrus.Infof("Syscall: %v", sig)

	sm.Close()
	server.Stop()
	storage.Close()
}
