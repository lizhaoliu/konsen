package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/lizhaoliu/konsen/v2/core"
	net2 "github.com/lizhaoliu/konsen/v2/net"
	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	clusterConfigPath string
	dbFilePath        string
)

func init() {
	flag.StringVar(&clusterConfigPath, "cluster_config_path", "", "Cluster configuration file path.")
	flag.StringVar(&dbFilePath, "db_file_path", "", "Local DB file path.")
	flag.Parse()
	if clusterConfigPath == "" {
		logrus.Fatalf("cluster_config_path is unspecified.")
	}
	if dbFilePath == "" {
		logrus.Fatalf("db_file_path is unspecified.")
	}

	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.InfoLevel)
}

func parseClusterConfig(configFilePath string) (*konsen.Cluster, error) {
	buf, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster config file: %v", err)
	}
	cluster := &konsen.Cluster{}
	if err := protojson.Unmarshal(buf, cluster); err != nil {
		return nil, fmt.Errorf("failed to parse cluster config file: %v", err)
	}

	numNodes := len(cluster.GetNodes())
	if numNodes%2 != 1 {
		return nil, fmt.Errorf("number of nodes in a cluster must be odd, got: %d", numNodes)
	}

	for _, node := range cluster.GetNodes() {
		if cluster.GetLocalNode().GetEndpoint() == node.GetEndpoint() {
			return cluster, nil
		}
	}

	return nil, fmt.Errorf("local node endpoint %q is not in cluster", cluster.GetLocalNode().GetEndpoint())
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

	sm, err := core.NewStateMachine(cluster, storage)
	if err != nil {
		logrus.Fatalf("Failed to create state machine: %v", err)
	}

	server := net2.NewRaftServer(net2.RaftServerConfig{
		Endpoint:     cluster.GetLocalNode().GetEndpoint(),
		StateMachine: sm,
	})
	go func() {
		server.Serve()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	sig := <-sigCh
	logrus.Infof("Syscall: %v", sig)

	sm.Close()
	server.Stop()
	storage.Close()
}
