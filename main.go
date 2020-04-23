package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/golang/protobuf/proto"
	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"github.com/sirupsen/logrus"
)

var (
	clusterConfigFile string
)

func init() {
	flag.StringVar(&clusterConfigFile, "cluster_config_file", "", "")
	flag.Parse()
	//if clusterConfigFile == "" {
	//    logrus.Fatalf("cluster_config_file is unspecified.")
	//}

	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.TraceLevel)
}

func parseClusterConfig(configFilePath string) (*konsen.Cluster, error) {
	buf, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster config file: %v", err)
	}
	cluster := &konsen.Cluster{}
	if err := proto.UnmarshalText(string(buf), cluster); err != nil {
		return nil, fmt.Errorf("failed to parse cluster config file: %v", err)
	}

	numNodes := len(cluster.GetNodes())
	if numNodes%2 != 1 {
		return nil, fmt.Errorf("number of nodes in a cluster must be odd, got: %d", numNodes)
	}

	return cluster, nil
}

func main() {
	c := &konsen.Cluster{
		Nodes: []*konsen.Node{
			{Endpoint: "192.168.86.25:10001"},
			{Endpoint: "192.168.86.25:10002"},
		},
		LocalNode: &konsen.Node{Endpoint: "192.168.86.25:10001"},
	}
	f, err := os.Create("C:\\Users\\lizhaoliu\\repos\\konsen\\aaa.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := proto.MarshalText(f, c); err != nil {
		panic(err)
	}
	s := proto.MarshalTextString(c)
	logrus.Infof("%s", s)

	//cluster, err := parseClusterConfig(clusterConfigFile)
	//if err != nil {
	//    logrus.Fatalf("%v", err)
	//}
	//cluster.GetLocalNode()
	//
	//impl := net.NewRaftServerImpl(net.RaftServerImplConfig{
	//    Endpoint:     "192.168.86.25:32768",
	//    StateMachine: nil,
	//})
	//if err := impl.Start(); err != nil {
	//    logrus.Fatalf("%v", err)
	//}
}
