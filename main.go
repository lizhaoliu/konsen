package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/chzyer/readline"
	"github.com/lizhaoliu/konsen/v2/core"
	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"github.com/lizhaoliu/konsen/v2/rpc"
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
			c, err := rpc.NewRaftGRPCClient(rpc.RaftGRPCClientConfig{
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
	ctx := context.Background()

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

	server := rpc.NewRaftGRPCServer(rpc.RaftGRPCServerConfig{
		Endpoint:     cluster.LocalEndpoint,
		StateMachine: sm,
	})
	go func() {
		if err := server.Serve(); err != nil {
			logrus.Fatalf("%v", err)
		}
	}()
	go func() {
		sm.Run(ctx)
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
	go func() {
		sig := <-sigCh
		logrus.Infof("Syscall: %v", sig)

		sm.Close()
		server.Stop()
		storage.Close()
	}()

	l, err := readline.New("\033[31m»\033[0m ")
	if err != nil {
		logrus.Fatalf("%v", err)
	}
	defer l.Close()

	logrus.SetOutput(l.Stderr())
	for {
		line, err := l.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "set "):
			data := line[4:]
			_, err := sm.AppendData(ctx, &konsen.AppendDataReq{Data: []byte(data)})
			if err != nil {
				logrus.Errorf("%v", err)
			}
		case strings.HasPrefix(line, "get "):
			key := line[4:]
			val, err := sm.GetValue(ctx, []byte(key))
			if err != nil {
				logrus.Errorf("%v", err)
			}
			logrus.Infof("%s", val)
		case line == "snapshot":
			s, err := sm.GetSnapshot(ctx)
			if err != nil {
				logrus.Errorf("%v", err)
			} else {
				buf, err := json.MarshalIndent(s, "", "  ")
				if err != nil {
					logrus.Errorf("%v", err)
				}
				logrus.Infof("\n%s", buf)
			}
		}
	}
}
