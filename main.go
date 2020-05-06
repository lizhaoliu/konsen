package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/lizhaoliu/konsen/v2/core"
	"github.com/lizhaoliu/konsen/v2/rpc"
	"github.com/lizhaoliu/konsen/v2/store"
	"github.com/lizhaoliu/konsen/v2/web/httpserver"
	"github.com/sirupsen/logrus"
)

var (
	clusterConfigPath string
	dbDir             string
)

func init() {
	flag.StringVar(&clusterConfigPath, "cluster_config_path", "", "Cluster configuration file path.")
	flag.StringVar(&dbDir, "db_dir", "db", "Local database directory path.")
	flag.Parse()

	if clusterConfigPath == "" {
		logrus.Fatalf("cluster_config_path is unspecified.")
	}
	if dbDir == "" {
		logrus.Fatalf("db_dir is unspecified.")
	}

	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logrus.SetLevel(logrus.InfoLevel)

	gin.SetMode(gin.ReleaseMode)
}

func createClients(cluster *core.ClusterConfig) (map[string]core.RaftService, error) {
	clients := make(map[string]core.RaftService)
	for server, endpoint := range cluster.Servers {
		if server != cluster.LocalServerName {
			c, err := rpc.NewRaftGRPCClient(rpc.RaftGRPCClientConfig{
				Endpoint: endpoint,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create GRPC client: %v", err)
			}
			clients[server] = c
		}
	}
	return clients, nil
}

func main() {
	ctx := context.Background()

	cluster, err := core.ParseClusterConfig(clusterConfigPath)
	if err != nil {
		logrus.Fatalf("%v", err)
	}

	if err := os.MkdirAll(dbDir, 0755); err != nil {
		logrus.Fatalf("Failed to create dir: %v", err)
	}
	storage, err := store.NewBadger(store.BadgerConfig{
		LogDir:   path.Join(dbDir, "logs"),
		StateDir: path.Join(dbDir, "state"),
	})
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

	raftServer := rpc.NewRaftGRPCServer(rpc.RaftGRPCServerConfig{
		Endpoint:     cluster.Servers[cluster.LocalServerName],
		StateMachine: sm,
	})

	httpSrv := httpserver.NewServer(httpserver.ServerConfig{
		StateMachine: sm,
		Address:      cluster.HttpServers[cluster.LocalServerName],
	})

	go func() {
		if err := raftServer.Serve(); err != nil {
			logrus.Fatalf("%v", err)
		}
	}()
	//
	go func() {
		sm.Run(ctx)
	}()
	go func() {
		if err := httpSrv.Run(); err != nil {
			logrus.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	// Starts pprof server.
	go func() {
		if err := http.ListenAndServe(":6060", nil); err != nil {
			logrus.Errorf("Failed to start pprof server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	sm.Close()
	raftServer.Stop()
	storage.Close()
}
