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
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lizhaoliu/konsen/v2/core"
	"github.com/lizhaoliu/konsen/v2/datastore"
	"github.com/lizhaoliu/konsen/v2/rpc"
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

func createClients(cluster *core.ClusterConfig) (map[string]*core.PeerClient, error) {
	clients := make(map[string]*core.PeerClient)
	for server, endpoint := range cluster.Servers {
		if server != cluster.LocalServerName {
			c, err := rpc.NewPeerGRPCClient(rpc.PeerGRPCClientConfig{
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

func closeClients(clients map[string]*core.PeerClient) {
	for server, client := range clients {
		if err := client.Close(); err != nil {
			logrus.Errorf("Failed to close client for %s: %v", server, err)
		}
	}
}

func main() {
	ctx := context.Background()

	cluster, err := core.ParseClusterConfig(clusterConfigPath)
	if err != nil {
		logrus.Fatalf("%v", err)
	}

	if err := os.MkdirAll(dbDir, 0700); err != nil {
		logrus.Fatalf("Failed to create dir: %v", err)
	}
	storage, err := datastore.NewBadger(datastore.BadgerConfig{
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
		StateMachine:    sm,
		Address:         cluster.HttpServers[cluster.LocalServerName],
		LocalServerName: cluster.LocalServerName,
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

	// Starts pprof server on localhost only (not exposed externally).
	go func() {
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			logrus.Errorf("Failed to start pprof server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	logrus.Info("Shutting down...")

	// Gracefully shutdown HTTP server with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logrus.Errorf("HTTP server shutdown error: %v", err)
	}

	raftServer.Stop()
	sm.Close()
	closeClients(clients)
	storage.Close()
	logrus.Info("Shutdown complete.")
}
