package rpc

import (
	"context"
	"net"
	"time"

	"github.com/lizhaoliu/konsen/v2/core"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const rpcTimeout = 10 * time.Second

// RaftGRPCServer implements the Raft consensus gRPC service.
type RaftGRPCServer struct {
	konsen.UnimplementedRaftServer

	endpoint string
	sm       *core.StateMachine
	kvServer *KVGRPCServer
	server   *grpc.Server
}

type RaftGRPCServerConfig struct {
	Endpoint     string
	StateMachine *core.StateMachine
}

func NewRaftGRPCServer(config RaftGRPCServerConfig) *RaftGRPCServer {
	s := &RaftGRPCServer{
		endpoint: config.Endpoint,
		sm:       config.StateMachine,
		kvServer: &KVGRPCServer{sm: config.StateMachine},
	}
	return s
}

func (r *RaftGRPCServer) AppendEntries(ctx context.Context, req *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return r.sm.AppendEntries(ctx, req)
}

func (r *RaftGRPCServer) RequestVote(ctx context.Context, req *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return r.sm.RequestVote(ctx, req)
}

func (r *RaftGRPCServer) Serve() error {
	logrus.Infof("Start konsen server on: %q", r.endpoint)
	lis, err := net.Listen("tcp", r.endpoint)
	if err != nil {
		logrus.Fatalf("Failed to start server: %v", err)
	}
	server := grpc.NewServer()
	konsen.RegisterRaftServer(server, r)
	konsen.RegisterKVServiceServer(server, r.kvServer)
	r.server = server
	return server.Serve(lis)
}

func (r *RaftGRPCServer) Stop() {
	if r.server != nil {
		r.server.GracefulStop()
	}
}

// KVGRPCServer implements the KVService gRPC service.
type KVGRPCServer struct {
	konsen.UnimplementedKVServiceServer

	sm *core.StateMachine
}

func (k *KVGRPCServer) Put(ctx context.Context, req *konsen.PutReq) (*konsen.PutResp, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return k.sm.Put(ctx, req)
}

func (k *KVGRPCServer) Get(ctx context.Context, req *konsen.GetReq) (*konsen.GetResp, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	val, err := k.sm.GetValue(ctx, req.GetKey())
	if err != nil {
		return &konsen.GetResp{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &konsen.GetResp{Success: true, Value: val}, nil
}
