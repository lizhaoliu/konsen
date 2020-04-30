package rpc

import (
	"context"
	"net"
	"time"

	"github.com/lizhaoliu/konsen/v2/core"
	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const rpcTimeout = 10 * time.Second

type RaftGRPCServer struct {
	endpoint string
	sm       *core.StateMachine
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

func (r *RaftGRPCServer) AppendData(ctx context.Context, req *konsen.AppendDataReq) (*konsen.AppendDataResp, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return r.sm.AppendData(ctx, req)
}

func (r *RaftGRPCServer) Serve() error {
	logrus.Infof("Start konsen server on: %q", r.endpoint)
	lis, err := net.Listen("tcp", r.endpoint)
	if err != nil {
		logrus.Fatalf("Failed to start server: %v", err)
	}
	server := grpc.NewServer()
	konsen.RegisterRaftServer(server, r)
	r.server = server
	return server.Serve(lis)
}

func (r *RaftGRPCServer) Stop() {
	if r.server != nil {
		r.server.GracefulStop()
	}
}
