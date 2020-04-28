package net

import (
	"context"
	"net"

	"github.com/lizhaoliu/konsen/v2/core"
	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

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
	return r.sm.AppendEntries(ctx, req)
}

func (r *RaftGRPCServer) RequestVote(ctx context.Context, req *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
	return r.sm.RequestVote(ctx, req)
}

func (r *RaftGRPCServer) AppendData(ctx context.Context, req *konsen.AppendDataReq) (*konsen.AppendDataResp, error) {
	return r.sm.AppendData(ctx, req)
}

func (r *RaftGRPCServer) Serve() error {
	logrus.Infof("Starting Raft server on: %q", r.endpoint)
	lis, err := net.Listen("tcp", r.endpoint)
	if err != nil {
		logrus.Fatalf("Failed to start Raft server: %v", err)
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
