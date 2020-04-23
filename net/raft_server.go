package net

import (
	"fmt"
	"io"
	"net"

	"github.com/lizhaoliu/konsen/v2/core"
	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"google.golang.org/grpc"
)

type RaftServerImpl struct {
	endpoint string
	sm       *core.StateMachine
}

type RaftServerImplConfig struct {
	Endpoint     string
	StateMachine *core.StateMachine
}

func NewRaftServerImpl(config RaftServerImplConfig) *RaftServerImpl {
	impl := &RaftServerImpl{
		endpoint: config.Endpoint,
		sm:       config.StateMachine,
	}
	return impl
}

func (r *RaftServerImpl) AppendEntries(server konsen.Raft_AppendEntriesServer) error {
	for {
		req, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// TODO: implementation.
		req.GetTerm()
	}
}

func (r *RaftServerImpl) RequestVote(server konsen.Raft_RequestVoteServer) error {
	for {
		req, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// TODO: implementation.
		req.GetTerm()
	}
}

func (r *RaftServerImpl) Start() error {
	lis, err := net.Listen("tcp", r.endpoint)
	if err != nil {
		return fmt.Errorf("failed to start Raft server on %q: %v", r.endpoint, err)
	}
	server := grpc.NewServer()
	konsen.RegisterRaftServer(server, r)
	return server.Serve(lis)
}
