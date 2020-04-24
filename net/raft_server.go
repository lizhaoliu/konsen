package net

import (
	"context"
	"io"

	"github.com/lizhaoliu/konsen/v2/core"
	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"github.com/sirupsen/logrus"
)

type RaftServerImpl struct {
	sm *core.StateMachine
}

type RaftServerImplConfig struct {
	StateMachine *core.StateMachine
}

func NewRaftServerImpl(config RaftServerImplConfig) *RaftServerImpl {
	impl := &RaftServerImpl{
		sm: config.StateMachine,
	}
	return impl
}

func (r *RaftServerImpl) AppendEntries(server konsen.Raft_AppendEntriesServer) error {
	for {
		req, err := server.Recv()
		if err == io.EOF {
			logrus.Infof("Received <EOF>, now exit.")
			return nil
		}
		if err != nil {
			return err
		}

		resp, err := r.sm.AppendEntries(context.Background(), req)
		err = server.Send(resp)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (r *RaftServerImpl) RequestVote(server konsen.Raft_RequestVoteServer) error {
	for {
		req, err := server.Recv()
		if err == io.EOF {
			logrus.Infof("Received <EOF>, now exit.")
			return nil
		}
		if err != nil {
			return err
		}

		resp, err := r.sm.RequestVote(context.Background(), req)
		err = server.Send(resp)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
