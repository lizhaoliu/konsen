package core

import (
	"context"

	konsen "github.com/lizhaoliu/konsen/v2/proto"
)

// RaftService defines methods exposed by a Raft service.
type RaftService interface {
	// AppendEntries sends AppendEntries request to the remote server.
	AppendEntries(ctx context.Context, in *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error)

	// RequestVote sends RequestVote request to the remote server.
	RequestVote(ctx context.Context, in *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error)

	// AppendData sends AppendData request to the remote server.
	AppendData(ctx context.Context, in *konsen.AppendDataReq) (*konsen.AppendDataResp, error)
}
