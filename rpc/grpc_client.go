package rpc

import (
	"context"

	"github.com/lizhaoliu/konsen/v2/core"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// peerGRPCClient implements both core.RaftService and core.KVService
// over a single shared gRPC connection.
type peerGRPCClient struct {
	conn       *grpc.ClientConn
	raftClient konsen.RaftClient
	kvClient   konsen.KVServiceClient
}

type PeerGRPCClientConfig struct {
	Endpoint string
}

func NewPeerGRPCClient(config PeerGRPCClientConfig) (*core.PeerClient, error) {
	// grpc.NewClient creates a lazy connection - it connects on first RPC call.
	// Connection failures are handled via WaitForReady(false) on each RPC and
	// the context timeout passed to each call.
	conn, err := grpc.NewClient(
		config.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TODO: enable secured connection.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{}),
	)
	if err != nil {
		return nil, err
	}

	c := &peerGRPCClient{
		conn:       conn,
		raftClient: konsen.NewRaftClient(conn),
		kvClient:   konsen.NewKVServiceClient(conn),
	}
	return &core.PeerClient{
		Raft:   c,
		KV:     c,
		Closer: c,
	}, nil
}

// RaftService implementation.

func (c *peerGRPCClient) AppendEntries(ctx context.Context, in *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
	return c.raftClient.AppendEntries(ctx, in, grpc.WaitForReady(false))
}

func (c *peerGRPCClient) RequestVote(ctx context.Context, in *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
	return c.raftClient.RequestVote(ctx, in, grpc.WaitForReady(false))
}

// KVService implementation.

func (c *peerGRPCClient) Put(ctx context.Context, in *konsen.PutReq) (*konsen.PutResp, error) {
	return c.kvClient.Put(ctx, in, grpc.WaitForReady(false))
}

func (c *peerGRPCClient) Get(ctx context.Context, in *konsen.GetReq) (*konsen.GetResp, error) {
	return c.kvClient.Get(ctx, in, grpc.WaitForReady(false))
}

// Close closes the shared gRPC connection.

func (c *peerGRPCClient) Close() error {
	return c.conn.Close()
}
