package rpc

import (
	"context"

	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type RaftGRPCClient struct {
	conn   *grpc.ClientConn
	client konsen.RaftClient
}

type RaftGRPCClientConfig struct {
	Endpoint string
}

func NewRaftGRPCClient(config RaftGRPCClientConfig) (*RaftGRPCClient, error) {
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

	client := konsen.NewRaftClient(conn)
	return &RaftGRPCClient{
		conn:   conn,
		client: client,
	}, nil
}

func (c *RaftGRPCClient) AppendEntries(ctx context.Context, in *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
	return c.client.AppendEntries(ctx, in, grpc.WaitForReady(false))
}

func (c *RaftGRPCClient) RequestVote(ctx context.Context, in *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
	return c.client.RequestVote(ctx, in, grpc.WaitForReady(false))
}

func (c *RaftGRPCClient) AppendData(ctx context.Context, in *konsen.AppendDataReq) (*konsen.AppendDataResp, error) {
	return c.client.AppendData(ctx, in, grpc.WaitForReady(false))
}

func (c *RaftGRPCClient) Close() error {
	return c.conn.Close()
}
