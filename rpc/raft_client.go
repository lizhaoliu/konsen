package rpc

import (
	"context"
	"time"

	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const defaultConnectionTimeout = 30 * time.Second

// RaftClient is a generic interface for Raft client.
type RaftClient interface {
	// AppendEntries sends AppendEntries request to the remote server.
	AppendEntries(ctx context.Context, in *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error)

	// RequestVote sends RequestVote request to the remote server.
	RequestVote(ctx context.Context, in *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error)
}

type RaftGRPCClient struct {
	conn   *grpc.ClientConn
	client konsen.RaftClient
}

type RaftGRPCClientConfig struct {
	Endpoint          string
	ConnectionTimeout time.Duration
}

func NewRaftGRPCClient(config RaftGRPCClientConfig) (*RaftGRPCClient, error) {
	if config.ConnectionTimeout == 0 {
		config.ConnectionTimeout = defaultConnectionTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.ConnectionTimeout)
	defer cancel()
	conn, err := grpc.DialContext(
		ctx,
		config.Endpoint,
		grpc.WithInsecure(),
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

func (c *RaftGRPCClient) Close() error {
	return c.conn.Close()
}
