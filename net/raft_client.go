package net

import (
	"context"
	"time"

	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const defaultConnectionTimeout = 120 * time.Second

type RaftClient struct {
	conn   *grpc.ClientConn
	client konsen.RaftClient
}

type RaftClientConfig struct {
	Endpoint          string
	ConnectionTimeout time.Duration
}

func NewRaftClient(config RaftClientConfig) (*RaftClient, error) {
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
	return &RaftClient{
		conn:   conn,
		client: client,
	}, nil
}

func (c *RaftClient) AppendEntries(ctx context.Context, in *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
	return c.client.AppendEntries(ctx, in, grpc.WaitForReady(false))
}

func (c *RaftClient) RequestVote(ctx context.Context, in *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
	return c.client.RequestVote(ctx, in, grpc.WaitForReady(false))
}

func (c *RaftClient) Close() error {
	return c.conn.Close()
}
