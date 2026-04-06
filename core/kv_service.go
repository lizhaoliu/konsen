package core

import (
	"context"
	"io"

	konsen "github.com/lizhaoliu/konsen/v2/proto"
)

// KVService defines application-level RPCs for reading and writing data.
type KVService interface {
	// Put writes data into the replicated log.
	Put(ctx context.Context, in *konsen.PutReq) (*konsen.PutResp, error)

	// Get reads a value by key.
	Get(ctx context.Context, in *konsen.GetReq) (*konsen.GetResp, error)
}

// PeerClient groups the Raft consensus and KV application clients for a single remote peer.
// Both clients share the same underlying connection.
type PeerClient struct {
	Raft   RaftService
	KV     KVService
	Closer io.Closer
}

// Close closes the shared connection to the remote peer.
func (p *PeerClient) Close() error {
	if p.Closer != nil {
		return p.Closer.Close()
	}
	return nil
}
