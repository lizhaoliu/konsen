package store

import "encoding/binary"

var (
	currentTermKey = []byte("current_term")
	votedForKey    = []byte("voted_for")
)

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
