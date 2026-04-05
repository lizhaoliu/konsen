package datastore

import (
	"math"
	"testing"
)

func TestUint64BytesRoundtrip(t *testing.T) {
	tests := []uint64{
		0,
		1,
		255,
		256,
		65535,
		math.MaxUint32,
		math.MaxUint64,
		42,
		1234567890,
	}
	for _, v := range tests {
		b := uint64ToBytes(v)
		if len(b) != 8 {
			t.Errorf("uint64ToBytes(%d) returned %d bytes, want 8", v, len(b))
		}
		got := bytesToUint64(b)
		if got != v {
			t.Errorf("roundtrip failed: got %d, want %d", got, v)
		}
	}
}

func TestUint64BytesOrdering(t *testing.T) {
	// Big-endian encoding should preserve lexicographic ordering,
	// which is important for Badger/BoltDB key ordering.
	a := uint64ToBytes(1)
	b := uint64ToBytes(2)
	c := uint64ToBytes(1000)

	if string(a) >= string(b) {
		t.Error("expected bytes(1) < bytes(2) in lexicographic order")
	}
	if string(b) >= string(c) {
		t.Error("expected bytes(2) < bytes(1000) in lexicographic order")
	}
}
