package datastore

import (
	"sort"
	"sync"

	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"google.golang.org/protobuf/proto"
)

// MemStorage is an in-memory implementation of Storage for testing.
type MemStorage struct {
	mu          sync.RWMutex
	currentTerm uint64
	votedFor    string
	logs        map[uint64]*konsen.Log // logIndex -> Log
	kv          map[string][]byte      // key -> value
}

func NewMemStorage() *MemStorage {
	return &MemStorage{
		logs: make(map[uint64]*konsen.Log),
		kv:   make(map[string][]byte),
	}
}

func (m *MemStorage) GetCurrentTerm() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentTerm, nil
}

func (m *MemStorage) SetCurrentTerm(term uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTerm = term
	return nil
}

func (m *MemStorage) GetVotedFor() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.votedFor, nil
}

func (m *MemStorage) SetVotedFor(candidateID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.votedFor = candidateID
	return nil
}

func (m *MemStorage) SetTermAndVotedFor(term uint64, candidateID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTerm = term
	m.votedFor = candidateID
	return nil
}

func (m *MemStorage) GetLog(logIndex uint64) (*konsen.Log, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	log, ok := m.logs[logIndex]
	if !ok {
		return nil, nil
	}
	return proto.Clone(log).(*konsen.Log), nil
}

func (m *MemStorage) GetLogsFrom(minLogIndex uint64) ([]*konsen.Log, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var indices []uint64
	for idx := range m.logs {
		if idx >= minLogIndex {
			indices = append(indices, idx)
		}
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })

	var logs []*konsen.Log
	for _, idx := range indices {
		logs = append(logs, proto.Clone(m.logs[idx]).(*konsen.Log))
	}
	return logs, nil
}

func (m *MemStorage) GetLogTerm(logIndex uint64) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	log, ok := m.logs[logIndex]
	if !ok {
		return 0, nil
	}
	return log.GetTerm(), nil
}

func (m *MemStorage) WriteLog(log *konsen.Log) error {
	return m.WriteLogs([]*konsen.Log{log})
}

func (m *MemStorage) WriteLogs(logs []*konsen.Log) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, log := range logs {
		m.logs[log.GetIndex()] = proto.Clone(log).(*konsen.Log)
	}
	return nil
}

func (m *MemStorage) LastLogIndex() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var maxIdx uint64
	for idx := range m.logs {
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	return maxIdx, nil
}

func (m *MemStorage) LastLogTerm() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var maxIdx uint64
	for idx := range m.logs {
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	if maxIdx == 0 {
		return 0, nil
	}
	return m.logs[maxIdx].GetTerm(), nil
}

func (m *MemStorage) DeleteLogsFrom(minLogIndex uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for idx := range m.logs {
		if idx >= minLogIndex {
			delete(m.logs, idx)
		}
	}
	return nil
}

func (m *MemStorage) SetValue(key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := make([]byte, len(value))
	copy(v, value)
	m.kv[string(key)] = v
	return nil
}

func (m *MemStorage) GetValue(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.kv[string(key)]
	if !ok {
		return nil, nil
	}
	result := make([]byte, len(v))
	copy(result, v)
	return result, nil
}

func (m *MemStorage) Close() error {
	return nil
}

// Verify MemStorage implements Storage at compile time.
var _ Storage = (*MemStorage)(nil)
