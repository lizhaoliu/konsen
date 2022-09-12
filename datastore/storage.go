package datastore

import (
	konsen "github.com/lizhaoliu/konsen/v2/proto"
)

// Storage provides an interface for a set of local persistent storage operations.
type Storage interface {
	// GetCurrentTerm returns the latest term server has seen (initialized to 0 on first boot, increases monotonically).
	GetCurrentTerm() (uint64, error)

	// SetCurrentTerm sets the current term.
	SetCurrentTerm(term uint64) error

	// GetVotedFor returns the candidate ID that received a vote in current term, empty/blank if none.
	GetVotedFor() (string, error)

	// SetVotedFor sets the candidate ID that received a vote in current term.
	SetVotedFor(candidateID string) error

	// GetLog returns the log entry on given index.
	GetLog(logIndex uint64) (*konsen.Log, error)

	// GetLogsFrom returns log entries with index greater equal than given index.
	GetLogsFrom(minLogIndex uint64) ([]*konsen.Log, error)

	// GetLogTerm returns the log term at given index.
	GetLogTerm(logIndex uint64) (uint64, error)

	// WriteLog writes the given log entry into storage.
	WriteLog(log *konsen.Log) error

	// WriteLogs writes the given log entries into storage.
	WriteLogs(logs []*konsen.Log) error

	// LastLogIndex returns the last(newest) log entry's index.
	LastLogIndex() (uint64, error)

	// LastLogTerm returns the last(newest) log entry's term.
	LastLogTerm() (uint64, error)

	// DeleteLogsFrom deletes logs with index greater equal than given index.
	DeleteLogsFrom(minLogIndex uint64) error

	// Following methods are not required by Raft.

	// SetValue stores a key-value pair.
	SetValue(key []byte, value []byte) error

	// GetValue returns value of a key.
	GetValue(key []byte) ([]byte, error)
}
