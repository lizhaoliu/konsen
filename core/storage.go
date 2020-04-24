package core

import konsen "github.com/lizhaoliu/konsen/v2/proto_gen"

// Storage provides an interface for a set of persistent storage operations.
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

	// PutLog stores the given log entry into storage.
	PutLog(log *konsen.Log) error

	// PutLogs stores the given log entries into storage.
	PutLogs(logs []*konsen.Log) error

	// FirstLogIndex returns the first(oldest) log entry's index.
	FirstLogIndex() (uint64, error)

	// LastLogIndex returns the last(newest) log entry's index.
	LastLogIndex() (uint64, error)

	//
	DeleteLogs(minLogIndex uint64) error
}
