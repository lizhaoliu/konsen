package core

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lizhaoliu/konsen/v2/datastore"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMinTimeout  = 1000 * time.Millisecond
	defaultTimeoutSpan = 1000 * time.Millisecond
	defaultHeartbeat   = 100 * time.Millisecond

	defaultRequestTimeout = 5 * time.Second

	defaultSnapshotThreshold uint64 = 10000
	defaultSnapshotChunkSize        = 1024 * 1024 // 1MB
)

// StateMachine is the state machine that implements Raft algorithm: https://raft.github.io/raft.pdf.
// The state machine maintains a message queue (mailbox) internally, all requests/responses to the state machine are
// processed asynchronously (although the caller still observes a synchronized behavior): they are firstly put onto the
// message queue, then the message worker (goroutine) in turns takes a message at a time and processes it, and passes
// the result back to caller. The internal state is never directly accessed by goroutines other than the message worker.
// Internal errors will always cause a crash on the server since otherwise the state machine may be left in an
// inconsistent state.
type StateMachine struct {
	msgCh           chan interface{} // Main message queue.
	stopCh          chan struct{}    // Signals to stop the state machine.
	resetTimerCh    chan struct{}    // Signals to reset election timer when valid AppendEntries or vote grant occurs.
	stopHeartbeatCh chan struct{}    // Signals to stop the heartbeat loop when no longer leader.

	// Persistent state storage on all servers.
	storage datastore.Storage

	// Volatile state on all servers.
	commitIndex   uint64       // Index of highest log entry known to be committed (initialized to 0).
	lastApplied   uint64       // Index of highest log entry applied to state machine (initialized to 0).
	role          atomic.Int32 // Current role (atomic for safe reads from heartbeat goroutine).
	currentLeader string       // Current leader.

	// Volatile state on candidates.
	numVotes int

	// Volatile state on leaders (must be reinitialized after election).
	nextIndex  map[string]uint64 // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1).
	matchIndex map[string]uint64 // For each server, index of highest log entry known to be replicated on that server (initialized to 0, increases monotonically).

	// ClusterConfig info.
	cluster *ClusterConfig
	clients map[string]*PeerClient

	wg   sync.WaitGroup
	once sync.Once

	// Configurable timeouts.
	minTimeout  time.Duration
	timeoutSpan time.Duration
	heartbeat   time.Duration

	// condMap stores channels for goroutines waiting on specific log indices to be applied.
	// Key: log index (uint64), Value: chan struct{}
	// Managed via message passing through registerCondMsg and unregisterCondMsg.
	condMap map[uint64]chan struct{}

	// Snapshot state (volatile cache of persisted snapshot metadata, loaded on startup).
	snapshotIndex uint64 // lastIncludedIndex of the most recent snapshot.
	snapshotTerm  uint64 // lastIncludedTerm of the most recent snapshot.

	// Snapshot configuration.
	snapshotThreshold uint64 // Number of applied entries since last snapshot before triggering compaction.
	snapshotChunkSize int    // Chunk size in bytes for InstallSnapshot transfers.

	// Tracks in-flight snapshot transfers to followers to avoid redundant goroutines.
	snapshotInFlight map[string]bool

	// Follower-side buffer for assembling snapshot chunks from leader.
	pendingSnapshot *pendingSnapshotState
}

// pendingSnapshotState tracks an in-progress chunked snapshot transfer on the follower side.
type pendingSnapshotState struct {
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	data              []byte // assembled snapshot data
}

// StateMachineConfig
type StateMachineConfig struct {
	Storage datastore.Storage      // Local storage instance.
	Cluster *ClusterConfig         // Cluster configuration.
	Clients map[string]*PeerClient // A map of "server name": peer client.

	// Optional timeout overrides (zero values use defaults).
	MinTimeout  time.Duration // Minimum election timeout.
	TimeoutSpan time.Duration // Random span added to MinTimeout.
	Heartbeat   time.Duration // Heartbeat interval.

	// Snapshot configuration (zero values use defaults).
	SnapshotThreshold uint64 // Number of applied entries since last snapshot before triggering compaction.
	SnapshotChunkSize int    // Chunk size in bytes for InstallSnapshot transfers.
}

// Snapshot is a snapshot of the internal state of a state machine.
// TODO: Reduce the fields or remove this, as this is for debug purpose.
type Snapshot struct {
	CurrentTerm   uint64            // Current term.
	CommitIndex   uint64            // Index of highest log entry known to be committed.
	LastApplied   uint64            // Index of highest log entry applied to state machine.
	Role          konsen.Role       // Current role.
	CurrentLeader string            // Current leader.
	NextIndex     map[string]uint64 // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1).
	MatchIndex    map[string]uint64 // For each server, index of highest log entry known to be replicated on that server (initialized to 0, increases monotonically).
	LogIndices    []uint64          // Logs indices.
	LogTerms      []uint64          // Log terms.
	LogBytes      []int             // Log binary sizes.
	SnapshotIndex uint64            // lastIncludedIndex of the most recent snapshot (0 if none).
	SnapshotTerm  uint64            // lastIncludedTerm of the most recent snapshot (0 if none).
}

// appendEntriesWrap
type appendEntriesWrap struct {
	req *konsen.AppendEntriesReq
	ch  chan<- *konsen.AppendEntriesResp
}

// appendEntriesRespWrap
type appendEntriesRespWrap struct {
	resp   *konsen.AppendEntriesResp // AppendEntries response from remote server.
	req    *konsen.AppendEntriesReq  // Original AppendEntries request sent to remote server.
	server string                    // Remote server name that sends the AppendEntries response.
}

// requestVoteWrap
type requestVoteWrap struct {
	req *konsen.RequestVoteReq
	ch  chan<- *konsen.RequestVoteResp
}

// electionTimeoutMsg represents a message for election timeout event.
type electionTimeoutMsg struct{}

// appendEntriesMsg represents a message to send AppendEntries request to all nodes.
type appendEntriesMsg struct{}

// putMsg represents a message to write data into the state machine.
type putMsg struct {
	req *konsen.PutReq
	ch  chan<- *konsen.PutResp
}

// getSnapshotResp is the response for a getSnapshotMsg.
type getSnapshotResp struct {
	snapshot *Snapshot
	err      error
}

// getSnapshotMsg represents a message to generate a state snapshot.
type getSnapshotMsg struct {
	ch chan<- getSnapshotResp
}

// getValueResp is the response for a getValueMsg.
type getValueResp struct {
	value []byte
	err   error
}

// getValueMsg represents a message to retrieve a value by given key.
type getValueMsg struct {
	key []byte
	ch  chan<- getValueResp
}

// listKeysResp is the response for a listKeysMsg.
type listKeysResp struct {
	keys [][]byte
	err  error
}

// listKeysMsg represents a message to list stored KV keys.
type listKeysMsg struct {
	prefix []byte
	limit  int
	ch     chan<- listKeysResp
}

// registerCondMsg represents a message to register a condition channel for a log index.
type registerCondMsg struct {
	logIndex uint64
	condCh   chan struct{}
}

// unregisterCondMsg represents a message to unregister a condition channel for a log index.
type unregisterCondMsg struct {
	logIndex uint64
}

// healthCheckResp is the response for a healthCheckMsg.
type healthCheckResp struct {
	role          konsen.Role
	currentLeader string
}

// healthCheckMsg is a lightweight message to check if the message loop is responsive.
type healthCheckMsg struct {
	ch chan<- healthCheckResp
}

// StatusSnapshot is a lightweight snapshot for status reporting, without loading all logs.
type StatusSnapshot struct {
	CurrentTerm   uint64
	CommitIndex   uint64
	LastApplied   uint64
	Role          konsen.Role
	CurrentLeader string
	NextIndex     map[string]uint64
	MatchIndex    map[string]uint64
	LogCount      uint64
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// getStatusResp is the response for a getStatusMsg.
type getStatusResp struct {
	status *StatusSnapshot
	err    error
}

// getStatusMsg represents a message to retrieve a lightweight status snapshot.
type getStatusMsg struct {
	ch chan<- getStatusResp
}

// installSnapshotWrap wraps an incoming InstallSnapshot request.
type installSnapshotWrap struct {
	req *konsen.InstallSnapshotReq
	ch  chan<- *konsen.InstallSnapshotResp
}

// installSnapshotRespWrap wraps a response to an InstallSnapshot we sent.
type installSnapshotRespWrap struct {
	resp   *konsen.InstallSnapshotResp
	req    *konsen.InstallSnapshotReq
	server string
}

// NewStateMachine creates a new instance of the state machine.
func NewStateMachine(config StateMachineConfig) (*StateMachine, error) {
	if len(config.Cluster.Servers)%2 != 1 {
		return nil, fmt.Errorf("number of nodes in the cluster must be an odd number, got: %d", len(config.Cluster.Servers))
	}

	minTimeout := config.MinTimeout
	if minTimeout == 0 {
		minTimeout = defaultMinTimeout
	}
	timeoutSpan := config.TimeoutSpan
	if timeoutSpan == 0 {
		timeoutSpan = defaultTimeoutSpan
	}
	heartbeat := config.Heartbeat
	if heartbeat == 0 {
		heartbeat = defaultHeartbeat
	}

	snapshotThreshold := config.SnapshotThreshold
	if snapshotThreshold == 0 {
		snapshotThreshold = defaultSnapshotThreshold
	}
	snapshotChunkSize := config.SnapshotChunkSize
	if snapshotChunkSize == 0 {
		snapshotChunkSize = defaultSnapshotChunkSize
	}

	// Load snapshot metadata from persistent storage.
	snapshotIndex, snapshotTerm, err := config.Storage.GetSnapshotMeta()
	if err != nil {
		return nil, fmt.Errorf("failed to load snapshot metadata: %v", err)
	}

	sm := &StateMachine{
		msgCh:           make(chan interface{}),
		stopCh:          make(chan struct{}),
		resetTimerCh:    make(chan struct{}, 1), // Buffered to prevent blocking message loop
		stopHeartbeatCh: nil,                    // Created when becoming leader

		storage: config.Storage,
		cluster: config.Cluster,
		clients: config.Clients,

		// Initialize volatile state from snapshot: entries up to snapshotIndex have been applied.
		commitIndex: snapshotIndex,
		lastApplied: snapshotIndex,

		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),

		minTimeout:  minTimeout,
		timeoutSpan: timeoutSpan,
		heartbeat:   heartbeat,

		condMap: make(map[uint64]chan struct{}),

		snapshotIndex:     snapshotIndex,
		snapshotTerm:      snapshotTerm,
		snapshotThreshold: snapshotThreshold,
		snapshotChunkSize: snapshotChunkSize,

		snapshotInFlight: make(map[string]bool),
	}
	sm.setRole(konsen.Role_FOLLOWER)

	return sm, nil
}

// Run starts the state machine and blocks until done.
func (sm *StateMachine) Run(ctx context.Context) {
	sm.once.Do(func() {
		sm.startMessageLoop(ctx)
		sm.startElectionLoop(ctx)
		sm.wg.Wait()
	})
}

// getRole returns the current role (thread-safe).
func (sm *StateMachine) getRole() konsen.Role {
	return konsen.Role(sm.role.Load())
}

// setRole sets the current role (thread-safe).
func (sm *StateMachine) setRole(role konsen.Role) {
	sm.role.Store(int32(role))
}

// AppendEntries puts the incoming AppendEntries request in main message channel and waits for result.
func (sm *StateMachine) AppendEntries(ctx context.Context, req *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
	ch := make(chan *konsen.AppendEntriesResp, 1) // Buffered to prevent blocking message loop if caller times out
	select {
	case sm.msgCh <- appendEntriesWrap{req: req, ch: ch}:
	case <-sm.stopCh:
		return nil, fmt.Errorf("server has been shut down")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp, nil
	}
}

// RequestVote puts the incoming RequestVote request in main message channel and waits for result.
func (sm *StateMachine) RequestVote(ctx context.Context, req *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
	ch := make(chan *konsen.RequestVoteResp, 1) // Buffered to prevent blocking message loop if caller times out
	select {
	case sm.msgCh <- requestVoteWrap{req: req, ch: ch}:
	case <-sm.stopCh:
		return nil, fmt.Errorf("server has been shut down")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp, nil
	}
}

// grantVote creates a positive vote response for the candidate for given term.
func (sm *StateMachine) grantVote(term uint64, candidateID string) (*konsen.RequestVoteResp, error) {
	if err := sm.storage.SetVotedFor(candidateID); err != nil {
		return nil, err
	}
	// Only reset election timer when granting a vote (per Raft paper Section 5.2).
	sm.resetElectionTimer()
	log.Debugf("Granted vote for candidate %q for term %d", candidateID, term)
	return &konsen.RequestVoteResp{Term: term, VoteGranted: true}, nil
}

// maybeBecomeFollower checks if the given term is greater than current term, if true then update current term to
// the given term and become a follower, otherwise it simply returns the current term.
func (sm *StateMachine) maybeBecomeFollower(term uint64) (uint64, error) {
	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return 0, fmt.Errorf("failed to get current term: %v", err)
	}
	// If given term is greater than current term, update current term and become a follower.
	if term > currentTerm {
		// Atomically persist term and votedFor to prevent split-brain after crash.
		if err := sm.storage.SetTermAndVotedFor(term, ""); err != nil {
			return 0, fmt.Errorf("failed to set term to %d and reset votedFor: %v", term, err)
		}
		currentTerm = term
		sm.stopHeartbeatIfRunning()
		sm.setRole(konsen.Role_FOLLOWER)
		sm.numVotes = 0
		// Clear stale leader to prevent forwarding requests to self (which would
		// cause a nil pointer panic since the local server is not in the clients map).
		sm.currentLeader = ""
	}
	return currentTerm, nil
}

// stopHeartbeatIfRunning stops the heartbeat loop if it's running.
func (sm *StateMachine) stopHeartbeatIfRunning() {
	if sm.stopHeartbeatCh != nil {
		close(sm.stopHeartbeatCh)
		sm.stopHeartbeatCh = nil
	}
}

// resetElectionTimer signals the election timer to start a new round of election timeout countdown.
// Uses non-blocking send since the channel is buffered - if a reset is already pending, we don't need another.
func (sm *StateMachine) resetElectionTimer() {
	select {
	case sm.resetTimerCh <- struct{}{}:
	default:
		// Reset already pending, no need to send another
	}
}

// effectiveLogTerm returns the term for a given log index, accounting for the snapshot boundary.
// Called only from the message loop (single-threaded).
func (sm *StateMachine) effectiveLogTerm(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	if index == sm.snapshotIndex {
		return sm.snapshotTerm, nil
	}
	if index < sm.snapshotIndex {
		return 0, nil
	}
	return sm.storage.GetLogTerm(index)
}

// effectiveLastLogIndex returns the last log index, accounting for snapshot.
func (sm *StateMachine) effectiveLastLogIndex() (uint64, error) {
	lastIdx, err := sm.storage.LastLogIndex()
	if err != nil {
		return 0, err
	}
	if sm.snapshotIndex > lastIdx {
		return sm.snapshotIndex, nil
	}
	return lastIdx, nil
}

// effectiveLastLogTerm returns the term of the last log entry, accounting for snapshot.
func (sm *StateMachine) effectiveLastLogTerm() (uint64, error) {
	lastIdx, err := sm.storage.LastLogIndex()
	if err != nil {
		return 0, err
	}
	if lastIdx == 0 || lastIdx <= sm.snapshotIndex {
		return sm.snapshotTerm, nil
	}
	return sm.storage.LastLogTerm()
}

// handleAppendEntries handles a AppendEntries request.
func (sm *StateMachine) handleAppendEntries(req *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	currentTerm, err := sm.maybeBecomeFollower(req.GetTerm())
	if err != nil {
		return nil, err
	}

	// 1. Reply false if term < currentTerm.
	if req.GetTerm() < currentTerm {
		return &konsen.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	// At this point, the request is coming from a legit leader, and request term == currentTerm.
	// Per Raft paper Section 5.2: if a candidate receives AppendEntries from a leader with a term
	// at least as large as its own, it recognizes the leader as legitimate and steps down.
	if sm.getRole() == konsen.Role_CANDIDATE {
		sm.setRole(konsen.Role_FOLLOWER)
		sm.numVotes = 0
	}
	// Only reset election timer for valid AppendEntries from current leader (per Raft paper Section 5.2).
	sm.resetElectionTimer()
	sm.currentLeader = req.GetLeaderId()

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
	prevLogTerm, err := sm.effectiveLogTerm(req.GetPrevLogIndex())
	if err != nil {
		return nil, fmt.Errorf("failed to get log at index %d: %v", req.GetPrevLogIndex(), err)
	}
	if prevLogTerm != req.GetPrevLogTerm() {
		log.Debugf("Local prevLogTerm(%d) mismatches request prevLogTerm(%d).", prevLogTerm, req.GetPrevLogTerm())
		return &konsen.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	entries := req.GetEntries()
	// If there are new logs to append.
	if len(entries) > 0 {
		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
		// 4. Append any new entries not already in the log.
		startIdx := len(entries) // Initialize to end (no entries to write by default)
		for i, newLog := range entries {
			localLog, err := sm.storage.GetLog(newLog.GetIndex())
			if err != nil {
				return nil, fmt.Errorf("failed to get log at index %d: %v", newLog.GetIndex(), err)
			}
			if localLog == nil {
				// Entry doesn't exist, need to write from here
				startIdx = i
				break
			}
			if localLog.GetTerm() != newLog.GetTerm() {
				// Conflict: delete existing entries and write from here
				log.Debugf("Local logs conflict from index %d, now delete onwards.", newLog.GetIndex())
				if err := sm.storage.DeleteLogsFrom(newLog.GetIndex()); err != nil {
					return nil, fmt.Errorf("failed to delete logs from min index %d: %v", newLog.GetIndex(), err)
				}
				startIdx = i
				break
			}
			// Entry exists with matching term - skip it
		}

		// Only write if there are new entries to append
		if startIdx < len(entries) {
			log.Debugf("Append logs from index %d.", entries[startIdx].GetIndex())
			if err := sm.storage.WriteLogs(entries[startIdx:]); err != nil {
				return nil, fmt.Errorf("failed to write logs: %v", err)
			}
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
	if req.GetLeaderCommit() > sm.commitIndex {
		lastLogIndex, err := sm.effectiveLastLogIndex()
		if err != nil {
			return nil, fmt.Errorf("failed to get index of the last log: %v", err)
		}
		sm.commitIndex = req.GetLeaderCommit()
		if lastLogIndex < sm.commitIndex {
			sm.commitIndex = lastLogIndex
		}
	}

	// Reply with success.
	return &konsen.AppendEntriesResp{Term: currentTerm, Success: true}, nil
}

// handleAppendEntriesResp handles a AppendEntries response.
func (sm *StateMachine) handleAppendEntriesResp(
	resp *konsen.AppendEntriesResp,
	req *konsen.AppendEntriesReq,
	server string) error {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	currentTerm, err := sm.maybeBecomeFollower(resp.GetTerm())
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	// Terminate if no longer a leader.
	if sm.getRole() != konsen.Role_LEADER {
		return nil
	}

	if resp.GetSuccess() {
		numEntries := len(req.GetEntries())

		// Response is from a pure heartbeat, can return now.
		if numEntries == 0 {
			return nil
		}

		// Successful: update nextIndex and matchIndex for follower.
		sm.nextIndex[server] = req.GetPrevLogIndex() + uint64(numEntries) + 1
		sm.matchIndex[server] = req.GetPrevLogIndex() + uint64(numEntries)

		// If there exists N: N > commitIndex && a majority of matchIndex[i] ≥ N && log[N].term == currentTerm, then set commitIndex = N.
		// Only need to find the highest index, as lower index logs will always match if a higher one matches.
		for i := numEntries - 1; i >= 0; i-- {
			entry := req.GetEntries()[i]
			logIndex := entry.GetIndex()
			logTerm := entry.GetTerm()
			if logIndex > sm.commitIndex && sm.isLogOnMajority(logIndex) && logTerm == currentTerm {
				sm.commitIndex = logIndex
				break
			}
		}
	} else {
		// AppendEntries fails because of log inconsistency: decrement nextIndex and retry.
		// Guard against underflow: nextIndex must stay >= 1 (log indices start at 1).
		if sm.nextIndex[server] > 1 {
			sm.nextIndex[server]--
		}
	}

	return nil
}

// isLogOnMajority returns true if the log at given index is replicated on quorum.
func (sm *StateMachine) isLogOnMajority(logIndex uint64) bool {
	n := 1 // Already on this server.
	for server := range sm.cluster.Servers {
		if server != sm.cluster.LocalServerName && sm.matchIndex[server] >= logIndex {
			n++
		}
	}
	return n >= sm.majority()
}

// handleRequestVote handles a RequestVote request.
func (sm *StateMachine) handleRequestVote(req *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	currentTerm, err := sm.maybeBecomeFollower(req.GetTerm())
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}

	// 1. Reply false if term < currentTerm.
	if req.GetTerm() < currentTerm {
		return &konsen.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
	}

	// Now candidate's term == currentTerm.

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote.
	votedFor, err := sm.storage.GetVotedFor()
	if err != nil {
		return nil, fmt.Errorf("failed to get votedFor: %v", err)
	}

	// If already voted for another candidate, deny the vote.
	if votedFor != "" && votedFor != req.GetCandidateId() {
		return &konsen.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
	}

	// If candidate’s log is at least as up-to-date as receiver’s log, grant vote.

	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	lastLogTerm, err := sm.effectiveLastLogTerm()
	if err != nil {
		return nil, fmt.Errorf("failed to get last log's term: %v", err)
	}
	// Candidate's last log term is older, deny the vote.
	if req.GetLastLogTerm() < lastLogTerm {
		return &konsen.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
	}

	// Candidate's last log term is newer, grant vote.
	if req.GetLastLogTerm() > lastLogTerm {
		return sm.grantVote(currentTerm, req.GetCandidateId())
	}

	// If last logs have the same term, then whichever log is longer is more up-to-date.
	lastLogIndex, err := sm.effectiveLastLogIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to get last log's index: %v", err)
	}
	if req.GetLastLogIndex() >= lastLogIndex {
		return sm.grantVote(currentTerm, req.GetCandidateId())
	}

	// The candidate's log is not as up-to-date as receiver's, deny the vote.
	return &konsen.RequestVoteResp{Term: currentTerm, VoteGranted: false}, nil
}

// handleRequestVoteResp handles a RequestVote response.
func (sm *StateMachine) handleRequestVoteResp(resp *konsen.RequestVoteResp) error {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	currentTerm, err := sm.maybeBecomeFollower(resp.GetTerm())
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	if sm.getRole() != konsen.Role_CANDIDATE {
		return nil
	}

	if resp.GetVoteGranted() {
		sm.numVotes++
		// If votes received from majority of servers, become leader.
		if sm.numVotes >= sm.majority() {
			if err := sm.becomeLeader(currentTerm); err != nil {
				return fmt.Errorf("failed to become leader: %v", err)
			}
		}
	}

	return nil
}

// becomeLeader modifies internal state to become a leader, and starts the worker that periodically sends heartbeat.
func (sm *StateMachine) becomeLeader(term uint64) error {
	lastLogIndex, err := sm.effectiveLastLogIndex()
	if err != nil {
		return err
	}

	// Initialize volatile leader state before announcing leadership,
	// so a storage error doesn't leave us in a half-initialized leader state.
	for server := range sm.cluster.Servers {
		if server != sm.cluster.LocalServerName {
			sm.nextIndex[server] = lastLogIndex + 1
			sm.matchIndex[server] = 0
		}
	}

	log.Infof("Term - %d, leader - %q.", term, sm.cluster.LocalServerName)
	sm.setRole(konsen.Role_LEADER)
	sm.currentLeader = sm.cluster.LocalServerName

	// Create a new stop channel for the heartbeat loop.
	sm.stopHeartbeatCh = make(chan struct{})
	sm.startHeartbeatLoop(context.Background())

	return nil
}

// sendVoteRequests constructs a RequestVote request and sends it to all nodes in the cluster.
func (sm *StateMachine) sendVoteRequests(ctx context.Context) error {
	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return fmt.Errorf("failed to get current term: %v", err)
	}
	lastLogIndex, err := sm.effectiveLastLogIndex()
	if err != nil {
		return fmt.Errorf("failed to get last log index: %v", err)
	}
	lastLogTerm, err := sm.effectiveLastLogTerm()
	if err != nil {
		return fmt.Errorf("failed to get last log term: %v", err)
	}
	req := &konsen.RequestVoteReq{
		Term:         currentTerm,
		CandidateId:  sm.cluster.LocalServerName,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	for server := range sm.cluster.Servers {
		if server != sm.cluster.LocalServerName {
			sm.wg.Add(1)
			server := server
			go func() {
				defer sm.wg.Done()
				rpcCtx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
				defer cancel()
				resp, err := sm.clients[server].Raft.RequestVote(rpcCtx, req)
				if err != nil {
					log.Debugf("Failed to send RequestVote to %q(%q): %v", server, sm.cluster.Servers[server], err)
					return // Don't send nil response to message loop
				}
				select {
				case sm.msgCh <- resp:
				case <-sm.stopCh:
				}
			}()
		}
	}
	return nil
}

// sendAppendEntries constructs a AppendEntries request and sends it to all nodes in the cluster.
func (sm *StateMachine) sendAppendEntries(ctx context.Context) error {
	if sm.getRole() != konsen.Role_LEADER {
		return nil
	}

	// Leader resets its own election timer when sending heartbeats to prevent
	// spurious elections. Without this, the leader's election timer would fire
	// every 1-2 seconds causing constant re-elections.
	sm.resetElectionTimer()

	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return fmt.Errorf("failed to get current term: %v", err)
	}

	for server := range sm.cluster.Servers {
		if server != sm.cluster.LocalServerName {
			// If the follower needs entries that have been compacted,
			// send InstallSnapshot instead of AppendEntries (Raft paper Section 7).
			// While a transfer is in flight we skip AppendEntries for this follower; the
			// InstallSnapshot chunks themselves act as leader contact and reset the
			// follower's election timer.
			if sm.nextIndex[server] <= sm.snapshotIndex {
				if !sm.snapshotInFlight[server] {
					sm.snapshotInFlight[server] = true
					sm.sendInstallSnapshot(ctx, server, currentTerm)
				}
				continue
			}

			prevLogIndex := sm.nextIndex[server] - 1
			prevLogTerm, err := sm.effectiveLogTerm(prevLogIndex)
			if err != nil {
				return fmt.Errorf("failed to get log term at index %d: %v", prevLogIndex, err)
			}
			entries, err := sm.storage.GetLogsFrom(sm.nextIndex[server])
			if err != nil {
				return fmt.Errorf("failed to get logs from index %d: %v", sm.nextIndex[server], err)
			}
			req := &konsen.AppendEntriesReq{
				Term:         currentTerm,
				LeaderId:     sm.cluster.LocalServerName,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: sm.commitIndex,
			}

			sm.wg.Add(1)
			server := server
			go func() {
				defer sm.wg.Done()
				rpcCtx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
				defer cancel()
				resp, err := sm.clients[server].Raft.AppendEntries(rpcCtx, req)
				if err != nil {
					log.Debugf("Failed to send AppendEntries to %q(%q): %v", server, sm.cluster.Servers[server], err)
					return
				}
				select {
				case sm.msgCh <- appendEntriesRespWrap{
					resp:   resp,
					req:    req,
					server: server,
				}:
				case <-sm.stopCh:
				}
			}()
		}
	}

	return nil
}

// handleElectionTimeout handles when the election timeout event triggers.
func (sm *StateMachine) handleElectionTimeout() error {
	// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate.
	sm.stopHeartbeatIfRunning()
	sm.setRole(konsen.Role_CANDIDATE)
	sm.numVotes = 0
	sm.currentLeader = ""

	// On conversion to candidate, start election:
	// 1. Increment currentTerm and 2. Vote for self (atomically persisted).
	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return fmt.Errorf("failed to get current term: %v", err)
	}
	log.Debugf("Term - %d: election timeout", currentTerm)
	currentTerm++
	if err := sm.storage.SetTermAndVotedFor(currentTerm, sm.cluster.LocalServerName); err != nil {
		return fmt.Errorf("failed to set term to %d and vote for self: %v", currentTerm, err)
	}
	sm.numVotes++

	// 4. Send RequestVote RPCs to all other servers.
	log.Debugf("Send RequestVote for term %d.", currentTerm)
	if err := sm.sendVoteRequests(context.Background()); err != nil {
		return fmt.Errorf("failed to send vote requests: %v", err)
	}

	// 5. If votes received from majority of servers: become leader.
	// Handled by handleRequestVoteResp.

	// 6. If AppendEntries RPC received from new leader: convert to follower.
	// Handled by handleAppendEntries.

	// 7. If election timeout elapses: start new election.
	// Handled by election loop.

	return nil
}

// maxApplyBatchSize caps the number of log entries applied per message loop iteration.
// This prevents a large backlog (e.g., follower catch-up) from starving heartbeat processing.
const maxApplyBatchSize = 100

// maybeApplyLogs applies logs that are not yet.
// This method is called from the message loop, so we can directly access condMap.
func (sm *StateMachine) maybeApplyLogs(applyCommand func(command []byte) error) error {
	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
	applied := 0
	for sm.commitIndex > sm.lastApplied && applied < maxApplyBatchSize {
		nextIndex := sm.lastApplied + 1
		logEntry, err := sm.storage.GetLog(nextIndex)
		if err != nil {
			return fmt.Errorf("failed to get log at index %d: %v", nextIndex, err)
		}
		if logEntry == nil {
			return fmt.Errorf("log entry at index %d is nil (may have been compacted by snapshot)", nextIndex)
		}
		if err := applyCommand(logEntry.GetData()); err != nil {
			return fmt.Errorf("failed to apply command from log at index %d: %v", nextIndex, err)
		}
		// Advance only after successful application to avoid skipping entries on error.
		sm.lastApplied = nextIndex
		// Notifies if there is a goroutine waiting on log[lastApplied] being applied.
		if condCh, ok := sm.condMap[sm.lastApplied]; ok {
			close(condCh)
			// Note: deletion is handled by unregisterCondMsg from the waiting goroutine
		}
		log.Debugf("Applied log at index %d", sm.lastApplied)
		applied++
	}
	return nil
}

// maybeTakeSnapshot checks if enough entries have been applied since the last snapshot
// to warrant creating a new snapshot and compacting the log.
func (sm *StateMachine) maybeTakeSnapshot() error {
	if sm.lastApplied <= sm.snapshotIndex {
		return nil
	}
	if sm.lastApplied-sm.snapshotIndex < sm.snapshotThreshold {
		return nil
	}

	snapshotTerm, err := sm.effectiveLogTerm(sm.lastApplied)
	if err != nil {
		return fmt.Errorf("failed to get log term at index %d for snapshot: %v", sm.lastApplied, err)
	}

	// Serialize the KV state machine into a snapshot file.
	kvData, err := sm.storage.SnapshotKVData()
	if err != nil {
		return fmt.Errorf("failed to serialize KV data for snapshot: %v", err)
	}
	if err := sm.storage.SaveSnapshotFile(kvData); err != nil {
		return fmt.Errorf("failed to save snapshot file: %v", err)
	}

	// Persist snapshot metadata.
	if err := sm.storage.SetSnapshotMeta(sm.lastApplied, snapshotTerm); err != nil {
		return fmt.Errorf("failed to persist snapshot metadata: %v", err)
	}

	// Delete compacted log entries.
	if err := sm.storage.DeleteLogsUpTo(sm.lastApplied); err != nil {
		return fmt.Errorf("failed to delete compacted logs: %v", err)
	}

	sm.snapshotIndex = sm.lastApplied
	sm.snapshotTerm = snapshotTerm

	log.Infof("Snapshot taken at index %d, term %d", sm.snapshotIndex, sm.snapshotTerm)
	return nil
}

// sendInstallSnapshot sends a snapshot to a follower in chunks (Raft paper Figure 13).
// Called from the message loop when nextIndex[server] <= snapshotIndex.
// The caller must set snapshotInFlight[server] = true before calling. On a load
// failure the flag is cleared here (we are on the message loop); otherwise the spawned
// goroutine always sends an installSnapshotRespWrap back (with resp==nil on transfer
// failure) so the flag is cleared in handleInstallSnapshotResp.
func (sm *StateMachine) sendInstallSnapshot(ctx context.Context, server string, term uint64) {
	// Capture the snapshot metadata AND load its bytes here on the message loop so the
	// advertised (lastIncludedIndex, lastIncludedTerm) always matches the data being
	// sent. maybeTakeSnapshot also runs on this loop, so loading off-loop could pair an
	// old index/term with a newer snapshot file (or vice versa). Only the network
	// transfer is deferred to the goroutine.
	snapshotIndex := sm.snapshotIndex
	snapshotTerm := sm.snapshotTerm
	chunkSize := sm.snapshotChunkSize

	snapshotData, err := sm.storage.LoadSnapshotFile()
	if err != nil || len(snapshotData) == 0 {
		log.Errorf("Failed to load snapshot file for %q: %v", server, err)
		// Clear the flag so the next heartbeat cycle can retry.
		delete(sm.snapshotInFlight, server)
		return
	}

	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()

		// notify sends the result (success or failure) back through the message loop
		// so that snapshotInFlight is always cleared on the main goroutine.
		notify := func(resp *konsen.InstallSnapshotResp, req *konsen.InstallSnapshotReq) {
			select {
			case sm.msgCh <- installSnapshotRespWrap{
				resp:   resp,
				req:    req,
				server: server,
			}:
			case <-sm.stopCh:
			}
		}

		for offset := 0; offset < len(snapshotData); {
			end := offset + chunkSize
			if end > len(snapshotData) {
				end = len(snapshotData)
			}
			done := end == len(snapshotData)

			req := &konsen.InstallSnapshotReq{
				Term:              term,
				LeaderId:          sm.cluster.LocalServerName,
				LastIncludedIndex: snapshotIndex,
				LastIncludedTerm:  snapshotTerm,
				Offset:            uint64(offset),
				Data:              snapshotData[offset:end],
				Done:              done,
			}

			rpcCtx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
			resp, err := sm.clients[server].Raft.InstallSnapshot(rpcCtx, req)
			cancel()
			if err != nil {
				log.Debugf("Failed to send InstallSnapshot chunk to %q (offset=%d): %v", server, offset, err)
				notify(nil, req)
				return
			}

			// If the follower reports a higher term, the leader is stale -- stop sending
			// the remaining chunks and route the response back so the leader steps down.
			if resp.GetTerm() > term {
				notify(resp, req)
				return
			}

			// If not the final chunk, just advance offset and send the next chunk.
			if !done {
				offset = end
				continue
			}

			// Final chunk: send response through the message loop to update nextIndex/matchIndex.
			notify(resp, req)
			return
		}
	}()
}

// InstallSnapshot puts the incoming InstallSnapshot request in the message channel and waits for result.
func (sm *StateMachine) InstallSnapshot(ctx context.Context, req *konsen.InstallSnapshotReq) (*konsen.InstallSnapshotResp, error) {
	ch := make(chan *konsen.InstallSnapshotResp, 1)
	select {
	case sm.msgCh <- installSnapshotWrap{req: req, ch: ch}:
	case <-sm.stopCh:
		return nil, fmt.Errorf("server has been shut down")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp, nil
	}
}

// handleInstallSnapshot processes an InstallSnapshot RPC from the leader (Figure 13 steps 1-8).
func (sm *StateMachine) handleInstallSnapshot(req *konsen.InstallSnapshotReq) (*konsen.InstallSnapshotResp, error) {
	currentTerm, err := sm.maybeBecomeFollower(req.GetTerm())
	if err != nil {
		return nil, err
	}

	// 1. Reply immediately if term < currentTerm.
	if req.GetTerm() < currentTerm {
		return &konsen.InstallSnapshotResp{Term: currentTerm}, nil
	}

	// Valid leader -- reset election timer, record leader.
	// Per Raft paper Section 5.2: if a candidate receives a valid RPC from a leader with a term
	// at least as large as its own, it recognizes the leader as legitimate and steps down.
	if sm.getRole() == konsen.Role_CANDIDATE {
		sm.setRole(konsen.Role_FOLLOWER)
		sm.numVotes = 0
	}
	sm.resetElectionTimer()
	sm.currentLeader = req.GetLeaderId()

	// 2. Create new snapshot file if first chunk (offset is 0).
	if req.GetOffset() == 0 {
		sm.pendingSnapshot = &pendingSnapshotState{
			lastIncludedIndex: req.GetLastIncludedIndex(),
			lastIncludedTerm:  req.GetLastIncludedTerm(),
			data:              make([]byte, 0),
		}
	}

	if sm.pendingSnapshot == nil {
		// Received a non-first chunk without a pending snapshot -- stale or out-of-order.
		return &konsen.InstallSnapshotResp{Term: currentTerm}, nil
	}

	// 3. Write data into snapshot file at given offset.
	offset := int(req.GetOffset())
	if offset != len(sm.pendingSnapshot.data) {
		// Offset mismatch -- discard pending snapshot and let leader retry. With in-order
		// sequential unary chunks this is unreachable; if it ever fires (a dropped or
		// reordered chunk), the leader's next AppendEntries probe reconciles via a
		// prevLogTerm mismatch, so no stale snapshot is silently accepted.
		sm.pendingSnapshot = nil
		return &konsen.InstallSnapshotResp{Term: currentTerm}, nil
	}
	sm.pendingSnapshot.data = append(sm.pendingSnapshot.data, req.GetData()...)

	// 4. Reply and wait for more data chunks if done is false.
	if !req.GetDone() {
		return &konsen.InstallSnapshotResp{Term: currentTerm}, nil
	}

	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index.
	pending := sm.pendingSnapshot
	sm.pendingSnapshot = nil

	if pending.lastIncludedIndex <= sm.snapshotIndex {
		// Stale snapshot -- ignore.
		return &konsen.InstallSnapshotResp{Term: currentTerm}, nil
	}

	// 6. Decide whether the existing log already contains an entry matching the
	//    snapshot's last included entry. If so we retain the entries following it;
	//    otherwise the whole log is discarded. This is a read-only decision -- the
	//    actual deletion is deferred until after the snapshot is durably committed
	//    (below) so a crash mid-install can never strand metadata ahead of the log.
	retainSuffix := false
	lastLogIndex, err := sm.storage.LastLogIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to get last log index: %v", err)
	}
	if lastLogIndex >= pending.lastIncludedIndex {
		logTerm, err := sm.storage.GetLogTerm(pending.lastIncludedIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to get log term: %v", err)
		}
		retainSuffix = logTerm == pending.lastIncludedTerm
	}

	// 7 & 8. Reset the state machine from the snapshot, then commit, then compact.
	//
	// Ordering is crash-safety critical. SetSnapshotMeta is the commit point, so it
	// must come AFTER the KV state is durably restored: snapshotIndex claims that
	// everything up to it is applied and durable, so persisting it while the KV state
	// is still stale would leave the node permanently believing it is caught up with
	// data it never applied (the compacted logs are gone and cannot be replayed).
	// Deleting the log happens LAST: a crash before that only leaves harmless stale
	// entries <= snapshotIndex, which are never re-applied (lastApplied ==
	// snapshotIndex) and are reconciled by the leader's subsequent AppendEntries.
	if err := sm.storage.RestoreKVData(pending.data); err != nil {
		return nil, fmt.Errorf("failed to restore KV data from snapshot: %v", err)
	}
	if err := sm.storage.SaveSnapshotFile(pending.data); err != nil {
		return nil, fmt.Errorf("failed to save snapshot file: %v", err)
	}
	if err := sm.storage.SetSnapshotMeta(pending.lastIncludedIndex, pending.lastIncludedTerm); err != nil {
		return nil, fmt.Errorf("failed to set snapshot metadata: %v", err)
	}

	// Metadata is durable and matches the restored KV state; advance volatile state.
	// RestoreKVData replaced the entire KV state with the snapshot's state, so
	// lastApplied must match snapshotIndex regardless of its previous value.
	sm.snapshotIndex = pending.lastIncludedIndex
	sm.snapshotTerm = pending.lastIncludedTerm
	sm.lastApplied = sm.snapshotIndex
	if sm.commitIndex < sm.snapshotIndex {
		sm.commitIndex = sm.snapshotIndex
	}

	// Compact the log last (see ordering note above).
	if retainSuffix {
		// Matching entry -- keep entries after lastIncludedIndex, discard the rest.
		if err := sm.storage.DeleteLogsUpTo(pending.lastIncludedIndex); err != nil {
			return nil, fmt.Errorf("failed to delete logs up to %d: %v", pending.lastIncludedIndex, err)
		}
	} else {
		// No matching entry -- discard the entire log.
		if err := sm.storage.DeleteLogsFrom(1); err != nil {
			return nil, fmt.Errorf("failed to delete all logs: %v", err)
		}
	}

	log.Infof("Installed snapshot at index %d, term %d from leader %q",
		sm.snapshotIndex, sm.snapshotTerm, req.GetLeaderId())
	return &konsen.InstallSnapshotResp{Term: currentTerm}, nil
}

// handleInstallSnapshotResp handles a response to an InstallSnapshot we sent.
// resp may be nil if the transfer failed — in that case we only clear the in-flight flag
// so the next heartbeat cycle can retry.
func (sm *StateMachine) handleInstallSnapshotResp(
	resp *konsen.InstallSnapshotResp,
	req *konsen.InstallSnapshotReq,
	server string) error {

	// Always clear the in-flight flag so the transfer can be retried.
	delete(sm.snapshotInFlight, server)

	if resp == nil {
		return nil
	}

	if _, err := sm.maybeBecomeFollower(resp.GetTerm()); err != nil {
		return err
	}

	if sm.getRole() != konsen.Role_LEADER {
		return nil
	}

	// Update nextIndex and matchIndex for this follower.
	sm.nextIndex[server] = req.GetLastIncludedIndex() + 1
	sm.matchIndex[server] = req.GetLastIncludedIndex()

	return nil
}

// startMessageLoop starts the main message loop, the goroutine that runs message loop in turns picks an event from the
// message channel and processes it.
func (sm *StateMachine) startMessageLoop(ctx context.Context) {
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()

		applyFn := func(data []byte) error {
			kvs := &konsen.KVList{}
			if err := proto.Unmarshal(data, kvs); err != nil {
				return err
			}
			for _, kv := range kvs.GetKvList() {
				if err := sm.storage.SetValue(kv.GetKey(), kv.GetValue()); err != nil {
					return err
				}
			}
			return nil
		}

		for {
			// If commitIndex > lastApplied: apply up to maxApplyBatchSize entries.
			if err := sm.maybeApplyLogs(applyFn); err != nil {
				log.Fatalf("maybeApplyLogs: %v", err)
			}
			if err := sm.maybeTakeSnapshot(); err != nil {
				log.Errorf("maybeTakeSnapshot: %v", err)
			}

			// If there are still entries pending, yield to messages non-blockingly
			// so heartbeats and elections can be processed between batches.
			var msg interface{}
			var open bool
			if sm.commitIndex > sm.lastApplied {
				select {
				case <-ctx.Done():
					return
				case <-sm.stopCh:
					return
				case msg, open = <-sm.msgCh:
					if !open {
						return
					}
				default:
					continue // No pending messages; loop back to apply more.
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case <-sm.stopCh:
					return
				case msg, open = <-sm.msgCh:
					if !open {
						return
					}
				}
			}

			switch v := msg.(type) {
			case appendEntriesWrap:
				// Process incoming AppendEntries request.
				resp, err := sm.handleAppendEntries(v.req)
				if err != nil {
					log.Errorf("handleAppendEntries failed: %v", err)
					v.ch <- &konsen.AppendEntriesResp{Success: false}
					continue
				}
				v.ch <- resp
			case requestVoteWrap:
				// Process incoming RequestVote request.
				resp, err := sm.handleRequestVote(v.req)
				if err != nil {
					log.Errorf("handleRequestVote failed: %v", err)
					v.ch <- &konsen.RequestVoteResp{VoteGranted: false}
					continue
				}
				v.ch <- resp
			case appendEntriesRespWrap:
				if err := sm.handleAppendEntriesResp(v.resp, v.req, v.server); err != nil {
					log.Fatalf("handleAppendEntriesResp failed: %v", err)
				}
			case *konsen.RequestVoteResp:
				if err := sm.handleRequestVoteResp(v); err != nil {
					log.Fatalf("handleRequestVoteResp failed: %v", err)
				}
			case electionTimeoutMsg:
				if err := sm.handleElectionTimeout(); err != nil {
					log.Fatalf("handleElectionTimeout failed: %v", err)
				}
			case appendEntriesMsg:
				if err := sm.sendAppendEntries(context.Background()); err != nil {
					log.Errorf("sendAppendEntries failed: %v", err)
				}
			case putMsg:
				if err := sm.handlePut(v.req, v.ch); err != nil {
					log.Errorf("handlePut failed: %v", err)
					v.ch <- &konsen.PutResp{
						Success:      false,
						ErrorMessage: "internal storage error",
					}
				}
			case getSnapshotMsg:
				snapshot, err := sm.handleGetSnapshot()
				if err != nil {
					log.Errorf("handleGetSnapshot failed: %v", err)
				}
				v.ch <- getSnapshotResp{snapshot: snapshot, err: err}
			case getValueMsg:
				sm.handleGetValue(v.key, v.ch)
			case listKeysMsg:
				keys, err := sm.storage.ListKeys(v.prefix, v.limit)
				v.ch <- listKeysResp{keys: keys, err: err}
			case registerCondMsg:
				sm.condMap[v.logIndex] = v.condCh
			case unregisterCondMsg:
				delete(sm.condMap, v.logIndex)
			case healthCheckMsg:
				v.ch <- healthCheckResp{
					role:          sm.getRole(),
					currentLeader: sm.currentLeader,
				}
			case getStatusMsg:
				status, err := sm.handleGetStatus()
				if err != nil {
					log.Errorf("handleGetStatus failed: %v", err)
				}
				v.ch <- getStatusResp{status: status, err: err}
			case installSnapshotWrap:
				resp, err := sm.handleInstallSnapshot(v.req)
				if err != nil {
					log.Errorf("handleInstallSnapshot failed: %v", err)
					currentTerm, termErr := sm.storage.GetCurrentTerm()
					if termErr != nil {
						log.Fatalf("failed to get current term: %v", termErr)
					}
					v.ch <- &konsen.InstallSnapshotResp{Term: currentTerm}
					continue
				}
				v.ch <- resp
			case installSnapshotRespWrap:
				if err := sm.handleInstallSnapshotResp(v.resp, v.req, v.server); err != nil {
					log.Fatalf("handleInstallSnapshotResp failed: %v", err)
				}
			default:
				log.Fatalf("Unrecognized message: %v", v)
			}
		}
	}()
}

// startElectionLoop starts the election timeout monitoring loop.
func (sm *StateMachine) startElectionLoop(ctx context.Context) {
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()

		for {
			timer := time.NewTimer(sm.nextTimeout())
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-sm.stopCh:
				timer.Stop()
				return
			case <-sm.resetTimerCh:
				timer.Stop()
				continue
			case <-timer.C:
				// Election timeout fired. Try to send, but allow cancellation
				// by a concurrent reset to avoid spurious elections.
				select {
				case sm.msgCh <- electionTimeoutMsg{}:
				case <-sm.resetTimerCh:
					continue
				case <-sm.stopCh:
					return
				case <-ctx.Done():
					return
				}
			}
		}
	}()
}

// startHeartbeatLoop starts the heartbeat loop.
func (sm *StateMachine) startHeartbeatLoop(ctx context.Context) {
	stopCh := sm.stopHeartbeatCh // Capture the stop channel to avoid race
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()

		// Heartbeat worker.
		ticker := time.NewTicker(sm.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sm.stopCh:
				return
			case <-stopCh:
				// No longer leader, stop heartbeat loop
				return
			case <-ticker.C:
				// Use atomic read to check role - safe for concurrent access
				if sm.getRole() != konsen.Role_LEADER {
					return
				}
				select {
				case sm.msgCh <- appendEntriesMsg{}:
				case <-sm.stopCh:
					return
				case <-stopCh:
					return
				}
			}
		}
	}()
}

// nextTimeout calculates the next election timeout duration.
func (sm *StateMachine) nextTimeout() time.Duration {
	timeout := rand.Int63n(int64(sm.timeoutSpan)) + int64(sm.minTimeout)
	return time.Duration(timeout)
}

// Put stores the given data into state machine, and it returns after the data is replicated onto quorum.
func (sm *StateMachine) Put(ctx context.Context, req *konsen.PutReq) (*konsen.PutResp, error) {
	ch := make(chan *konsen.PutResp, 1) // Buffered to prevent blocking message loop if caller times out
	select {
	case sm.msgCh <- putMsg{req: req, ch: ch}:
	case <-sm.stopCh:
		return nil, fmt.Errorf("server has been shut down")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-sm.stopCh:
		return nil, fmt.Errorf("server has been shut down")
	case resp := <-ch:
		return resp, nil
	}
}

func (sm *StateMachine) majority() int {
	return len(sm.cluster.Servers)/2 + 1
}

// handlePut processes a putMsg, it writes the data into local (or on leader's) logs, returns after the
// data is applied to local state machine.
// The message loop goroutine should NEVER block in this method since it depends on subsequent AppendEntriesResp in
// order to determine which log is committed, otherwise it will deadlock.
func (sm *StateMachine) handlePut(req *konsen.PutReq, ch chan<- *konsen.PutResp) error {
	// If no leader is elected, put the message back to message queue.
	if sm.currentLeader == "" {
		ch <- &konsen.PutResp{
			Success:      false,
			ErrorMessage: fmt.Sprintf("no leader is elected yet (are there more than %d nodes down?)", sm.majority()-1),
		}
		return nil
	}

	// Only leader writes data to its logs.
	if sm.getRole() != konsen.Role_LEADER {
		// Forward the request to leader.
		// Capture currentLeader before spawning goroutine to avoid data race.
		sm.forwardPutToLeader(sm.currentLeader, req, ch)
		return nil
	}

	// Writes data into a new log entry next to the last log.
	newLog, err := sm.writeToLogs(req.GetData())
	if err != nil {
		return err
	}

	// Starts a new goroutine to wait until the new log is committed (replicated on quorum) and applied to local state machine.
	sm.replyWhenLogApplied(newLog.GetIndex(), ch)
	return nil
}

func (sm *StateMachine) writeToLogs(data []byte) (*konsen.Log, error) {
	lastLogIndex, err := sm.effectiveLastLogIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to get last log index: %v", err)
	}
	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return nil, fmt.Errorf("failed to get current term: %v", err)
	}
	newLog := &konsen.Log{
		Index: lastLogIndex + 1,
		Term:  currentTerm,
		Data:  data,
	}
	if err := sm.storage.WriteLog(newLog); err != nil {
		return nil, fmt.Errorf("failed to write log: %v", err)
	}
	log.Debugf("Log written: index - %d, term - %d, bytes - %d.", newLog.GetIndex(), newLog.GetTerm(), len(newLog.GetData()))
	return newLog, nil
}

// forwardPutToLeader starts a new goroutine to send a Put request to the current leader, and the goroutine replies after receiving result from leader.
// The leader parameter is captured before spawning the goroutine to avoid data race on sm.currentLeader.
func (sm *StateMachine) forwardPutToLeader(leader string, req *konsen.PutReq, ch chan<- *konsen.PutResp) {
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
		defer cancel()
		resp, err := sm.clients[leader].KV.Put(ctx, req)
		if err != nil {
			log.Debugf("Failed to send PutReq to leader %q: %v", leader, err)
			ch <- &konsen.PutResp{Success: false, ErrorMessage: err.Error()}
			return
		}
		ch <- resp
	}()
}

// replyWhenLogApplied starts a new goroutine to reply request after new log is applied to local state machine.
// This method is called from the message loop, so we can directly access condMap.
func (sm *StateMachine) replyWhenLogApplied(logIndex uint64, ch chan<- *konsen.PutResp) {
	condCh := make(chan struct{})
	// Register the condition channel directly - we're in the message loop
	sm.condMap[logIndex] = condCh

	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()

		// Use NewTimer instead of time.After to allow proper cleanup
		timer := time.NewTimer(defaultRequestTimeout)
		defer timer.Stop()

		select {
		case <-condCh:
			// Unregister via message to maintain actor pattern
			select {
			case sm.msgCh <- unregisterCondMsg{logIndex: logIndex}:
			case <-sm.stopCh:
			}
			log.Debugf("Log[%d] is committed and applied on local state machine.", logIndex)
			ch <- &konsen.PutResp{Success: true}
		case <-timer.C:
			// Unregister via message to maintain actor pattern
			select {
			case sm.msgCh <- unregisterCondMsg{logIndex: logIndex}:
			case <-sm.stopCh:
			}
			log.Debugf("Timeout while waiting for log[%d] to be committed and applied.", logIndex)
			ch <- &konsen.PutResp{
				Success:      false,
				ErrorMessage: fmt.Sprintf("failed to replicate onto quorum and apply commands (are there more than %d nodes down?)", sm.majority()-1),
			}
		case <-sm.stopCh:
			// Unregister on shutdown as well
			select {
			case sm.msgCh <- unregisterCondMsg{logIndex: logIndex}:
			default: // Non-blocking - msgCh might be closed or full during shutdown
			}
			ch <- &konsen.PutResp{
				Success:      false,
				ErrorMessage: "server has been shut down",
			}
		}
	}()
}

// HealthCheck sends a lightweight ping through the message loop and returns the node's
// current role and leader. If the message loop is unresponsive, the context will time out.
func (sm *StateMachine) HealthCheck(ctx context.Context) (role konsen.Role, leader string, err error) {
	ch := make(chan healthCheckResp, 1)
	select {
	case sm.msgCh <- healthCheckMsg{ch: ch}:
	case <-sm.stopCh:
		return 0, "", fmt.Errorf("server has been shut down")
	case <-ctx.Done():
		return 0, "", ctx.Err()
	}
	select {
	case <-ctx.Done():
		return 0, "", ctx.Err()
	case resp := <-ch:
		return resp.role, resp.currentLeader, nil
	}
}

func (sm *StateMachine) GetSnapshot(ctx context.Context) (*Snapshot, error) {
	ch := make(chan getSnapshotResp, 1) // Buffered to prevent blocking message loop if caller times out
	select {
	case sm.msgCh <- getSnapshotMsg{ch: ch}:
	case <-sm.stopCh:
		return nil, fmt.Errorf("server has been shut down")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp.snapshot, resp.err
	}
}

func (sm *StateMachine) handleGetSnapshot() (*Snapshot, error) {
	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return nil, fmt.Errorf("failed to get current term: %v", err)
	}
	logs, err := sm.storage.GetLogsFrom(sm.snapshotIndex + 1)
	if err != nil {
		return nil, fmt.Errorf("failed to get logs: %v", err)
	}
	nextIndexMap := make(map[string]uint64)
	for k, v := range sm.nextIndex {
		nextIndexMap[k] = v
	}
	matchIndexMap := make(map[string]uint64)
	for k, v := range sm.matchIndex {
		matchIndexMap[k] = v
	}
	logIndices := make([]uint64, len(logs))
	logTerms := make([]uint64, len(logs))
	logBytes := make([]int, len(logs))
	for i, e := range logs {
		logIndices[i] = e.GetIndex()
		logTerms[i] = e.GetTerm()
		logBytes[i] = len(e.GetData())
	}
	return &Snapshot{
		CurrentTerm:   currentTerm,
		CommitIndex:   sm.commitIndex,
		LastApplied:   sm.lastApplied,
		Role:          sm.getRole(),
		CurrentLeader: sm.currentLeader,
		NextIndex:     nextIndexMap,
		MatchIndex:    matchIndexMap,
		LogIndices:    logIndices,
		LogTerms:      logTerms,
		LogBytes:      logBytes,
		SnapshotIndex: sm.snapshotIndex,
		SnapshotTerm:  sm.snapshotTerm,
	}, nil
}

func (sm *StateMachine) handleGetStatus() (*StatusSnapshot, error) {
	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return nil, fmt.Errorf("failed to get current term: %v", err)
	}
	lastLogIndex, err := sm.effectiveLastLogIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to get last log index: %v", err)
	}
	nextIndexMap := make(map[string]uint64)
	for k, v := range sm.nextIndex {
		nextIndexMap[k] = v
	}
	matchIndexMap := make(map[string]uint64)
	for k, v := range sm.matchIndex {
		matchIndexMap[k] = v
	}
	return &StatusSnapshot{
		CurrentTerm:   currentTerm,
		CommitIndex:   sm.commitIndex,
		LastApplied:   sm.lastApplied,
		Role:          sm.getRole(),
		CurrentLeader: sm.currentLeader,
		NextIndex:     nextIndexMap,
		MatchIndex:    matchIndexMap,
		LogCount:      lastLogIndex,
		SnapshotIndex: sm.snapshotIndex,
		SnapshotTerm:  sm.snapshotTerm,
	}, nil
}

// GetStatus returns a lightweight status snapshot without loading all logs.
func (sm *StateMachine) GetStatus(ctx context.Context) (*StatusSnapshot, error) {
	ch := make(chan getStatusResp, 1)
	select {
	case sm.msgCh <- getStatusMsg{ch: ch}:
	case <-sm.stopCh:
		return nil, fmt.Errorf("server has been shut down")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp.status, resp.err
	}
}

func (sm *StateMachine) SetKeyValue(ctx context.Context, kv *konsen.KVList) error {
	buf, err := proto.Marshal(kv)
	if err != nil {
		return fmt.Errorf("failed to marshal: %v", err)
	}
	resp, err := sm.Put(ctx, &konsen.PutReq{Data: buf})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.ErrorMessage)
	}
	return nil
}

func (sm *StateMachine) GetValue(ctx context.Context, key []byte) ([]byte, error) {
	ch := make(chan getValueResp, 1) // Buffered to prevent blocking message loop if caller times out
	select {
	case sm.msgCh <- getValueMsg{key: key, ch: ch}:
	case <-sm.stopCh:
		return nil, fmt.Errorf("server has been shut down")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp.value, resp.err
	}
}

// ListKeys returns stored KV keys matching the given prefix. This is a local read
// (not forwarded to the leader) since it's intended for browsing/debugging.
func (sm *StateMachine) ListKeys(ctx context.Context, prefix []byte, limit int) ([][]byte, error) {
	ch := make(chan listKeysResp, 1)
	select {
	case sm.msgCh <- listKeysMsg{prefix: prefix, limit: limit, ch: ch}:
	case <-sm.stopCh:
		return nil, fmt.Errorf("server has been shut down")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp.keys, resp.err
	}
}

func (sm *StateMachine) handleGetValue(key []byte, ch chan<- getValueResp) {
	if sm.getRole() != konsen.Role_LEADER {
		if sm.currentLeader == "" {
			ch <- getValueResp{err: fmt.Errorf("no leader is elected yet")}
			return
		}
		sm.forwardGetToLeader(sm.currentLeader, key, ch)
		return
	}
	val, err := sm.storage.GetValue(key)
	ch <- getValueResp{value: val, err: err}
}

// forwardGetToLeader starts a new goroutine to send a Get request to the current leader.
// The leader parameter is captured before spawning the goroutine to avoid data race on sm.currentLeader.
func (sm *StateMachine) forwardGetToLeader(leader string, key []byte, ch chan<- getValueResp) {
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
		defer cancel()
		resp, err := sm.clients[leader].KV.Get(ctx, &konsen.GetReq{Key: key})
		if err != nil {
			ch <- getValueResp{err: err}
			return
		}
		if !resp.GetSuccess() {
			ch <- getValueResp{err: fmt.Errorf("%s", resp.GetErrorMessage())}
			return
		}
		ch <- getValueResp{value: resp.GetValue()}
	}()
}

func (sm *StateMachine) Close() error {
	// Closing stopCh signals all goroutines (message loop, election loop, heartbeat loop) to exit.
	// No need to call stopHeartbeatIfRunning() here — avoids data race with message loop.
	close(sm.stopCh)
	sm.wg.Wait()
	return nil
}
