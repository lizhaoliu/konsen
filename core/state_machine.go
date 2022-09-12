package core

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/lizhaoliu/konsen/v2/datastore"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
	log "github.com/sirupsen/logrus"
)

const (
	defaultMinTimeout  = 1000 * time.Millisecond
	defaultTimeoutSpan = 1000 * time.Millisecond
	defaultHeartbeat   = 100 * time.Millisecond

	defaultRequestTimeout = 5 * time.Second
)

// StateMachine is the state machine that implements Raft algorithm: https://raft.github.io/raft.pdf.
// The state machine maintains a message queue (mailbox) internally, all requests/responses to the state machine are
// processed asynchronously (although the caller still observes a synchronized behavior): they are firstly put onto the
// message queue, then the message worker (goroutine) in turns takes a message at a time and processes it, and passes
// the result back to caller. The internal state is never directly accessed by goroutines other than the message worker.
// Internal errors will always cause a crash on the server since otherwise the state machine may be left in an
// inconsistent state.
type StateMachine struct {
	msgCh        chan interface{} // Main message queue.
	stopCh       chan struct{}    // Signals to stop the state machine.
	timerGateCh  chan struct{}    // Signals to run next round of election timeout countdown.
	resetTimerCh chan struct{}    // Signals to reset election timer when AppendEntries or RequestVote requests/response are received.

	// Persistent state storage on all servers.
	storage datastore.Storage

	// Volatile state on all servers.
	commitIndex   uint64      // Index of highest log entry known to be committed (initialized to 0).
	lastApplied   uint64      // Index of highest log entry applied to state machine (initialized to 0).
	role          konsen.Role // Current role.
	currentLeader string      // Current leader.

	// Volatile state on candidates.
	numVotes int

	// Volatile state on leaders (must be reinitialized after election).
	nextIndex  map[string]uint64 // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1).
	matchIndex map[string]uint64 // For each server, index of highest log entry known to be replicated on that server (initialized to 0, increases monotonically).

	// ClusterConfig info.
	cluster *ClusterConfig
	clients map[string]RaftService

	wg   sync.WaitGroup
	once sync.Once

	condMap sync.Map
}

// StateMachineConfig
type StateMachineConfig struct {
	Storage datastore.Storage      // Local storage instance.
	Cluster *ClusterConfig         // Cluster configuration.
	Clients map[string]RaftService // A map of "server name": "Raft service".
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

// appendDataMsg represents a message to append given data into state machine.
type appendDataMsg struct {
	req *konsen.AppendDataReq
	ch  chan<- *konsen.AppendDataResp
}

// getSnapshotMsg represents a message to generate a state snapshot.
type getSnapshotMsg struct {
	ch chan<- *Snapshot
}

// getValueMsg represents a message to retrieve a value by given key.
type getValueMsg struct {
	key []byte
	ch  chan<- []byte
}

// NewStateMachine creates a new instance of the state machine.
func NewStateMachine(config StateMachineConfig) (*StateMachine, error) {
	if len(config.Cluster.Servers)%2 != 1 {
		return nil, fmt.Errorf("number of nodes in the cluster must be an odd number, got: %d", len(config.Cluster.Servers))
	}

	sm := &StateMachine{
		msgCh:        make(chan interface{}),
		stopCh:       make(chan struct{}),
		timerGateCh:  make(chan struct{}, 1),
		resetTimerCh: make(chan struct{}),

		storage: config.Storage,
		cluster: config.Cluster,
		clients: config.Clients,

		commitIndex: 0,
		lastApplied: 0,
		role:        konsen.Role_FOLLOWER,

		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
	}

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

// AppendEntries puts the incoming AppendEntries request in main message channel and waits for result.
func (sm *StateMachine) AppendEntries(ctx context.Context, req *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
	ch := make(chan *konsen.AppendEntriesResp)
	sm.msgCh <- appendEntriesWrap{req: req, ch: ch}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp, nil
	}
}

// RequestVote puts the incoming RequestVote request in main message channel and waits for result.
func (sm *StateMachine) RequestVote(ctx context.Context, req *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
	ch := make(chan *konsen.RequestVoteResp)
	sm.msgCh <- requestVoteWrap{req: req, ch: ch}
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
	log.Debug("Granted vote for candidate %q for term %d", candidateID, term)
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
		if err := sm.storage.SetCurrentTerm(term); err != nil {
			return 0, fmt.Errorf("failed to set current term to %d: %v", term, err)
		}
		currentTerm = term
		sm.role = konsen.Role_FOLLOWER
		if err := sm.storage.SetVotedFor(""); err != nil {
			return currentTerm, fmt.Errorf("failed to reset voted for: %v", err)
		}
	}
	return currentTerm, nil
}

// resetElectionTimer signals the election timer to start a new round of election timeout countdown.
func (sm *StateMachine) resetElectionTimer() {
	sm.resetTimerCh <- struct{}{}
}

// openElectionTimerGate signals the election timer that it can start countdown.
func (sm *StateMachine) openElectionTimerGate() {
	sm.timerGateCh <- struct{}{}
}

// handleAppendEntries handles a AppendEntries request.
func (sm *StateMachine) handleAppendEntries(req *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
	sm.resetElectionTimer()
	defer sm.openElectionTimerGate()

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
	sm.currentLeader = req.GetLeaderId()

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
	prevLogTerm, err := sm.storage.GetLogTerm(req.GetPrevLogIndex())
	if err != nil {
		return nil, fmt.Errorf("failed to get log at index %d: %v", req.GetPrevLogIndex(), err)
	}
	if prevLogTerm != req.GetPrevLogTerm() {
		log.Debug("Local prevLogTerm(%d) mismatches request prevLogTerm(%d).", prevLogTerm, req.GetPrevLogTerm())
		return &konsen.AppendEntriesResp{Term: currentTerm, Success: false}, nil
	}

	entries := req.GetEntries()
	// If there are new logs to append.
	if len(entries) > 0 {
		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
		startIdx := 0
		for i, newLog := range entries {
			localLog, err := sm.storage.GetLog(newLog.GetIndex())
			if err != nil {
				return nil, fmt.Errorf("failed to get log at index %d: %v", newLog.GetIndex(), err)
			}
			if localLog == nil {
				startIdx = i
				break
			}
			if localLog.GetTerm() != newLog.GetTerm() {
				log.Debug("Local logs conflict from index %d, now delete onwards.", newLog.GetIndex())
				if err := sm.storage.DeleteLogsFrom(newLog.GetIndex()); err != nil {
					return nil, fmt.Errorf("failed to delete logs from min index %d: %v", newLog.GetIndex(), err)
				}
				startIdx = i
				break
			}
		}

		// 4. Append any new entries not already in the log.
		log.Debug("Append logs from index %d.", entries[startIdx].GetIndex())
		if err := sm.storage.WriteLogs(entries[startIdx:]); err != nil {
			return nil, fmt.Errorf("failed to write logs: %v", err)
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
	if req.GetLeaderCommit() > sm.commitIndex {
		lastLogIndex, err := sm.storage.LastLogIndex()
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
	sm.resetElectionTimer()
	defer sm.openElectionTimerGate()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	currentTerm, err := sm.maybeBecomeFollower(resp.GetTerm())
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	// Terminate if no longer a leader.
	if sm.role != konsen.Role_LEADER {
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
			logIndex := req.GetEntries()[i].GetIndex()
			logTerm, err := sm.storage.GetLogTerm(logIndex)
			if err != nil {
				return fmt.Errorf("failed to get log term at index %d: %v", logIndex, err)
			}
			if logIndex > sm.commitIndex && sm.isLogOnMajority(logIndex) && logTerm == currentTerm {
				sm.commitIndex = logIndex
				break
			}
		}
	} else {
		// AppendEntries fails because of log inconsistency: decrement nextIndex and retry.
		sm.nextIndex[server]--
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
	return n > sm.getQuorum()
}

// handleRequestVote handles a RequestVote request.
func (sm *StateMachine) handleRequestVote(req *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
	sm.resetElectionTimer()
	defer sm.openElectionTimerGate()

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
	lastLogTerm, err := sm.storage.LastLogTerm()
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
	lastLogIndex, err := sm.storage.LastLogIndex()
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
	sm.resetElectionTimer()
	defer sm.openElectionTimerGate()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	currentTerm, err := sm.maybeBecomeFollower(resp.GetTerm())
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	if sm.role != konsen.Role_CANDIDATE {
		return nil
	}

	if resp.GetVoteGranted() {
		sm.numVotes++
		// If votes received from majority of servers, become leader.
		if sm.numVotes > sm.getQuorum() {
			if err := sm.becomeLeader(currentTerm); err != nil {
				return fmt.Errorf("failed to become leader: %v", err)
			}
		}
	}

	return nil
}

// becomeLeader modifies internal state to become a leader, and starts the worker that periodically sends heartbeat.
func (sm *StateMachine) becomeLeader(term uint64) error {
	log.Infof("Term - %d, leader - %q.", term, sm.cluster.LocalServerName)
	sm.role = konsen.Role_LEADER
	sm.currentLeader = sm.cluster.LocalServerName
	lastLogIndex, err := sm.storage.LastLogIndex()
	if err != nil {
		return err
	}

	// Resets nextIndex and matchIndex after election.
	for server := range sm.cluster.Servers {
		if server != sm.cluster.LocalServerName {
			sm.nextIndex[server] = lastLogIndex + 1
			sm.matchIndex[server] = 0
		}
	}

	sm.startHeartbeatLoop(context.Background())

	return nil
}

// sendVoteRequests constructs a RequestVote request and sends it to all nodes in the cluster.
func (sm *StateMachine) sendVoteRequests(ctx context.Context) error {
	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return fmt.Errorf("failed to get current term: %v", err)
	}
	lastLogIndex, err := sm.storage.LastLogIndex()
	if err != nil {
		return fmt.Errorf("failed to get last log index: %v", err)
	}
	lastLogTerm, err := sm.storage.LastLogTerm()
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
				resp, err := sm.clients[server].RequestVote(ctx, req)
				if err != nil {
					log.Debug("Failed to send RequestVote to %q(%q): %v", server, sm.cluster.Servers[server], err)
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
	if sm.role != konsen.Role_LEADER {
		return nil
	}

	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return fmt.Errorf("failed to get current term: %v", err)
	}

	for server := range sm.cluster.Servers {
		if server != sm.cluster.LocalServerName {
			prevLogIndex := sm.nextIndex[server] - 1
			prevLogTerm, err := sm.storage.GetLogTerm(prevLogIndex)
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
				resp, err := sm.clients[server].AppendEntries(ctx, req)
				if err != nil {
					log.Debug("Failed to send AppendEntries to %q(%q): %v", server, sm.cluster.Servers[server], err)
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
	sm.role = konsen.Role_CANDIDATE
	sm.numVotes = 0
	sm.currentLeader = ""

	// On conversion to candidate, start election:
	// 1. Increment currentTerm.
	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return fmt.Errorf("failed to get current term: %v", err)
	}
	log.Debug("Term - %d: election timeout", currentTerm)
	currentTerm++
	if err := sm.storage.SetCurrentTerm(currentTerm); err != nil {
		return fmt.Errorf("failed to set current term to %d: %v", currentTerm, err)
	}

	// 2. Vote for self.
	if err := sm.storage.SetVotedFor(sm.cluster.LocalServerName); err != nil {
		return fmt.Errorf("failed to set votedFor: %v", err)
	}
	sm.numVotes++

	// 3. Reset election timer.
	sm.timerGateCh <- struct{}{}

	// 4. Send RequestVote RPCs to all other servers.
	log.Debug("Send RequestVote for term %d.", currentTerm)
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

// maybeApplyLogs applies logs that are not yet.
func (sm *StateMachine) maybeApplyLogs(applyCommand func(command []byte) error) error {
	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
	for sm.commitIndex > sm.lastApplied {
		sm.lastApplied++
		logEntry, err := sm.storage.GetLog(sm.lastApplied)
		if err != nil {
			return fmt.Errorf("failed to get log at index %d", sm.lastApplied)
		}
		if err := applyCommand(logEntry.GetData()); err != nil {
			return fmt.Errorf("failed to apply command from log at index %d", sm.lastApplied)
		}
		// Notifies if there is a goroutine waiting on log[lastApplied] being applied.
		condCh, ok := sm.condMap.Load(sm.lastApplied)
		if ok {
			condCh := condCh.(chan struct{})
			close(condCh)
		}
		log.Debug("Applied log at index %d", sm.lastApplied)
	}
	return nil
}

// startMessageLoop starts the main message loop, the goroutine that runs message loop in turns picks an event from the
// message channel and processes it.
func (sm *StateMachine) startMessageLoop(ctx context.Context) {
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()

		for {
			// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
			if err := sm.maybeApplyLogs(func(data []byte) error {
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
			}); err != nil {
				log.Fatalf("%v", err)
			}

			select {
			case <-ctx.Done():
				return
			case <-sm.stopCh:
				return
			case msg, open := <-sm.msgCh:
				if !open {
					return
				}

				switch v := msg.(type) {
				case appendEntriesWrap:
					// Process incoming AppendEntries request.
					resp, err := sm.handleAppendEntries(v.req)
					if err != nil {
						log.Fatalf("%v", err)
					}
					v.ch <- resp
				case requestVoteWrap:
					// Process incoming RequestVote request.
					resp, err := sm.handleRequestVote(v.req)
					if err != nil {
						log.Fatalf("%v", err)
					}
					v.ch <- resp
				case appendEntriesRespWrap:
					if err := sm.handleAppendEntriesResp(v.resp, v.req, v.server); err != nil {
						log.Fatalf("%v", err)
					}
				case *konsen.RequestVoteResp:
					if err := sm.handleRequestVoteResp(v); err != nil {
						log.Fatalf("%v", err)
					}
				case electionTimeoutMsg:
					if err := sm.handleElectionTimeout(); err != nil {
						log.Fatalf("%v", err)
					}
				case appendEntriesMsg:
					if err := sm.sendAppendEntries(context.Background()); err != nil {
						log.Fatalf("%v", err)
					}
				case appendDataMsg:
					if err := sm.handleAppendData(v.req, v.ch); err != nil {
						log.Fatalf("%v", err)
					}
				case getSnapshotMsg:
					snapshot, err := sm.handleGetSnapshot()
					if err != nil {
						log.Fatalf("%v", err)
					}
					v.ch <- snapshot
				case getValueMsg:
					val, err := sm.handleGetValue(v.key)
					if err != nil {
						log.Fatalf("%v", err)
					}
					v.ch <- val
				default:
					log.Fatalf("Unrecognized message: %v", v)
				}
			}
		}
	}()
}

// startElectionLoop starts the election timeout monitoring loop.
func (sm *StateMachine) startElectionLoop(ctx context.Context) {
	sm.timerGateCh <- struct{}{}
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()

		for {
			// Waits for gate signal to start a new round of election timeout countdown.
			<-sm.timerGateCh

			timer := time.NewTimer(sm.nextTimeout())
			select {
			case <-ctx.Done():
				return
			case <-sm.stopCh:
				return
			case <-sm.resetTimerCh:
				// Start the next round of timeout.
				timer.Stop()
				continue
			case <-timer.C:
				// Election timeout occurs.
				sm.msgCh <- electionTimeoutMsg{}
			}
		}
	}()
}

// startHeartbeatLoop starts the heartbeat loop.
func (sm *StateMachine) startHeartbeatLoop(ctx context.Context) {
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()

		// Heartbeat worker.
		ticker := time.NewTicker(defaultHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sm.stopCh:
				return
			case <-ticker.C:
				// TODO: role is read by another goroutine, there might be data race.
				if sm.role != konsen.Role_LEADER {
					return
				}
				sm.msgCh <- appendEntriesMsg{}
			}
		}
	}()
}

// nextTimeout calculates the next election timeout duration.
func (sm *StateMachine) nextTimeout() time.Duration {
	timeout := rand.Int63n(int64(defaultTimeoutSpan)) + int64(defaultMinTimeout)
	return time.Duration(timeout)
}

// AppendData stores the given data into state machine, and it returns after the data is replicated onto quorum.
func (sm *StateMachine) AppendData(ctx context.Context, req *konsen.AppendDataReq) (*konsen.AppendDataResp, error) {
	ch := make(chan *konsen.AppendDataResp)
	sm.msgCh <- appendDataMsg{req: req, ch: ch}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-sm.stopCh:
		return nil, fmt.Errorf("server has been shut down")
	case resp := <-ch:
		return resp, nil
	}
}

func (sm *StateMachine) getQuorum() int {
	return len(sm.cluster.Servers) / 2
}

// handleAppendData processes a appendDataMsg, it writes the data into local (or on leader's) logs, returns after the
// data is applied to local state machine.
// The message loop goroutine should NEVER block in this method since it depends on subsequent AppendEntriesResp in
// order to determine which log is committed, otherwise it will deadlock.
func (sm *StateMachine) handleAppendData(req *konsen.AppendDataReq, ch chan<- *konsen.AppendDataResp) error {
	// If no leader is elected, put the message back to message queue.
	if sm.currentLeader == "" {
		ch <- &konsen.AppendDataResp{
			Success:      false,
			ErrorMessage: fmt.Sprintf("no leader is elected yet (are there more than %d nodes down?)", sm.getQuorum()),
		}
		return nil
	}

	// Only leader writes data to its logs.
	if sm.role != konsen.Role_LEADER {
		// Forward the request to leader.
		sm.forwardRequestToLeader(req, ch)
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
	lastLogIndex, err := sm.storage.LastLogIndex()
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
	log.Debug("Log written: index - %d, term - %d, bytes - %d.", newLog.GetIndex(), newLog.GetTerm(), len(newLog.GetData()))
	return newLog, nil
}

// forwardRequestToLeader starts a new goroutine to send request to current leader, and the goroutine replies after receiving result from leader.
func (sm *StateMachine) forwardRequestToLeader(req *konsen.AppendDataReq, ch chan<- *konsen.AppendDataResp) {
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		ctx := context.Background()
		resp, err := sm.clients[sm.currentLeader].AppendData(ctx, req)
		if err != nil {
			log.Debug("Failed to send AppendDataReq to leader %q: %v", sm.currentLeader, err)
			ch <- &konsen.AppendDataResp{Success: false, ErrorMessage: err.Error()}
			return
		}
		ch <- resp
	}()
}

// replyWhenLogApplied starts a new goroutine to reply request after new log is applied to local state machine.
func (sm *StateMachine) replyWhenLogApplied(logIndex uint64, ch chan<- *konsen.AppendDataResp) {
	condCh := make(chan struct{})
	sm.condMap.Store(logIndex, condCh)
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		select {
		case <-condCh:
			sm.condMap.Delete(logIndex)
			log.Debug("Log[%d] is committed and applied on local state machine.", logIndex)
			ch <- &konsen.AppendDataResp{Success: true}
		case <-time.After(defaultRequestTimeout):
			log.Debug("Timeout while waiting for log[%d] to be committed and applied.", logIndex)
			ch <- &konsen.AppendDataResp{
				Success:      false,
				ErrorMessage: fmt.Sprintf("failed to replicate onto quorum and apply commands (are there more than %d nodes down?)", sm.getQuorum()),
			}
		}
	}()
}

func (sm *StateMachine) GetSnapshot(ctx context.Context) (*Snapshot, error) {
	ch := make(chan *Snapshot)
	sm.msgCh <- getSnapshotMsg{ch: ch}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp, nil
	}
}

func (sm *StateMachine) handleGetSnapshot() (*Snapshot, error) {
	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return nil, fmt.Errorf("failed to get current term: %v", err)
	}
	logs, err := sm.storage.GetLogsFrom(1)
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
		Role:          sm.role,
		CurrentLeader: sm.currentLeader,
		NextIndex:     nextIndexMap,
		MatchIndex:    matchIndexMap,
		LogIndices:    logIndices,
		LogTerms:      logTerms,
		LogBytes:      logBytes,
	}, nil
}

func (sm *StateMachine) SetKeyValue(ctx context.Context, kv *konsen.KVList) error {
	buf, err := proto.Marshal(kv)
	if err != nil {
		return fmt.Errorf("failed to marshal: %v", err)
	}
	resp, err := sm.AppendData(ctx, &konsen.AppendDataReq{Data: buf})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf(resp.ErrorMessage)
	}
	return nil
}

func (sm *StateMachine) GetValue(ctx context.Context, key []byte) ([]byte, error) {
	ch := make(chan []byte)
	sm.msgCh <- getValueMsg{key: key, ch: ch}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp, nil
	}
}

func (sm *StateMachine) handleGetValue(key []byte) ([]byte, error) {
	return sm.storage.GetValue(key)
}

func (sm *StateMachine) Close() error {
	close(sm.stopCh)
	sm.wg.Wait()
	return nil
}
