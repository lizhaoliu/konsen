package core

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	log "github.com/sirupsen/logrus"
)

const (
	defaultMinTimeout  = 500 * time.Millisecond
	defaultTimeoutSpan = 500 * time.Millisecond
	defaultHeartbeat   = 50 * time.Millisecond
)

// StateMachine is the state machine that implements Raft algorithm: https://raft.github.io/raft.pdf.
type StateMachine struct {
	msgCh        chan interface{} // Message channel.
	stopCh       chan struct{}    // Signals to stop the state machine.
	timerGateCh  chan struct{}    // Signals to run next round of election timeout.
	resetTimerCh chan struct{}    // Signals to reset election timer, in case of receiving AppendEntries or RequestVote.

	// Persistent state storage on all servers.
	storage Storage

	// Volatile state on all servers.
	commitIndex uint64      // Index of highest log entry known to be committed.
	lastApplied uint64      // Index of highest log entry applied to state machine.
	role        konsen.Role // Current role.

	// Volatile state on candidates.
	numVotes int

	// Volatile state on leaders (must be reinitialized after election).
	nextIndex  map[string]uint64 // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1).
	matchIndex map[string]uint64 // For each server, index of highest log entry known to be replicated on that server (initialized to 0, increases monotonically).

	// ClusterConfig info.
	cluster *ClusterConfig
	clients map[string]RaftClient

	wg sync.WaitGroup
}

// StateMachineConfig
type StateMachineConfig struct {
	Storage Storage               // Local storage instance.
	Cluster *ClusterConfig        // Cluster configuration.
	Clients map[string]RaftClient // A map of {endpoint: client instance}.
}

type appendEntriesWrap struct {
	req *konsen.AppendEntriesReq
	ch  chan<- *konsen.AppendEntriesResp
}

type appendEntriesRespWrap struct {
	resp     *konsen.AppendEntriesResp
	req      *konsen.AppendEntriesReq
	endpoint string
}

type requestVoteWrap struct {
	req *konsen.RequestVoteReq
	ch  chan<- *konsen.RequestVoteResp
}

// electionTimeout represents a signal for election timeout event.
type electionTimeout struct{}

// appendEntriesSignal represents a signal to send AppendEntries request to all nodes.
type appendEntriesSignal struct{}

// NewStateMachine
func NewStateMachine(config StateMachineConfig) (*StateMachine, error) {
	if len(config.Cluster.Endpoints)%2 != 1 {
		return nil, fmt.Errorf("number of nodes in the cluster must be an odd number, got: %d", len(config.Cluster.Endpoints))
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
	sm.startMessageLoop(ctx)
	sm.startElectionLoop(ctx)
	sm.wg.Wait()
}

// AppendEntries puts the incoming AppendEntries request in main message channel and waits for result.
func (sm *StateMachine) AppendEntries(ctx context.Context, req *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
	ch := make(chan *konsen.AppendEntriesResp)
	wrap := appendEntriesWrap{
		req: req,
		ch:  ch,
	}
	sm.msgCh <- wrap
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
	wrap := requestVoteWrap{
		req: req,
		ch:  ch,
	}
	sm.msgCh <- wrap
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp, nil
	}
}

func (sm *StateMachine) grantVote(term uint64, candidateID string, ch chan<- *konsen.RequestVoteResp) error {
	if err := sm.storage.SetVotedFor(candidateID); err != nil {
		return err
	}
	ch <- &konsen.RequestVoteResp{
		Term:        term,
		VoteGranted: true,
	}
	log.Debug("Granted vote for candidate %q for term %d", candidateID, term)
	return nil
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

func (sm *StateMachine) handleAppendEntries(req *konsen.AppendEntriesReq, ch chan<- *konsen.AppendEntriesResp) error {
	sm.resetTimerCh <- struct{}{}
	defer func() { sm.timerGateCh <- struct{}{} }()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	currentTerm, err := sm.maybeBecomeFollower(req.GetTerm())
	if err != nil {
		return err
	}

	// 1. Reply false if term < currentTerm.
	if req.GetTerm() < currentTerm {
		ch <- &konsen.AppendEntriesResp{
			Term:    currentTerm,
			Success: false,
		}
		return nil
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
	prevLogTerm, err := sm.storage.GetLogTerm(req.GetPrevLogIndex())
	if err != nil {
		return fmt.Errorf("failed to get log at index %d: %v", req.GetPrevLogIndex(), err)
	}
	if prevLogTerm != req.GetPrevLogTerm() {
		ch <- &konsen.AppendEntriesResp{
			Term:    currentTerm,
			Success: false,
		}
		return nil
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
	startIdx := 0
	for i, newLog := range req.GetEntries() {
		localLog, err := sm.storage.GetLog(newLog.GetIndex())
		if err != nil {
			return fmt.Errorf("failed to get log at index %d: %v", newLog.GetIndex(), err)
		}
		if localLog == nil {
			startIdx = i
			break
		}
		if localLog.GetTerm() != newLog.GetTerm() {
			if err := sm.storage.DeleteLogsFrom(newLog.GetIndex()); err != nil {
				return fmt.Errorf("failed to delete logs from min index %d: %v", newLog.GetIndex(), err)
			}
			startIdx = i
			break
		}
	}

	// 4. Append any new entries not already in the log.
	if err := sm.storage.WriteLogs(req.GetEntries()[startIdx:]); err != nil {
		return fmt.Errorf("failed to write logs: %v", err)
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
	if req.GetLeaderCommit() > sm.commitIndex {
		sm.commitIndex = req.GetLeaderCommit()
		lastLogIndex, err := sm.storage.LastLogIndex()
		if err != nil {
			return fmt.Errorf("failed to get index of the last log: %v", err)
		}
		if lastLogIndex < sm.commitIndex {
			sm.commitIndex = lastLogIndex
		}
	}

	// Reply with success.
	ch <- &konsen.AppendEntriesResp{
		Term:    currentTerm,
		Success: true,
	}

	return nil
}

func (sm *StateMachine) handleAppendEntriesResp(
	resp *konsen.AppendEntriesResp,
	req *konsen.AppendEntriesReq,
	endpoint string) error {
	sm.resetTimerCh <- struct{}{}
	defer func() { sm.timerGateCh <- struct{}{} }()

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

		// Pure heartbeat, can return now.
		if numEntries == 0 {
			return nil
		}

		// Successful: update nextIndex and matchIndex for follower.
		sm.nextIndex[endpoint] = req.GetPrevLogIndex() + uint64(numEntries) + 1
		sm.matchIndex[endpoint] = req.GetPrevLogIndex() + uint64(numEntries)

		// If there exists N: N > commitIndex && a majority of matchIndex[i] ≥ N && log[N].term == currentTerm, then set commitIndex = N.
		for i := numEntries - 1; i >= 0; i-- {
			idx := req.GetEntries()[i].GetIndex()
			logTerm, err := sm.storage.GetLogTerm(idx)
			if err != nil {
				return fmt.Errorf("failed to get log term at index %d: %v", idx, err)
			}
			if idx > sm.commitIndex && sm.isLogOnMajority(idx) && logTerm == currentTerm {
				sm.commitIndex = idx
				break
			}
		}
	} else {
		// AppendEntries fails because of log inconsistency: decrement nextIndex and retry.
		sm.nextIndex[endpoint]--
	}

	return nil
}

func (sm *StateMachine) isLogOnMajority(logIndex uint64) bool {
	n := 1 // Already on this server.
	for _, endpoint := range sm.cluster.Endpoints {
		if endpoint != sm.cluster.LocalEndpoint && sm.matchIndex[endpoint] >= logIndex {
			n++
		}
	}
	return n > len(sm.cluster.Endpoints)/2
}

func (sm *StateMachine) handleRequestVote(req *konsen.RequestVoteReq, ch chan<- *konsen.RequestVoteResp) error {
	sm.resetTimerCh <- struct{}{}
	defer func() { sm.timerGateCh <- struct{}{} }()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	currentTerm, err := sm.maybeBecomeFollower(req.GetTerm())
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	// 1. Reply false if term < currentTerm.
	if req.GetTerm() < currentTerm {
		ch <- &konsen.RequestVoteResp{
			Term:        currentTerm,
			VoteGranted: false,
		}
		return nil
	}

	// Now candidate's term == currentTerm.

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote.
	votedFor, err := sm.storage.GetVotedFor()
	if err != nil {
		return fmt.Errorf("failed to get votedFor: %v", err)
	}

	// If already voted for another candidate, deny the vote.
	if votedFor != "" && votedFor != req.GetCandidateId() {
		ch <- &konsen.RequestVoteResp{
			Term:        currentTerm,
			VoteGranted: false,
		}
		return nil
	}

	// If candidate’s log is at least as up-to-date as receiver’s log, grant vote.

	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	lastLogTerm, err := sm.storage.LastLogTerm()
	if err != nil {
		return fmt.Errorf("failed to get last log's term: %v", err)
	}
	// Candidate's last log term is older, deny the vote.
	if req.GetLastLogTerm() < lastLogTerm {
		ch <- &konsen.RequestVoteResp{
			Term:        currentTerm,
			VoteGranted: false,
		}
		return nil
	}

	// Candidate's last log term is newer, grant vote.
	if req.GetLastLogTerm() > lastLogTerm {
		if err := sm.grantVote(currentTerm, req.GetCandidateId(), ch); err != nil {
			return fmt.Errorf("failed to grant vote: %v", err)
		}
		return nil
	}

	// If last logs have the same term, then whichever log is longer is more up-to-date.
	lastLogIndex, err := sm.storage.LastLogIndex()
	if err != nil {
		return fmt.Errorf("failed to get last log's index: %v", err)
	}
	if req.GetLastLogIndex() >= lastLogIndex {
		if err := sm.grantVote(currentTerm, req.GetCandidateId(), ch); err != nil {
			return fmt.Errorf("failed to grant vote: %v", err)
		}
		return nil
	}

	// The candidate's log is not as up-to-date as receiver's, deny the vote.
	ch <- &konsen.RequestVoteResp{
		Term:        currentTerm,
		VoteGranted: false,
	}

	return nil
}

func (sm *StateMachine) handleRequestVoteResp(resp *konsen.RequestVoteResp) error {
	sm.resetTimerCh <- struct{}{}
	defer func() { sm.timerGateCh <- struct{}{} }()

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
		if sm.numVotes > len(sm.cluster.Endpoints)/2 {
			if err := sm.becomeLeader(currentTerm); err != nil {
				return fmt.Errorf("failed to become leader: %v", err)
			}
		}
	}

	return nil
}

func (sm *StateMachine) becomeLeader(term uint64) error {
	log.Infof("Term - %d, leader - %q.", term, sm.cluster.LocalEndpoint)
	sm.role = konsen.Role_LEADER
	lastLogIndex, err := sm.storage.LastLogIndex()
	if err != nil {
		return err
	}
	for c := range sm.nextIndex {
		sm.nextIndex[c] = lastLogIndex + 1
	}
	for c := range sm.matchIndex {
		sm.matchIndex[c] = 0
	}

	sm.startHeartbeatLoop(context.Background())

	return nil
}

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
		CandidateId:  sm.cluster.LocalEndpoint,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	for _, endpoint := range sm.cluster.Endpoints {
		if endpoint != sm.cluster.LocalEndpoint {
			endpoint := endpoint
			go func() {
				resp, err := sm.clients[endpoint].RequestVote(ctx, req)
				if err != nil {
					log.Debug("Failed to send RequestVote to %q: %v", endpoint, err)
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

func (sm *StateMachine) sendAppendEntries(ctx context.Context) error {
	if sm.role != konsen.Role_LEADER {
		return nil
	}

	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return fmt.Errorf("failed to get current term: %v", err)
	}

	for _, endpoint := range sm.cluster.Endpoints {
		if endpoint != sm.cluster.LocalEndpoint {
			prevLogIndex := sm.nextIndex[endpoint] - 1
			prevLogTerm, err := sm.storage.GetLogTerm(prevLogIndex)
			if err != nil {
				return fmt.Errorf("failed to get log term at index %d: %v", prevLogIndex, err)
			}
			entries, err := sm.storage.GetLogsFrom(sm.nextIndex[endpoint])
			if err != nil {
				return fmt.Errorf("failed to get logs from index %d: %v", sm.nextIndex[endpoint], err)
			}
			req := &konsen.AppendEntriesReq{
				Term:         currentTerm,
				LeaderId:     sm.cluster.LocalEndpoint,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: sm.commitIndex,
			}

			endpoint := endpoint
			go func() {
				resp, err := sm.clients[endpoint].AppendEntries(ctx, req)
				if err != nil {
					log.Debug("Failed to send AppendEntries to %q: %v", endpoint, err)
				}
				select {
				case sm.msgCh <- appendEntriesRespWrap{
					resp:     resp,
					req:      req,
					endpoint: endpoint,
				}:
				case <-sm.stopCh:
				}
			}()
		}
	}

	return nil
}

func (sm *StateMachine) handleElectionTimeout() error {
	// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate.
	sm.role = konsen.Role_CANDIDATE
	sm.numVotes = 0

	// On conversion to candidate, start election:
	// 1. Increment currentTerm.
	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return fmt.Errorf("failed to get current term: %v", err)
	}
	currentTerm++
	if err := sm.storage.SetCurrentTerm(currentTerm); err != nil {
		return fmt.Errorf("failed to set current term to %d: %v", currentTerm, err)
	}

	// 2. Vote for self.
	if err := sm.storage.SetVotedFor(sm.cluster.LocalEndpoint); err != nil {
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

// startMessageLoop starts the main message loop.
func (sm *StateMachine) startMessageLoop(ctx context.Context) {
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()

		for {
			// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
			for sm.commitIndex > sm.lastApplied {
				sm.lastApplied++
				// TODO: apply log[lastApplied].
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
					if err := sm.handleAppendEntries(v.req, v.ch); err != nil {
						log.Fatalf("%v", err)
					}
				case requestVoteWrap:
					// Process incoming RequestVote request.
					if err := sm.handleRequestVote(v.req, v.ch); err != nil {
						log.Fatalf("%v", err)
					}
				case appendEntriesRespWrap:
					if err := sm.handleAppendEntriesResp(v.resp, v.req, v.endpoint); err != nil {
						log.Fatalf("%v", err)
					}
				case *konsen.RequestVoteResp:
					if err := sm.handleRequestVoteResp(v); err != nil {
						log.Fatalf("%v", err)
					}
				case electionTimeout:
					if err := sm.handleElectionTimeout(); err != nil {
						log.Fatalf("%v", err)
					}
				case appendEntriesSignal:
					if err := sm.sendAppendEntries(context.Background()); err != nil {
						log.Fatalf("%v", err)
					}
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
				log.Warnf("Election timeout occurs.")
				sm.msgCh <- electionTimeout{}
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
				// TODO: potential data race, use a channel to signal role change.
				if sm.role != konsen.Role_LEADER {
					return
				}
				sm.msgCh <- appendEntriesSignal{}
			}
		}
	}()
}

// nextTimeout calculates the next election timeout duration.
func (sm *StateMachine) nextTimeout() time.Duration {
	timeout := rand.Int63n(int64(defaultTimeoutSpan)) + int64(defaultMinTimeout)
	return time.Duration(timeout)
}

func (sm *StateMachine) Close() error {
	close(sm.stopCh)
	return nil
}
