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
	defaultTimeoutSpan = int64(500)
	defaultHeartbeat   = 40 * time.Millisecond
)

// electionTimeout represents a signal for election timeout event.
type electionTimeout struct{}

// StateMachine is the state machine that implements Raft algorithm.
type StateMachine struct {
	msgCh           chan interface{} // Message channel.
	stopCh          chan struct{}    // Signals to stop the state machine.
	timerGateCh     chan struct{}    // Signals to run next round of election timeout.
	heartbeatGateCh chan struct{}    //
	resetTimerCh    chan struct{}    // Signals to reset election timer, in case of receiving AppendEntries or RequestVote.

	// Persistent state storage on all servers.
	storage Storage

	// Volatile state on all servers.
	commitIndex uint64      // Index of highest log entry known to be committed.
	lastApplied uint64      // Index of highest log entry applied to state machine.
	role        konsen.Role // Current role.

	// Volatile state on leaders (must be reinitialized after election).
	nextIndex  map[string]uint64 // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1).
	matchIndex map[string]uint64 // For each server, index of highest log entry known to be replicated on that server (initialized to 0, increases monotonically).
}

type appendEntriesWrap struct {
	req *konsen.AppendEntriesReq
	ch  chan<- *konsen.AppendEntriesResp
}

type requestVoteWrap struct {
	req *konsen.RequestVoteReq
	ch  chan<- *konsen.RequestVoteResp
}

// NewStateMachine
func NewStateMachine(cluster *konsen.Cluster, storage Storage) (*StateMachine, error) {
	if len(cluster.GetNodes())%2 != 1 {
		return nil, fmt.Errorf("number of nodes in the cluster must be an odd number, got: %d", len(cluster.GetNodes()))
	}

	sm := &StateMachine{
		msgCh:           make(chan interface{}),
		stopCh:          make(chan struct{}),
		timerGateCh:     make(chan struct{}),
		heartbeatGateCh: make(chan struct{}),
		resetTimerCh:    make(chan struct{}),

		storage: storage,

		commitIndex: 0,
		lastApplied: 0,
		role:        konsen.Role_FOLLOWER,

		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
	}

	return sm, nil
}

// Start starts the state machine and blocks until done.
func (sm *StateMachine) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	sm.startMessageLoop(ctx, &wg)
	sm.startElectionLoop(ctx, &wg)
	sm.startHeartbeatLoop(ctx, &wg)
	wg.Wait()
	return nil
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
	return nil
}

func (sm *StateMachine) maybeUpdateCurrentTerm(otherTerm uint64) (uint64, error) {
	currentTerm, err := sm.storage.GetCurrentTerm()
	if err != nil {
		return 0, fmt.Errorf("failed to get current term: %v", err)
	}
	if otherTerm > currentTerm {
		if err := sm.storage.SetCurrentTerm(otherTerm); err != nil {
			return 0, fmt.Errorf("failed to set current term to %d: %v", otherTerm, err)
		}
		currentTerm = otherTerm
		sm.role = konsen.Role_FOLLOWER
	}
	return currentTerm, nil
}

// startMessageLoop starts the main message loop.
func (sm *StateMachine) startMessageLoop(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

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
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
					currentTerm, err := sm.maybeUpdateCurrentTerm(v.req.GetTerm())
					if err != nil {
						log.Fatalf("%v", err)
					}

					// 1. Reply false if term < currentTerm.
					if v.req.GetTerm() < currentTerm {
						v.ch <- &konsen.AppendEntriesResp{
							Term:    currentTerm,
							Success: false,
						}
						continue
					}

					// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
					prevLog, err := sm.storage.GetLog(v.req.GetPrevLogIndex())
					if err != nil {
						log.Fatalf("Failed to get log at index %d: %v", v.req.GetPrevLogIndex(), err)
					}
					if prevLog != nil && prevLog.GetTerm() != v.req.GetPrevLogTerm() {
						v.ch <- &konsen.AppendEntriesResp{
							Term:    currentTerm,
							Success: false,
						}
						continue
					}

					// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
					for _, newLog := range v.req.GetEntries() {
						localLog, err := sm.storage.GetLog(newLog.GetIndex())
						if err != nil {
							log.Fatalf("Failed to get log at index %d: %v", newLog.GetIndex(), err)
						}
						if localLog != nil && localLog.GetTerm() != newLog.GetTerm() {
							if err := sm.storage.DeleteLogs(newLog.GetIndex()); err != nil {
								log.Fatalf("Failed to delete logs with min index %d: %v", newLog.GetIndex(), err)
							}
						}
					}

					// 4. Append any new entries not already in the log.
					if err := sm.storage.PutLogs(v.req.GetEntries()); err != nil {
						log.Fatalf("Failed to store logs: %v", err)
					}

					// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
					if v.req.GetLeaderCommit() > sm.commitIndex {
						sm.commitIndex = v.req.GetLeaderCommit()
						lastLogIndex, err := sm.storage.LastLogIndex()
						if err != nil {
							log.Fatalf("Failed to get index of the last log: %v", err)
						}
						if lastLogIndex > sm.commitIndex {
							sm.commitIndex = lastLogIndex
						}
					}

					// Reply with success.
					v.ch <- &konsen.AppendEntriesResp{
						Term:    currentTerm,
						Success: true,
					}

					sm.timerGateCh <- struct{}{}
				case requestVoteWrap:
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
					currentTerm, err := sm.maybeUpdateCurrentTerm(v.req.GetTerm())
					if err != nil {
						log.Fatalf("%v", err)
					}

					// 1. Reply false if term < currentTerm.
					if v.req.GetTerm() < currentTerm {
						v.ch <- &konsen.RequestVoteResp{
							Term:        currentTerm,
							VoteGranted: false,
						}
						continue
					}

					// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote.
					votedFor, err := sm.storage.GetVotedFor()
					if err != nil {
						log.Fatalf("Failed to get votedFor: %v", err)
					}
					// If not yet voted for anyone or already voted for this candidate.
					if votedFor == "" || votedFor == v.req.GetCandidateId() {
						if err := sm.grantVote(currentTerm, v.req.GetCandidateId(), v.ch); err != nil {
							log.Fatalf("Failed to grant vote: %v", err)
						}
						continue
					}

					// If candidate’s log is at least as up-to-date as receiver’s log, grant vote.
					// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
					lastLogTerm, err := sm.storage.LastLogTerm()
					if err != nil {
						log.Fatalf("Failed to get last log's term: %v", err)
					}
					if v.req.GetLastLogTerm() > lastLogTerm {
						if err := sm.grantVote(currentTerm, v.req.GetCandidateId(), v.ch); err != nil {
							log.Fatalf("Failed to grant vote: %v", err)
						}
						continue
					}

					// If the logs end with the same term, then whichever log is longer is more up-to-date.
					lastLogIndex, err := sm.storage.LastLogIndex()
					if err != nil {
						log.Fatalf("Failed to get last log's index: %v", err)
					}
					if v.req.GetLastLogTerm() == lastLogTerm && v.req.GetLastLogIndex() >= lastLogIndex {
						if err := sm.grantVote(currentTerm, v.req.GetCandidateId(), v.ch); err != nil {
							log.Fatalf("Failed to grant vote: %v", err)
						}
						continue
					}

					// The candidate's log is not as up-to-date as receiver's, deny the vote.
					v.ch <- &konsen.RequestVoteResp{
						Term:        currentTerm,
						VoteGranted: false,
					}

					sm.timerGateCh <- struct{}{}
				case konsen.AppendEntriesResp:
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
					currentTerm, err := sm.maybeUpdateCurrentTerm(v.GetTerm())
					if err != nil {
						log.Fatalf("%v", err)
					}
					currentTerm = currentTerm

					sm.timerGateCh <- struct{}{}
				case konsen.RequestVoteResp:
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
					currentTerm, err := sm.maybeUpdateCurrentTerm(v.GetTerm())
					if err != nil {
						log.Fatalf("%v", err)
					}
					currentTerm = currentTerm

					sm.timerGateCh <- struct{}{}
				case electionTimeout:
					// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate.
					sm.role = konsen.Role_CANDIDATE

					currentTerm, err := sm.storage.GetCurrentTerm()
					if err != nil {
						log.Fatalf("Failed to get current term: %v", err)
					}
					// On conversion to candidate, start election:
					// 1. Increment currentTerm.
					currentTerm++
					if err := sm.storage.SetCurrentTerm(currentTerm); err != nil {
						log.Fatalf("Failed to set current term to %d: %v", currentTerm, err)
					}
					// 2. TODO: Vote for self.
					// 3. Reset election timer.
					sm.resetTimerCh <- struct{}{}
					// 4. TODO: Send RequestVote RPCs to all other servers.

					// 5. TODO: If votes received from majority of servers: become leader.
					// 6. TODO: If AppendEntries RPC received from new leader: convert to follower.
					// 7. TODO: If election timeout elapses: start new election.
				default:
					log.Fatalf("Unrecognized message: %v", v)
				}
			}
		}
	}()
}

// startElectionLoop starts the election timeout monitoring loop.
func (sm *StateMachine) startElectionLoop(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Election timer worker.
		for {
			// Waits for signal.
			<-sm.timerGateCh

			timer := time.NewTimer(sm.nextTimeout())
			select {
			case <-ctx.Done():
				return
			case <-sm.stopCh:
				return
			case <-sm.resetTimerCh:
				timer.Stop()
				continue
			case <-timer.C:
				// Election timeout happens here.
				log.Traceln("Election timeout occurs.")
				sm.msgCh <- electionTimeout{}
			}
		}
	}()
}

// startHeartbeatLoop starts the heartbeat loop.
func (sm *StateMachine) startHeartbeatLoop(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

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
				// TODO: send out heartbeat.
				if sm.role != konsen.Role_LEADER {
					continue
				}
			}
		}
	}()
}

// nextTimeout calculates the next election timeout duration.
func (sm *StateMachine) nextTimeout() time.Duration {
	timeout := rand.Int63n(defaultTimeoutSpan) + int64(defaultMinTimeout)
	return time.Duration(timeout)
}

func (sm *StateMachine) Close() error {
	close(sm.stopCh)
	return nil
}
