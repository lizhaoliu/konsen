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

func (sm *StateMachine) checkTerm(term, currentTerm uint64) error {
	if term > currentTerm {
		if err := sm.storage.SetCurrentTerm(term); err != nil {
			return fmt.Errorf("failed  to set current term: %v", err)
		}
		sm.role = konsen.Role_FOLLOWER
	}
	return nil
}

// startMessageLoop starts the main message loop.
func (sm *StateMachine) startMessageLoop(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
			for sm.commitIndex > sm.lastApplied {
				// TODO: apply log[lastApplied].
				sm.lastApplied++
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
					term := v.req.GetTerm()
					currentTerm, err := sm.storage.GetCurrentTerm()
					if err != nil {
						log.Fatalf("Failed to get current term: %v", err)
					}
					if err := sm.checkTerm(term, currentTerm); err != nil {
						log.Errorf("%v", err)
					}

					// 1. Reply false if term < currentTerm.
					if term < currentTerm {
						v.ch <- &konsen.AppendEntriesResp{
							Term:    currentTerm,
							Success: false,
						}
						continue
					}

					// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
					logEntry, err := sm.storage.GetLog(v.req.GetPrevLogIndex())
					if err != nil {
						log.Fatalf("Failed to get log: %v", err)
					}
					if logEntry.GetTerm() != v.req.GetPrevLogTerm() {
						v.ch <- &konsen.AppendEntriesResp{
							Term:    currentTerm,
							Success: false,
						}
						continue
					}

					// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
					// TODO.

					// 4. Append any new entries not already in the log.
					// TODO.

					// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
					if v.req.GetLeaderCommit() > sm.commitIndex {
						// TODO.
					}

					sm.timerGateCh <- struct{}{}
				case requestVoteWrap:
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.

					sm.timerGateCh <- struct{}{}
				case konsen.RequestVoteReq:
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.

					sm.timerGateCh <- struct{}{}
				case konsen.RequestVoteResp:
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.

					sm.timerGateCh <- struct{}{}
				case electionTimeout:
					// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate.
					sm.role = konsen.Role_CANDIDATE

					// On conversion to candidate, start election:
					//  Increment currentTerm.
					//sm.storage.SetCurrentTerm(sm.storage.GetCurrentTerm() + 1)
					//  Vote for self.
					//  Reset election timer.
					sm.resetTimerCh <- struct{}{}
					//  Send RequestVote RPCs to all other servers.
					//  If votes received from majority of servers: become leader.
					//  If AppendEntries RPC received from new leader: convert to follower.
					//  If election timeout elapses: start new election.
				default:
					log.Warnf("Unknown message: %v", v)
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
