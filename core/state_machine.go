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

func NewStateMachine(cluster *konsen.Cluster) (*StateMachine, error) {
	if len(cluster.GetNodes())%2 != 1 {
		return nil, fmt.Errorf("number of nodes in the cluster must be an odd number, got: %d", len(cluster.GetNodes()))
	}

	sm := &StateMachine{
		msgCh:           make(chan interface{}),
		stopCh:          make(chan struct{}),
		timerGateCh:     make(chan struct{}),
		heartbeatGateCh: make(chan struct{}),
		resetTimerCh:    make(chan struct{}),

		commitIndex: 0,
		lastApplied: 0,
		role:        konsen.Role_FOLLOWER,

		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
	}

	return sm, nil
}

func (sm *StateMachine) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	sm.startMessageLoop(ctx, &wg)
	sm.startElectionLoop(ctx, &wg)
	sm.startHeartbeatLoop(ctx, &wg)
	wg.Wait()
	return nil
}

func (sm *StateMachine) startMessageLoop(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		for {
			// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
			if sm.commitIndex > sm.lastApplied {
				// TODO.
			}

			select {
			case <-ctx.Done():
				return
			case <-sm.stopCh:
				return
			case msg := <-sm.msgCh:
				switch v := msg.(type) {
				case konsen.AppendEntriesReq:
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

					sm.timerGateCh <- struct{}{}
				case konsen.AppendEntriesResp:
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

					sm.timerGateCh <- struct{}{}
				case konsen.RequestVoteReq:
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

					sm.timerGateCh <- struct{}{}
				case konsen.RequestVoteResp:
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

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

func (sm *StateMachine) startElectionLoop(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
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

func (sm *StateMachine) startHeartbeatLoop(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
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

func (sm *StateMachine) nextTimeout() time.Duration {
	timeout := rand.Int63n(defaultTimeoutSpan) + int64(defaultMinTimeout)
	return time.Duration(timeout)
}

func (sm *StateMachine) Close() error {
	close(sm.stopCh)
	return nil
}
