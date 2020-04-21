package core

import (
	"context"
	"math/rand"
	"sync"
	"time"

	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	log "github.com/sirupsen/logrus"
)

const (
	defaultMinTimeout  = 200 * time.Millisecond
	defaultTimeoutSpan = int64(400)
	defaultHeartbeat   = 50 * time.Millisecond
)

type PersistentState interface {
	// Latest term server has seen (initialized to 0 on first boot, increases monotonically).
	GetCurrentTerm() int64

	//
	SetCurrentTerm(term int64)

	// Candidate ID that received a vote in current term, empty/blank if none.
	GetVotedFor() string

	//
	SetVotedFor(candidateID string)

	//
	GetLogEntry(logIndex int64) konsen.LogEntry

	//
	AppendLogEntry(logEntry konsen.LogEntry) error
}

type StateMachine struct {
	msgCh           chan interface{} // Message channel.
	stopCh          chan struct{}    // Signals to stop the state machine.
	timerGateCh     chan struct{}    // Signals to run next round of election timeout.
	heartbeatGateCh chan struct{}
	resetTimerCh    chan struct{} // Signals to reset election timer, in case of receiving AppendEntries or RequestVote.

	// Persistent state on all servers.
	persistentState PersistentState

	// Volatile state on all servers.
	commitIndex int64       // Index of highest log entry known to be committed.
	lastApplied int64       // Index of highest log entry applied to state machine.
	role        konsen.Role // Current role.

	// Volatile state on leaders (must be reinitialized after election).
	nextIndex  map[string]int64 // For each server, index of the next log entry to send to that server.
	matchIndex map[string]int64 // For each server, index of highest log entry known to be replicated on that server.
}

func NewStateMachine() *StateMachine {
	sm := &StateMachine{
		msgCh:           make(chan interface{}),
		stopCh:          make(chan struct{}),
		timerGateCh:     make(chan struct{}),
		heartbeatGateCh: make(chan struct{}),
		resetTimerCh:    make(chan struct{}),

		commitIndex: 0,
		lastApplied: 0,
		role:        konsen.Role_FOLLOWER,

		nextIndex:  make(map[string]int64),
		matchIndex: make(map[string]int64),
	}

	return sm
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
			// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3).
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
					if v.Term > sm.persistentState.GetCurrentTerm() {
						sm.role = konsen.Role_FOLLOWER
						// TODO.
					}

					sm.timerGateCh <- struct{}{}
				case konsen.RequestVoteReq:
					sm.resetTimerCh <- struct{}{}

					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
					if v.Term > sm.persistentState.GetCurrentTerm() {
						sm.role = konsen.Role_FOLLOWER
						// TODO.
					}

					sm.timerGateCh <- struct{}{}
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
				// TODO: add election timeout logic.
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

func closeTimer(timer *time.Timer) {
	if !timer.Stop() {
		<-timer.C
	}
}
