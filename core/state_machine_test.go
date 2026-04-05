package core

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lizhaoliu/konsen/v2/datastore"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"google.golang.org/protobuf/proto"
)

// mockRaftService is a test double for RaftService that records calls and returns configured responses.
type mockRaftService struct {
	mu sync.Mutex

	appendEntriesFunc func(ctx context.Context, in *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error)
	requestVoteFunc   func(ctx context.Context, in *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error)
	appendDataFunc    func(ctx context.Context, in *konsen.AppendDataReq) (*konsen.AppendDataResp, error)

	appendEntriesCalls []*konsen.AppendEntriesReq
	requestVoteCalls   []*konsen.RequestVoteReq
	appendDataCalls    []*konsen.AppendDataReq
}

func newMockRaftService() *mockRaftService {
	return &mockRaftService{
		appendEntriesFunc: func(ctx context.Context, in *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
			return &konsen.AppendEntriesResp{Term: in.GetTerm(), Success: true}, nil
		},
		requestVoteFunc: func(ctx context.Context, in *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
			return &konsen.RequestVoteResp{Term: in.GetTerm(), VoteGranted: true}, nil
		},
		appendDataFunc: func(ctx context.Context, in *konsen.AppendDataReq) (*konsen.AppendDataResp, error) {
			return &konsen.AppendDataResp{Success: true}, nil
		},
	}
}

func (m *mockRaftService) AppendEntries(ctx context.Context, in *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
	m.mu.Lock()
	m.appendEntriesCalls = append(m.appendEntriesCalls, in)
	fn := m.appendEntriesFunc
	m.mu.Unlock()
	return fn(ctx, in)
}

func (m *mockRaftService) RequestVote(ctx context.Context, in *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
	m.mu.Lock()
	m.requestVoteCalls = append(m.requestVoteCalls, in)
	fn := m.requestVoteFunc
	m.mu.Unlock()
	return fn(ctx, in)
}

func (m *mockRaftService) AppendData(ctx context.Context, in *konsen.AppendDataReq) (*konsen.AppendDataResp, error) {
	m.mu.Lock()
	m.appendDataCalls = append(m.appendDataCalls, in)
	fn := m.appendDataFunc
	m.mu.Unlock()
	return fn(ctx, in)
}

func (m *mockRaftService) Close() error { return nil }

// makeTestCluster creates a 3-node cluster config for testing.
func makeTestCluster(localName string) *ClusterConfig {
	return &ClusterConfig{
		Servers: map[string]string{
			"node1": "localhost:10001",
			"node2": "localhost:10002",
			"node3": "localhost:10003",
		},
		LocalServerName: localName,
	}
}

// makeTestSM creates a test state machine with mock clients and starts its message loop.
// Returns the state machine and a cancel function.
//
// NOTE: Several tests access sm.storage directly (e.g. SetCurrentTerm, WriteLog) to set up
// preconditions while the message loop is running. This is safe because MemStorage has its
// own mutex protecting all operations. However, it bypasses the state machine's single-writer
// invariant (all state access via the message channel). This trade-off is acceptable in tests
// to avoid needing to funnel setup through RPCs.
func makeTestSM(t *testing.T, localName string) (*StateMachine, map[string]*mockRaftService, context.CancelFunc) {
	t.Helper()
	return makeTestSMWithConfig(t, localName, StateMachineConfig{})
}

// makeTestSMWithConfig creates a test state machine with custom config overrides.
func makeTestSMWithConfig(t *testing.T, localName string, override StateMachineConfig) (*StateMachine, map[string]*mockRaftService, context.CancelFunc) {
	t.Helper()
	storage := datastore.NewMemStorage()
	cluster := makeTestCluster(localName)

	mocks := make(map[string]*mockRaftService)
	clients := make(map[string]RaftService)
	for name := range cluster.Servers {
		if name != localName {
			m := newMockRaftService()
			mocks[name] = m
			clients[name] = m
		}
	}

	cfg := StateMachineConfig{
		Storage:     storage,
		Cluster:     cluster,
		Clients:     clients,
		MinTimeout:  override.MinTimeout,
		TimeoutSpan: override.TimeoutSpan,
		Heartbeat:   override.Heartbeat,
	}

	sm, err := NewStateMachine(cfg)
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go sm.Run(ctx)

	// Give the message loop time to start.
	time.Sleep(10 * time.Millisecond)

	return sm, mocks, cancel
}

// stopSM cleanly shuts down a test state machine.
func stopSM(sm *StateMachine, cancel context.CancelFunc) {
	cancel()
	sm.Close()
}

func TestNewStateMachine_EvenNodes(t *testing.T) {
	_, err := NewStateMachine(StateMachineConfig{
		Storage: datastore.NewMemStorage(),
		Cluster: &ClusterConfig{
			Servers:         map[string]string{"n1": "a", "n2": "b"},
			LocalServerName: "n1",
		},
		Clients: map[string]RaftService{},
	})
	if err == nil {
		t.Fatal("expected error for even number of nodes")
	}
}

func TestNewStateMachine_StartsAsFollower(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	if sm.getRole() != konsen.Role_FOLLOWER {
		t.Errorf("initial role = %v, want FOLLOWER", sm.getRole())
	}
}

func TestAppendEntries_HeartbeatResetsTimer(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	// Send a heartbeat from a leader.
	resp, err := sm.AppendEntries(ctx, &konsen.AppendEntriesReq{
		Term:     1,
		LeaderId: "node2",
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !resp.GetSuccess() {
		t.Error("expected heartbeat to succeed")
	}
}

func TestAppendEntries_RejectStaleTerm(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	// Set current term to 5.
	sm.storage.SetTermAndVotedFor(5, "")

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	// Send AppendEntries with term 3 (stale).
	resp, err := sm.AppendEntries(ctx, &konsen.AppendEntriesReq{
		Term:     3,
		LeaderId: "node2",
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if resp.GetSuccess() {
		t.Error("expected rejection for stale term")
	}
	if resp.GetTerm() != 5 {
		t.Errorf("response term = %d, want 5", resp.GetTerm())
	}
}

func TestAppendEntries_HigherTermConvertsToFollower(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	sm.storage.SetCurrentTerm(1)

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	resp, err := sm.AppendEntries(ctx, &konsen.AppendEntriesReq{
		Term:     5,
		LeaderId: "node2",
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !resp.GetSuccess() {
		t.Error("expected success")
	}

	// Term should be updated.
	term, _ := sm.storage.GetCurrentTerm()
	if term != 5 {
		t.Errorf("term = %d, want 5", term)
	}
}

func TestAppendEntries_LogReplication(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	entries := []*konsen.Log{
		{Index: 1, Term: 1, Data: []byte("cmd1")},
		{Index: 2, Term: 1, Data: []byte("cmd2")},
	}

	resp, err := sm.AppendEntries(ctx, &konsen.AppendEntriesReq{
		Term:         1,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 0,
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !resp.GetSuccess() {
		t.Error("expected success for valid log replication")
	}

	// Verify logs were written.
	log1, _ := sm.storage.GetLog(1)
	log2, _ := sm.storage.GetLog(2)
	if log1 == nil || log2 == nil {
		t.Fatal("expected logs to be written")
	}
	if string(log1.GetData()) != "cmd1" || string(log2.GetData()) != "cmd2" {
		t.Error("log data mismatch")
	}
}

func TestAppendEntries_LogConflictResolution(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	// Pre-populate with conflicting log (index 2, term 1).
	sm.storage.WriteLogs([]*konsen.Log{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("old")},
	})

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	// Leader sends entry at index 2 with term 2 (conflict).
	resp, err := sm.AppendEntries(ctx, &konsen.AppendEntriesReq{
		Term:         2,
		LeaderId:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []*konsen.Log{{Index: 2, Term: 2, Data: []byte("new")}},
		LeaderCommit: 0,
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !resp.GetSuccess() {
		t.Error("expected success")
	}

	// Log at index 2 should now have term 2.
	log2, _ := sm.storage.GetLog(2)
	if log2.GetTerm() != 2 {
		t.Errorf("log[2].term = %d, want 2", log2.GetTerm())
	}
	if string(log2.GetData()) != "new" {
		t.Errorf("log[2].data = %q, want %q", log2.GetData(), "new")
	}
}

func TestAppendEntries_CommitIndexAdvances(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	sm.storage.WriteLogs([]*konsen.Log{
		{Index: 1, Term: 1, Data: marshalKV(t, "k1", "v1")},
	})

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	resp, err := sm.AppendEntries(ctx, &konsen.AppendEntriesReq{
		Term:         1,
		LeaderId:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		LeaderCommit: 1,
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !resp.GetSuccess() {
		t.Error("expected success")
	}

	// Give time for log application.
	time.Sleep(50 * time.Millisecond)

	snapshot, err := sm.GetSnapshot(ctx)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snapshot.CommitIndex != 1 {
		t.Errorf("commitIndex = %d, want 1", snapshot.CommitIndex)
	}
}

func TestAppendEntries_PrevLogMismatch(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	// Write log at index 1 with term 1.
	sm.storage.WriteLog(&konsen.Log{Index: 1, Term: 1, Data: []byte("x")})

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	// Request claims prevLog at index 1 has term 2, but local has term 1.
	resp, err := sm.AppendEntries(ctx, &konsen.AppendEntriesReq{
		Term:         2,
		LeaderId:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  2, // Mismatch.
		Entries:      []*konsen.Log{{Index: 2, Term: 2, Data: []byte("y")}},
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if resp.GetSuccess() {
		t.Error("expected failure for prevLog mismatch")
	}
}

func TestRequestVote_GrantVote(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	resp, err := sm.RequestVote(ctx, &konsen.RequestVoteReq{
		Term:         1,
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !resp.GetVoteGranted() {
		t.Error("expected vote to be granted")
	}
}

func TestRequestVote_RejectStaleTerm(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	sm.storage.SetCurrentTerm(5)

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	resp, err := sm.RequestVote(ctx, &konsen.RequestVoteReq{
		Term:        3,
		CandidateId: "node2",
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if resp.GetVoteGranted() {
		t.Error("expected vote to be denied for stale term")
	}
}

func TestRequestVote_RejectAlreadyVoted(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	sm.storage.SetTermAndVotedFor(1, "node3")

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	resp, err := sm.RequestVote(ctx, &konsen.RequestVoteReq{
		Term:        1,
		CandidateId: "node2",
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if resp.GetVoteGranted() {
		t.Error("expected vote to be denied (already voted for node3)")
	}
}

func TestRequestVote_AllowRevoteForSameCandidate(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	sm.storage.SetTermAndVotedFor(1, "node2")

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	resp, err := sm.RequestVote(ctx, &konsen.RequestVoteReq{
		Term:        1,
		CandidateId: "node2",
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !resp.GetVoteGranted() {
		t.Error("expected vote to be granted for same candidate")
	}
}

func TestRequestVote_RejectOutdatedLog(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	// Local node has a log at term 3.
	sm.storage.WriteLog(&konsen.Log{Index: 1, Term: 3, Data: []byte("x")})

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	resp, err := sm.RequestVote(ctx, &konsen.RequestVoteReq{
		Term:         4,
		CandidateId:  "node2",
		LastLogIndex: 1,
		LastLogTerm:  2, // Candidate's last log term is older.
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if resp.GetVoteGranted() {
		t.Error("expected vote to be denied for outdated log")
	}
}

func TestRequestVote_GrantForNewerLogTerm(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	sm.storage.WriteLog(&konsen.Log{Index: 1, Term: 1, Data: []byte("x")})

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	resp, err := sm.RequestVote(ctx, &konsen.RequestVoteReq{
		Term:         2,
		CandidateId:  "node2",
		LastLogIndex: 1,
		LastLogTerm:  2, // Candidate's log term is newer.
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !resp.GetVoteGranted() {
		t.Error("expected vote to be granted for newer log term")
	}
}

func TestRequestVote_GrantForLongerLog(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	sm.storage.WriteLog(&konsen.Log{Index: 1, Term: 1, Data: []byte("x")})

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	resp, err := sm.RequestVote(ctx, &konsen.RequestVoteReq{
		Term:         2,
		CandidateId:  "node2",
		LastLogIndex: 5, // Candidate has longer log.
		LastLogTerm:  1,
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !resp.GetVoteGranted() {
		t.Error("expected vote to be granted for longer log")
	}
}

func TestRequestVote_RejectShorterLog(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	// Local node has logs 1-3 all at term 1.
	sm.storage.WriteLogs([]*konsen.Log{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	resp, err := sm.RequestVote(ctx, &konsen.RequestVoteReq{
		Term:         2,
		CandidateId:  "node2",
		LastLogIndex: 2, // Candidate has shorter log.
		LastLogTerm:  1,
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if resp.GetVoteGranted() {
		t.Error("expected vote to be denied for shorter log")
	}
}

func TestElectionTimeout_BecomesCandidate(t *testing.T) {
	fastCfg := StateMachineConfig{
		MinTimeout:  100 * time.Millisecond,
		TimeoutSpan: 100 * time.Millisecond,
		Heartbeat:   20 * time.Millisecond,
	}
	sm, mocks, cancel := makeTestSMWithConfig(t, "node1", fastCfg)
	defer stopSM(sm, cancel)

	// Configure mock clients to deny votes so election doesn't complete.
	for _, m := range mocks {
		m.mu.Lock()
		m.requestVoteFunc = func(ctx context.Context, in *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
			return &konsen.RequestVoteResp{Term: in.GetTerm(), VoteGranted: false}, nil
		}
		m.mu.Unlock()
	}

	// Poll until term increments (election triggered).
	// NOTE: sm.storage is accessed directly here; this is safe because MemStorage
	// has its own mutex, but it bypasses the state machine's message channel.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		term, _ := sm.storage.GetCurrentTerm()
		if term >= 1 {
			return // success
		}
		time.Sleep(50 * time.Millisecond)
	}
	term, _ := sm.storage.GetCurrentTerm()
	t.Errorf("term = %d, expected >= 1 after election timeout", term)
}

func TestElectionTimeout_BecomesLeader(t *testing.T) {
	fastCfg := StateMachineConfig{
		MinTimeout:  100 * time.Millisecond,
		TimeoutSpan: 100 * time.Millisecond,
		Heartbeat:   20 * time.Millisecond,
	}
	sm, mocks, cancel := makeTestSMWithConfig(t, "node1", fastCfg)
	defer stopSM(sm, cancel)

	// Configure mock clients to grant votes.
	for _, m := range mocks {
		m.mu.Lock()
		m.requestVoteFunc = func(ctx context.Context, in *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
			return &konsen.RequestVoteResp{Term: in.GetTerm(), VoteGranted: true}, nil
		}
		m.mu.Unlock()
	}

	// Poll until the node becomes leader.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if sm.getRole() == konsen.Role_LEADER {
			return // success
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("role = %v, want LEADER after winning election", sm.getRole())
}

func TestAppendData_NoLeader(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	// No leader elected yet; directly inject data request while still a follower.
	resp, err := sm.AppendData(ctx, &konsen.AppendDataReq{Data: []byte("test")})
	if err != nil {
		t.Fatalf("AppendData: %v", err)
	}
	if resp.GetSuccess() {
		t.Error("expected failure when no leader")
	}
}

func TestGetValue_NotLeader(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	_, err := sm.GetValue(ctx, []byte("key"))
	if err == nil {
		t.Error("expected error when not leader")
	}
}

func TestGetSnapshot(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()

	snapshot, err := sm.GetSnapshot(ctx)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snapshot.Role != konsen.Role_FOLLOWER {
		t.Errorf("snapshot.Role = %v, want FOLLOWER", snapshot.Role)
	}
	if snapshot.CurrentTerm != 0 {
		t.Errorf("snapshot.CurrentTerm = %d, want 0", snapshot.CurrentTerm)
	}
}

func TestMajority(t *testing.T) {
	tests := []struct {
		nodes    int
		majority int
	}{
		{1, 1},
		{3, 2},
		{5, 3},
		{7, 4},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d_nodes", tt.nodes), func(t *testing.T) {
			servers := make(map[string]string)
			for i := 0; i < tt.nodes; i++ {
				name := fmt.Sprintf("node%d", i+1)
				servers[name] = fmt.Sprintf("localhost:%d", 10000+i)
			}
			sm := &StateMachine{
				cluster: &ClusterConfig{Servers: servers},
			}
			if got := sm.majority(); got != tt.majority {
				t.Errorf("majority() = %d, want %d", got, tt.majority)
			}
		})
	}
}

func TestClose_StopsStateMachine(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")

	// Close should return without blocking.
	cancel()
	done := make(chan struct{})
	go func() {
		sm.Close()
		close(done)
	}()

	select {
	case <-done:
		// OK.
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked for too long")
	}
}

func TestContextCancellation(t *testing.T) {
	sm, _, cancel := makeTestSM(t, "node1")
	defer stopSM(sm, cancel)

	ctx, c := context.WithCancel(context.Background())
	c() // Cancel immediately.

	_, err := sm.AppendEntries(ctx, &konsen.AppendEntriesReq{Term: 1, LeaderId: "node2"})
	if err == nil {
		t.Error("expected error for cancelled context")
	}

	_, err = sm.RequestVote(ctx, &konsen.RequestVoteReq{Term: 1, CandidateId: "node2"})
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

// marshalKV is a test helper to create serialized KVList data.
func marshalKV(t *testing.T, key, value string) []byte {
	t.Helper()
	kvList := &konsen.KVList{
		KvList: []*konsen.KV{{Key: []byte(key), Value: []byte(value)}},
	}
	data, err := proto.Marshal(kvList)
	if err != nil {
		t.Fatalf("failed to marshal KVList: %v", err)
	}
	return data
}
