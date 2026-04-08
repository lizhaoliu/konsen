package konsen_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lizhaoliu/konsen/v2/core"
	"github.com/lizhaoliu/konsen/v2/datastore"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// Faulty Storage: a wrapper that injects errors on demand
// ============================================================================

// faultFunc decides whether to return an error. callCount starts at 1.
type faultFunc func(callCount int) error

func alwaysFail(err error) faultFunc {
	return func(_ int) error { return err }
}

func failWithRate(rate float64, err error) faultFunc {
	return func(_ int) error {
		if rand.Float64() < rate {
			return err
		}
		return nil
	}
}

// faultyStorage wraps a real Storage and injects errors on configured methods.
type faultyStorage struct {
	inner      datastore.Storage
	mu         sync.RWMutex
	faults     map[string]faultFunc
	callCounts map[string]int
}

func newFaultyStorage(inner datastore.Storage) *faultyStorage {
	return &faultyStorage{
		inner:      inner,
		faults:     make(map[string]faultFunc),
		callCounts: make(map[string]int),
	}
}

func (f *faultyStorage) SetFault(method string, fn faultFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[method] = fn
	f.callCounts[method] = 0
}

func (f *faultyStorage) ClearFault(method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.faults, method)
	delete(f.callCounts, method)
}

func (f *faultyStorage) ClearAllFaults() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults = make(map[string]faultFunc)
	f.callCounts = make(map[string]int)
}

func (f *faultyStorage) maybeFault(method string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn, ok := f.faults[method]
	if !ok {
		return nil
	}
	f.callCounts[method]++
	return fn(f.callCounts[method])
}

// --- Storage interface delegation ---

func (f *faultyStorage) GetCurrentTerm() (uint64, error) {
	if err := f.maybeFault("GetCurrentTerm"); err != nil {
		return 0, err
	}
	return f.inner.GetCurrentTerm()
}

func (f *faultyStorage) SetCurrentTerm(term uint64) error {
	if err := f.maybeFault("SetCurrentTerm"); err != nil {
		return err
	}
	return f.inner.SetCurrentTerm(term)
}

func (f *faultyStorage) GetVotedFor() (string, error) {
	if err := f.maybeFault("GetVotedFor"); err != nil {
		return "", err
	}
	return f.inner.GetVotedFor()
}

func (f *faultyStorage) SetVotedFor(candidateID string) error {
	if err := f.maybeFault("SetVotedFor"); err != nil {
		return err
	}
	return f.inner.SetVotedFor(candidateID)
}

func (f *faultyStorage) SetTermAndVotedFor(term uint64, candidateID string) error {
	if err := f.maybeFault("SetTermAndVotedFor"); err != nil {
		return err
	}
	return f.inner.SetTermAndVotedFor(term, candidateID)
}

func (f *faultyStorage) GetLog(logIndex uint64) (*konsen.Log, error) {
	if err := f.maybeFault("GetLog"); err != nil {
		return nil, err
	}
	return f.inner.GetLog(logIndex)
}

func (f *faultyStorage) GetLogsFrom(minLogIndex uint64) ([]*konsen.Log, error) {
	if err := f.maybeFault("GetLogsFrom"); err != nil {
		return nil, err
	}
	return f.inner.GetLogsFrom(minLogIndex)
}

func (f *faultyStorage) GetLogTerm(logIndex uint64) (uint64, error) {
	if err := f.maybeFault("GetLogTerm"); err != nil {
		return 0, err
	}
	return f.inner.GetLogTerm(logIndex)
}

func (f *faultyStorage) WriteLog(log *konsen.Log) error {
	if err := f.maybeFault("WriteLog"); err != nil {
		return err
	}
	return f.inner.WriteLog(log)
}

func (f *faultyStorage) WriteLogs(logs []*konsen.Log) error {
	if err := f.maybeFault("WriteLogs"); err != nil {
		return err
	}
	return f.inner.WriteLogs(logs)
}

func (f *faultyStorage) LastLogIndex() (uint64, error) {
	if err := f.maybeFault("LastLogIndex"); err != nil {
		return 0, err
	}
	return f.inner.LastLogIndex()
}

func (f *faultyStorage) LastLogTerm() (uint64, error) {
	if err := f.maybeFault("LastLogTerm"); err != nil {
		return 0, err
	}
	return f.inner.LastLogTerm()
}

func (f *faultyStorage) DeleteLogsFrom(minLogIndex uint64) error {
	if err := f.maybeFault("DeleteLogsFrom"); err != nil {
		return err
	}
	return f.inner.DeleteLogsFrom(minLogIndex)
}

func (f *faultyStorage) SetValue(key []byte, value []byte) error {
	if err := f.maybeFault("SetValue"); err != nil {
		return err
	}
	return f.inner.SetValue(key, value)
}

func (f *faultyStorage) GetValue(key []byte) ([]byte, error) {
	if err := f.maybeFault("GetValue"); err != nil {
		return nil, err
	}
	return f.inner.GetValue(key)
}

func (f *faultyStorage) ListKeys(prefix []byte, limit int) ([][]byte, error) {
	if err := f.maybeFault("ListKeys"); err != nil {
		return nil, err
	}
	return f.inner.ListKeys(prefix, limit)
}

func (f *faultyStorage) Close() error {
	return f.inner.Close()
}

var _ datastore.Storage = (*faultyStorage)(nil)

// ============================================================================
// Test cluster with custom storage
// ============================================================================

// newTestClusterWithStorage creates a test cluster where one node uses the given
// faultyStorage and the rest use MemStorage. Returns the cluster and the faulty
// storage for the target node.
func newTestClusterWithStorage(t *testing.T, nodeNames []string, faultyNode string, fs *faultyStorage, underlyingMem *datastore.MemStorage) *testCluster {
	t.Helper()
	tc := &testCluster{
		t:       t,
		nodes:   make(map[string]*core.StateMachine),
		storage: make(map[string]*datastore.MemStorage),
		clients: make(map[string]map[string]*inProcessClient),
		cancels: make(map[string]context.CancelFunc),
		stopped: make(map[string]bool),
	}

	servers := make(map[string]string)
	for _, name := range nodeNames {
		servers[name] = "localhost:10000"
	}

	// Create storage for all nodes. The faulty node's underlying MemStorage is
	// stored so that shared helpers (restartNode, waitForValue, etc.) work.
	for _, name := range nodeNames {
		if name == faultyNode {
			tc.storage[name] = underlyingMem
		} else {
			tc.storage[name] = datastore.NewMemStorage()
		}
	}

	// First pass: create all SMs.
	for _, name := range nodeNames {
		cluster := &core.ClusterConfig{
			Servers:         servers,
			LocalServerName: name,
		}
		clientMap := make(map[string]*core.PeerClient)
		inProcClients := make(map[string]*inProcessClient)
		for _, peer := range nodeNames {
			if peer != name {
				ipc := &inProcessClient{}
				inProcClients[peer] = ipc
				clientMap[peer] = &core.PeerClient{Raft: ipc, KV: ipc}
			}
		}
		tc.clients[name] = inProcClients

		var storage datastore.Storage
		if name == faultyNode {
			storage = fs
		} else {
			storage = tc.storage[name]
		}

		sm, err := core.NewStateMachine(core.StateMachineConfig{
			Storage:     storage,
			Cluster:     cluster,
			Clients:     clientMap,
			MinTimeout:  testMinTimeout,
			TimeoutSpan: testTimeoutSpan,
			Heartbeat:   testHeartbeat,
		})
		if err != nil {
			t.Fatalf("NewStateMachine(%s): %v", name, err)
		}
		tc.nodes[name] = sm
	}

	// Second pass: wire client targets.
	for _, name := range nodeNames {
		for peer, client := range tc.clients[name] {
			client.mu.Lock()
			client.target = tc.nodes[peer]
			client.mu.Unlock()
		}
	}

	return tc
}

// ============================================================================
// Non-fatal storage fault tests
// ============================================================================

func TestStorageFault_AppendEntriesReadError(t *testing.T) {
	mem := datastore.NewMemStorage()
	fs := newFaultyStorage(mem)
	nodeNames := []string{"node1", "node2", "node3"}
	tc := newTestClusterWithStorage(t, nodeNames, "node3", fs, mem)
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}
	t.Logf("Leader: %s", leader)
	if leader == "node3" {
		t.Skip("need node3 as follower for this test")
	}

	// Write some data so the cluster is active.
	if err := tc.appendKV(leader, "key1", "value1"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}

	// Inject GetLogTerm fault on node3. This is called in handleAppendEntries AFTER
	// the election timer is reset (line 386), so the node stays a follower and won't
	// hit the fatal handleElectionTimeout path. The AppendEntries handler returns
	// {Success: false} without crashing.
	fs.SetFault("GetLogTerm", alwaysFail(fmt.Errorf("injected: disk read error")))

	// The leader will keep sending heartbeats to node3. Let it attempt a few.
	time.Sleep(500 * time.Millisecond)

	// Verify node3 is still alive (message loop responding).
	fs.ClearFault("GetLogTerm")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	snapshot, err := tc.nodes["node3"].GetSnapshot(ctx)
	if err != nil {
		t.Fatalf("node3 should still be alive after non-fatal storage error: %v", err)
	}
	t.Logf("node3 still alive, role=%s, term=%d", snapshot.Role, snapshot.CurrentTerm)
}

func TestStorageFault_RequestVoteReadError(t *testing.T) {
	mem := datastore.NewMemStorage()
	fs := newFaultyStorage(mem)
	nodeNames := []string{"node1", "node2", "node3"}
	tc := newTestClusterWithStorage(t, nodeNames, "node3", fs, mem)
	tc.start()
	defer tc.stop()

	// Wait for initial leader election to succeed.
	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}
	t.Logf("Initial leader: %s", leader)
	if leader == "node3" {
		t.Skip("need node3 as follower for this test")
	}

	// Inject GetVotedFor fault on node3. This is called exclusively in handleRequestVote
	// (non-fatal path) and NOT in any fatal path like handleElectionTimeout.
	fs.SetFault("GetVotedFor", alwaysFail(fmt.Errorf("injected: GetVotedFor error")))

	// Force a re-election by partitioning the leader.
	tc.partition(leader)
	time.Sleep(1 * time.Second)

	// Despite node3 being unable to grant votes, node3's own election attempt
	// and the other follower's attempt can still proceed (GetVotedFor fault only
	// affects incoming RequestVote handling, not outgoing vote requests).
	// The cluster may or may not elect a new leader, but node3 should not crash.
	time.Sleep(1 * time.Second)

	// Verify node3 is alive.
	fs.ClearFault("GetVotedFor")
	tc.heal(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = tc.nodes["node3"].GetSnapshot(ctx)
	if err != nil {
		t.Fatalf("node3 should still be alive after non-fatal RequestVote error: %v", err)
	}
}

func TestStorageFault_PutWriteError(t *testing.T) {
	mem := datastore.NewMemStorage()
	fs := newFaultyStorage(mem)
	nodeNames := []string{"node1", "node2", "node3"}
	tc := newTestClusterWithStorage(t, nodeNames, "node1", fs, mem)
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	if leader != "node1" {
		// We need node1 to be leader for this test. Partition the current leader to force re-election.
		tc.partition(leader)
		time.Sleep(1 * time.Second)
		tc.heal(leader)
		// Wait and try again — if node1 still isn't leader, skip.
		time.Sleep(1 * time.Second)
		newLeader, err := tc.waitForLeader(5 * time.Second)
		if err != nil || newLeader != "node1" {
			t.Skip("could not get node1 elected as leader for this test")
		}
		leader = newLeader
	}

	// Inject WriteLog fault on the leader (node1).
	// handlePut → writeToLogs calls WriteLog to append to the leader's log — this is non-fatal.
	fs.SetFault("WriteLog", alwaysFail(fmt.Errorf("injected: write error")))

	kvList := &konsen.KVList{
		KvList: []*konsen.KV{{Key: []byte("fail-key"), Value: []byte("fail-val")}},
	}
	data, err := proto.Marshal(kvList)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := tc.nodes[leader].Put(ctx, &konsen.PutReq{Data: data})
	if err != nil {
		// The error might propagate as a Go error or as resp.Success=false.
		t.Logf("Put returned error (expected): %v", err)
	} else if resp.GetSuccess() {
		t.Fatal("Put should have failed with storage error")
	} else {
		t.Logf("Put failed as expected: %s", resp.GetErrorMessage())
	}

	// Verify node is still alive.
	fs.ClearFault("WriteLog")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	_, err = tc.nodes[leader].GetSnapshot(ctx2)
	if err != nil {
		t.Fatalf("leader should still be alive after non-fatal Put error: %v", err)
	}
}

func TestStorageFault_GetValueError(t *testing.T) {
	mem := datastore.NewMemStorage()
	fs := newFaultyStorage(mem)
	nodeNames := []string{"node1", "node2", "node3"}
	tc := newTestClusterWithStorage(t, nodeNames, "node1", fs, mem)
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	if leader != "node1" {
		t.Skip("need node1 as leader for this test")
	}

	// Write a value first (without fault).
	if err := tc.appendKV(leader, "test-key", "test-val"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}

	// Now inject GetValue fault.
	fs.SetFault("GetValue", alwaysFail(fmt.Errorf("injected: GetValue error")))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = tc.nodes[leader].GetValue(ctx, []byte("test-key"))
	if err == nil {
		t.Fatal("GetValue should have returned an error")
	}
	t.Logf("GetValue failed as expected: %v", err)

	// Clear fault, verify recovery.
	fs.ClearFault("GetValue")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	val, err := tc.nodes[leader].GetValue(ctx2, []byte("test-key"))
	if err != nil {
		t.Fatalf("GetValue should work after clearing fault: %v", err)
	}
	if string(val) != "test-val" {
		t.Errorf("expected test-val, got %q", string(val))
	}
}

func TestStorageFault_TransientRecovery(t *testing.T) {
	mem := datastore.NewMemStorage()
	fs := newFaultyStorage(mem)
	nodeNames := []string{"node1", "node2", "node3"}
	tc := newTestClusterWithStorage(t, nodeNames, "node3", fs, mem)
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}
	if leader == "node3" {
		t.Skip("need node3 as follower for this test")
	}

	// Write some data while node3 is healthy.
	for i := range 3 {
		if err := tc.appendKV(leader, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("appendKV: %v", err)
		}
	}

	// Inject transient fault: GetLogTerm fails with 50% rate.
	// GetLogTerm is called in handleAppendEntries (non-fatal) after the election timer
	// is reset, so the node stays a follower and won't hit fatal paths.
	// Probabilistic faults are more realistic for simulating intermittent I/O errors.
	fs.SetFault("GetLogTerm", failWithRate(0.5, fmt.Errorf("injected: transient error")))
	time.Sleep(1 * time.Second)

	// Clear fault and verify node3 recovers and can process requests.
	fs.ClearAllFaults()
	time.Sleep(1 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	snapshot, err := tc.nodes["node3"].GetSnapshot(ctx)
	if err != nil {
		t.Fatalf("node3 should recover after transient faults: %v", err)
	}
	t.Logf("node3 recovered: role=%s, term=%d, commitIndex=%d", snapshot.Role, snapshot.CurrentTerm, snapshot.CommitIndex)
}

// ============================================================================
// Fatal storage fault tests
// ============================================================================

// fatalExitMu serializes FATAL tests since they modify the global logrus ExitFunc.
var fatalExitMu sync.Mutex

func TestStorageFault_ApplyLogFatal(t *testing.T) {
	fatalExitMu.Lock()
	defer fatalExitMu.Unlock()

	var exitCalled atomic.Int32
	logrus.StandardLogger().ExitFunc = func(code int) {
		exitCalled.Store(int32(code))
		runtime.Goexit()
	}
	defer func() { logrus.StandardLogger().ExitFunc = nil }()

	mem := datastore.NewMemStorage()
	fs := newFaultyStorage(mem)
	nodeNames := []string{"node1", "node2", "node3"}
	tc := newTestClusterWithStorage(t, nodeNames, "node1", fs, mem)
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	if leader != "node1" {
		t.Skip("need node1 as leader for this test")
	}

	// Write data to get a committed log entry that will be applied.
	// First, make sure data can be written and committed normally.
	if err := tc.appendKV(leader, "before-fault", "ok"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}

	// Inject SetValue fault — this is called in maybeApplyLogs → applyFn.
	// The next committed log entry that gets applied will trigger log.Fatalf.
	fs.SetFault("SetValue", alwaysFail(fmt.Errorf("injected: disk full")))

	// Write another entry. It will be replicated to the majority (node2, node3 apply fine),
	// but when node1 tries to apply it locally, SetValue fails → Fatalf.
	kvList := &konsen.KVList{
		KvList: []*konsen.KV{{Key: []byte("fatal-key"), Value: []byte("fatal-val")}},
	}
	data, _ := proto.Marshal(kvList)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// This may or may not return — the message loop may crash before responding.
	tc.nodes[leader].Put(ctx, &konsen.PutReq{Data: data})

	// Wait for the fatal exit to be triggered.
	deadline := time.After(5 * time.Second)
	for {
		if exitCalled.Load() != 0 {
			t.Logf("log.Fatalf triggered as expected (exit code %d)", exitCalled.Load())
			return
		}
		select {
		case <-deadline:
			t.Fatal("expected log.Fatalf to be called within timeout")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestStorageFault_ElectionTimeoutFatal(t *testing.T) {
	fatalExitMu.Lock()
	defer fatalExitMu.Unlock()

	var exitCalled atomic.Int32
	logrus.StandardLogger().ExitFunc = func(code int) {
		exitCalled.Store(int32(code))
		runtime.Goexit()
	}
	defer func() { logrus.StandardLogger().ExitFunc = nil }()

	mem := datastore.NewMemStorage()
	fs := newFaultyStorage(mem)
	nodeNames := []string{"node1", "node2", "node3"}
	tc := newTestClusterWithStorage(t, nodeNames, "node1", fs, mem)

	// Inject SetTermAndVotedFor fault BEFORE starting.
	// When node1's election timeout fires and it tries to become a candidate,
	// handleElectionTimeout calls SetTermAndVotedFor → error → log.Fatalf.
	fs.SetFault("SetTermAndVotedFor", alwaysFail(fmt.Errorf("injected: atomic write failed")))

	tc.start()
	defer tc.stop()

	// Wait for node1's election timeout to fire (within ~300ms with test timeouts).
	deadline := time.After(5 * time.Second)
	for {
		if exitCalled.Load() != 0 {
			t.Logf("log.Fatalf triggered as expected (exit code %d)", exitCalled.Load())
			return
		}
		select {
		case <-deadline:
			t.Fatal("expected log.Fatalf to be called when election timeout fires with SetTermAndVotedFor fault")
		case <-time.After(50 * time.Millisecond):
		}
	}
}
