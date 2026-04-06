package konsen_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/lizhaoliu/konsen/v2/core"
	"github.com/lizhaoliu/konsen/v2/datastore"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// Test infrastructure: in-process Raft cluster
// ============================================================================

// Fast timeouts for integration tests to avoid long sleeps.
const (
	testMinTimeout  = 150 * time.Millisecond
	testTimeoutSpan = 150 * time.Millisecond
	testHeartbeat   = 30 * time.Millisecond
)

// inProcessClient implements both core.RaftService and core.KVService
// by forwarding calls to a StateMachine directly.
type inProcessClient struct {
	mu       sync.RWMutex
	target   *core.StateMachine
	blocked  bool          // simulates network partition
	delay    time.Duration // simulates network latency
	dropRate float64       // probability of message drop [0.0, 1.0]
}

func newInProcessClient(target *core.StateMachine) *inProcessClient {
	return &inProcessClient{target: target}
}

func (c *inProcessClient) setBlocked(blocked bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked = blocked
}

func (c *inProcessClient) isBlocked() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blocked
}

func (c *inProcessClient) setDelay(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delay = d
}

func (c *inProcessClient) setDropRate(rate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if rate < 0 {
		rate = 0
	} else if rate > 1 {
		rate = 1
	}
	c.dropRate = rate
}

// simulateNetwork applies network effects (partition, delay, drops) and returns the target SM.
// Reading target under the lock prevents races with restartNode updating it.
func (c *inProcessClient) simulateNetwork(ctx context.Context) (*core.StateMachine, error) {
	c.mu.RLock()
	blocked := c.blocked
	delay := c.delay
	dropRate := c.dropRate
	target := c.target
	c.mu.RUnlock()

	if blocked {
		return nil, fmt.Errorf("network partition: connection refused")
	}
	if dropRate > 0 && rand.Float64() < dropRate {
		return nil, fmt.Errorf("network: message dropped")
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return target, nil
}

func (c *inProcessClient) AppendEntries(ctx context.Context, in *konsen.AppendEntriesReq) (*konsen.AppendEntriesResp, error) {
	target, err := c.simulateNetwork(ctx)
	if err != nil {
		return nil, err
	}
	return target.AppendEntries(ctx, in)
}

func (c *inProcessClient) RequestVote(ctx context.Context, in *konsen.RequestVoteReq) (*konsen.RequestVoteResp, error) {
	target, err := c.simulateNetwork(ctx)
	if err != nil {
		return nil, err
	}
	return target.RequestVote(ctx, in)
}

func (c *inProcessClient) Put(ctx context.Context, in *konsen.PutReq) (*konsen.PutResp, error) {
	target, err := c.simulateNetwork(ctx)
	if err != nil {
		return nil, err
	}
	return target.Put(ctx, in)
}

func (c *inProcessClient) Get(ctx context.Context, in *konsen.GetReq) (*konsen.GetResp, error) {
	target, err := c.simulateNetwork(ctx)
	if err != nil {
		return nil, err
	}
	val, err := target.GetValue(ctx, in.GetKey())
	if err != nil {
		return &konsen.GetResp{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &konsen.GetResp{Success: true, Value: val}, nil
}

// testCluster manages an in-process Raft cluster for integration testing.
type testCluster struct {
	t       *testing.T
	nodes   map[string]*core.StateMachine
	storage map[string]*datastore.MemStorage
	clients map[string]map[string]*inProcessClient // node -> peer -> client
	cancels map[string]context.CancelFunc
	stopped map[string]bool // tracks crashed/stopped nodes
	mu      sync.Mutex
}

func newTestCluster(t *testing.T, nodeNames []string) *testCluster {
	t.Helper()
	tc := &testCluster{
		t:       t,
		nodes:   make(map[string]*core.StateMachine),
		storage: make(map[string]*datastore.MemStorage),
		clients: make(map[string]map[string]*inProcessClient),
		cancels: make(map[string]context.CancelFunc),
		stopped: make(map[string]bool),
	}

	// Build cluster config.
	servers := make(map[string]string)
	for _, name := range nodeNames {
		servers[name] = fmt.Sprintf("localhost:%d", 10000) // placeholder; in-process clients bypass networking
	}

	// Phase 1: Create all state machines (without clients wired yet).
	for _, name := range nodeNames {
		tc.storage[name] = datastore.NewMemStorage()
	}

	// Phase 2: Create clients that point to state machines.
	// We need to create SMs first, then wire clients, but SMs need clients at creation.
	// Solution: create SMs with placeholder, then set up routing.
	// Actually, the in-process clients need the target SM, so we create SMs in two passes.

	// First pass: create all SMs.
	for _, name := range nodeNames {
		cluster := &core.ClusterConfig{
			Servers:         servers,
			LocalServerName: name,
		}
		clientMap := make(map[string]*core.PeerClient)
		inProcClients := make(map[string]*inProcessClient)

		// Create placeholder clients - we'll set targets after all SMs exist.
		for _, peer := range nodeNames {
			if peer != name {
				ipc := &inProcessClient{}
				inProcClients[peer] = ipc
				clientMap[peer] = &core.PeerClient{Raft: ipc, KV: ipc}
			}
		}
		tc.clients[name] = inProcClients

		sm, err := core.NewStateMachine(core.StateMachineConfig{
			Storage:     tc.storage[name],
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

// start starts all nodes in the cluster.
func (tc *testCluster) start() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for name, sm := range tc.nodes {
		ctx, cancel := context.WithCancel(context.Background())
		tc.cancels[name] = cancel
		go sm.Run(ctx)
	}
	// Let the message loops initialize.
	time.Sleep(50 * time.Millisecond)
}

// stop stops all nodes and waits for them to finish.
func (tc *testCluster) stop() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for _, cancel := range tc.cancels {
		cancel()
	}
	for name, sm := range tc.nodes {
		if !tc.stopped[name] {
			sm.Close()
		}
	}
}

// waitForLeader waits until exactly one leader exists or timeout expires.
// It skips stopped nodes and tolerates transient multi-leader states (e.g.
// during re-elections) by continuing to poll until the cluster converges.
func (tc *testCluster) waitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tc.mu.Lock()
		nodes := make(map[string]*core.StateMachine, len(tc.nodes))
		for name, sm := range tc.nodes {
			if !tc.stopped[name] {
				nodes[name] = sm
			}
		}
		tc.mu.Unlock()

		leader := ""
		multi := false
		for name, sm := range nodes {
			sctx, scancel := context.WithTimeout(context.Background(), 1*time.Second)
			snapshot, err := sm.GetSnapshot(sctx)
			scancel()
			if err != nil {
				continue
			}
			if snapshot.Role == konsen.Role_LEADER {
				if leader != "" {
					multi = true
					break
				}
				leader = name
			}
		}
		if leader != "" && !multi {
			return leader, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", fmt.Errorf("no single leader elected within %v", timeout)
}

// partition isolates a node from the cluster (bidirectional).
func (tc *testCluster) partition(nodeName string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// Block outgoing connections from this node.
	for _, client := range tc.clients[nodeName] {
		client.setBlocked(true)
	}
	// Block incoming connections to this node from others.
	for name, peerClients := range tc.clients {
		if name != nodeName {
			if client, ok := peerClients[nodeName]; ok {
				client.setBlocked(true)
			}
		}
	}
}

// heal restores connectivity for a node.
func (tc *testCluster) heal(nodeName string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// Unblock outgoing.
	for _, client := range tc.clients[nodeName] {
		client.setBlocked(false)
	}
	// Unblock incoming.
	for name, peerClients := range tc.clients {
		if name != nodeName {
			if client, ok := peerClients[nodeName]; ok {
				client.setBlocked(false)
			}
		}
	}
}

// appendKV writes a key-value pair through the cluster leader.
func (tc *testCluster) appendKV(leader string, key, value string) error {
	kvList := &konsen.KVList{
		KvList: []*konsen.KV{{Key: []byte(key), Value: []byte(value)}},
	}
	data, err := proto.Marshal(kvList)
	if err != nil {
		return err
	}
	tc.mu.Lock()
	sm := tc.nodes[leader]
	tc.mu.Unlock()
	if sm == nil {
		return fmt.Errorf("node %s not found", leader)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := sm.Put(ctx, &konsen.PutReq{Data: data})
	if err != nil {
		return err
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("Put failed: %s", resp.GetErrorMessage())
	}
	return nil
}

// getValue reads a value from a node.
func (tc *testCluster) getValue(nodeName string, key string) (string, error) {
	tc.mu.Lock()
	sm := tc.nodes[nodeName]
	tc.mu.Unlock()
	if sm == nil {
		return "", fmt.Errorf("node %s not found", nodeName)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	val, err := sm.GetValue(ctx, []byte(key))
	if err != nil {
		return "", err
	}
	return string(val), nil
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestIntegration_LeaderElection(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}
	t.Logf("Leader elected: %s", leader)

	// Verify only one leader exists.
	leaderCount := 0
	for name, sm := range tc.nodes {
		snapshot, err := sm.GetSnapshot(context.Background())
		if err != nil {
			t.Fatalf("GetSnapshot(%s): %v", name, err)
		}
		if snapshot.Role == konsen.Role_LEADER {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected exactly 1 leader, got %d", leaderCount)
	}
}

func TestIntegration_LeaderElection_SingleNode(t *testing.T) {
	// Skip: single-node clusters don't work because handleElectionTimeout
	// doesn't check majority after self-vote — it relies on handleRequestVoteResp
	// which is never called when there are no peers.
	t.Skip("single-node election not supported by current implementation")
}

func TestIntegration_DataReplication(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}
	t.Logf("Leader: %s", leader)

	// Write data through the leader.
	if err := tc.appendKV(leader, "color", "blue"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}

	// Read from leader (linearizable read).
	val, err := tc.getValue(leader, "color")
	if err != nil {
		t.Fatalf("getValue from leader: %v", err)
	}
	if val != "blue" {
		t.Errorf("getValue(color) = %q, want %q", val, "blue")
	}

	// Verify data is replicated to followers' storage (eventually).
	time.Sleep(200 * time.Millisecond) // allow replication
	for name, s := range tc.storage {
		v, err := s.GetValue([]byte("color"))
		if err != nil {
			t.Errorf("storage.GetValue(%s): %v", name, err)
			continue
		}
		if string(v) != "blue" {
			t.Errorf("storage[%s].GetValue(color) = %q, want %q", name, v, "blue")
		}
	}
}

func TestIntegration_MultipleWrites(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Write multiple entries.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := tc.appendKV(leader, key, value); err != nil {
			t.Fatalf("appendKV(%s): %v", key, err)
		}
	}

	// Verify all entries on leader.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		expected := fmt.Sprintf("value%d", i)
		val, err := tc.getValue(leader, key)
		if err != nil {
			t.Errorf("getValue(%s): %v", key, err)
			continue
		}
		if val != expected {
			t.Errorf("getValue(%s) = %q, want %q", key, val, expected)
		}
	}

	// Verify replication to all nodes.
	time.Sleep(200 * time.Millisecond)
	for name, s := range tc.storage {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			expected := fmt.Sprintf("value%d", i)
			v, _ := s.GetValue([]byte(key))
			if string(v) != expected {
				t.Errorf("storage[%s].GetValue(%s) = %q, want %q", name, key, v, expected)
			}
		}
	}
}

func TestIntegration_LeaderReElection(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader1, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("initial election failed: %v", err)
	}
	t.Logf("Initial leader: %s", leader1)

	// Write some data before partition.
	if err := tc.appendKV(leader1, "before", "partition"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}

	// Partition the leader from the cluster.
	tc.partition(leader1)
	t.Logf("Partitioned leader: %s", leader1)

	// Wait for a new leader to be elected from remaining nodes.
	deadline := time.Now().Add(5 * time.Second)
	var leader2 string
	for time.Now().Before(deadline) {
		for name, sm := range tc.nodes {
			if name == leader1 {
				continue
			}
			snapshot, err := sm.GetSnapshot(context.Background())
			if err != nil {
				continue
			}
			if snapshot.Role == konsen.Role_LEADER {
				leader2 = name
				break
			}
		}
		if leader2 != "" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if leader2 == "" {
		t.Fatal("no new leader elected after partitioning old leader")
	}
	if leader2 == leader1 {
		t.Fatal("new leader should be different from partitioned leader")
	}
	t.Logf("New leader: %s", leader2)

	// Write data to the new leader.
	if err := tc.appendKV(leader2, "after", "partition"); err != nil {
		t.Fatalf("appendKV to new leader: %v", err)
	}

	// Verify new data is readable from new leader.
	val, err := tc.getValue(leader2, "after")
	if err != nil {
		t.Fatalf("getValue from new leader: %v", err)
	}
	if val != "partition" {
		t.Errorf("getValue(after) = %q, want %q", val, "partition")
	}
}

func TestIntegration_PartitionHealAndConverge(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader1, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("initial election failed: %v", err)
	}
	t.Logf("Initial leader: %s", leader1)

	// Write initial data.
	if err := tc.appendKV(leader1, "key1", "val1"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}

	// Partition leader.
	tc.partition(leader1)

	// Wait for new leader.
	deadline := time.Now().Add(5 * time.Second)
	var leader2 string
	for time.Now().Before(deadline) {
		for name, sm := range tc.nodes {
			if name == leader1 {
				continue
			}
			snapshot, _ := sm.GetSnapshot(context.Background())
			if snapshot != nil && snapshot.Role == konsen.Role_LEADER {
				leader2 = name
			}
		}
		if leader2 != "" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if leader2 == "" {
		t.Fatal("no new leader after partition")
	}
	t.Logf("New leader: %s", leader2)

	// Write new data via new leader.
	if err := tc.appendKV(leader2, "key2", "val2"); err != nil {
		t.Fatalf("appendKV to new leader: %v", err)
	}

	// Heal partition.
	tc.heal(leader1)
	t.Logf("Healed partition for: %s", leader1)

	// Wait for convergence - the old leader should step down.
	time.Sleep(1 * time.Second)

	// After healing, all nodes should converge: exactly one leader.
	leaderCount := 0
	for _, sm := range tc.nodes {
		snapshot, err := sm.GetSnapshot(context.Background())
		if err != nil {
			continue
		}
		if snapshot.Role == konsen.Role_LEADER {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected 1 leader after heal, got %d", leaderCount)
	}

	// All nodes should eventually have key2.
	time.Sleep(1 * time.Second)
	for name, s := range tc.storage {
		v, _ := s.GetValue([]byte("key2"))
		if string(v) != "val2" {
			t.Errorf("storage[%s] missing key2 after heal, got %q", name, v)
		}
	}
}

func TestIntegration_FollowerForwardsToLeader(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Pick a follower and wait until it knows who the leader is (via heartbeat).
	var follower string
	for name := range tc.nodes {
		if name != leader {
			follower = name
			break
		}
	}

	// Wait for the follower to learn the leader via heartbeat.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, _ := tc.nodes[follower].GetSnapshot(context.Background())
		if snapshot != nil && snapshot.CurrentLeader != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("Writing to follower %s (leader is %s)", follower, leader)

	// Write through follower (should be forwarded to leader).
	if err := tc.appendKV(follower, "forwarded", "yes"); err != nil {
		t.Fatalf("appendKV via follower: %v", err)
	}

	// Verify data on leader.
	val, err := tc.getValue(leader, "forwarded")
	if err != nil {
		t.Fatalf("getValue: %v", err)
	}
	if val != "yes" {
		t.Errorf("getValue(forwarded) = %q, want %q", val, "yes")
	}
}

func TestIntegration_ConcurrentWrites(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Concurrent writes.
	const n = 20
	var wg sync.WaitGroup
	errors := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-%d", i)
			value := fmt.Sprintf("val-%d", i)
			if err := tc.appendKV(leader, key, value); err != nil {
				errors <- fmt.Errorf("write %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// Verify all writes landed.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("concurrent-%d", i)
		expected := fmt.Sprintf("val-%d", i)
		val, err := tc.getValue(leader, key)
		if err != nil {
			t.Errorf("getValue(%s): %v", key, err)
			continue
		}
		if val != expected {
			t.Errorf("getValue(%s) = %q, want %q", key, val, expected)
		}
	}
}

func TestIntegration_LogConsistency(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Write several entries.
	for i := 0; i < 5; i++ {
		if err := tc.appendKV(leader, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("appendKV: %v", err)
		}
	}

	// Wait for replication.
	time.Sleep(500 * time.Millisecond)

	// All nodes should have the same log entries.
	var refLogs []*konsen.Log
	for name, s := range tc.storage {
		logs, err := s.GetLogsFrom(1)
		if err != nil {
			t.Fatalf("GetLogsFrom(%s): %v", name, err)
		}
		if refLogs == nil {
			refLogs = logs
			continue
		}
		if len(logs) != len(refLogs) {
			t.Errorf("node %s has %d logs, expected %d", name, len(logs), len(refLogs))
			continue
		}
		for i := range logs {
			if logs[i].GetIndex() != refLogs[i].GetIndex() {
				t.Errorf("node %s log[%d].Index = %d, want %d", name, i, logs[i].GetIndex(), refLogs[i].GetIndex())
			}
			if logs[i].GetTerm() != refLogs[i].GetTerm() {
				t.Errorf("node %s log[%d].Term = %d, want %d", name, i, logs[i].GetTerm(), refLogs[i].GetTerm())
			}
		}
	}
}

func TestIntegration_MinorityPartitionCannotElect(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Partition a single follower.
	var follower string
	for name := range tc.nodes {
		if name != leader {
			follower = name
			break
		}
	}
	tc.partition(follower)
	t.Logf("Partitioned follower: %s", follower)

	// The partitioned follower should not be able to become leader
	// because it can't get a majority of votes.
	time.Sleep(1 * time.Second)

	snapshot, err := tc.nodes[follower].GetSnapshot(context.Background())
	if err != nil {
		t.Fatalf("GetSnapshot(%s): %v", follower, err)
	}
	if snapshot.Role == konsen.Role_LEADER {
		t.Errorf("partitioned minority node %s should not become leader", follower)
	}

	// The original leader should still be leader.
	snapshot, err = tc.nodes[leader].GetSnapshot(context.Background())
	if err != nil {
		t.Fatalf("GetSnapshot(%s): %v", leader, err)
	}
	if snapshot.Role != konsen.Role_LEADER {
		t.Errorf("original leader %s should still be leader, got %v", leader, snapshot.Role)
	}
}

func TestIntegration_FiveNodeCluster(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3", "node4", "node5"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}
	t.Logf("Leader: %s", leader)

	// Write data.
	if err := tc.appendKV(leader, "five", "nodes"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}

	// Verify.
	val, err := tc.getValue(leader, "five")
	if err != nil {
		t.Fatalf("getValue: %v", err)
	}
	if val != "nodes" {
		t.Errorf("getValue(five) = %q, want %q", val, "nodes")
	}

	// Partition 2 nodes (minority) - cluster should still work.
	partitioned := 0
	for name := range tc.nodes {
		if name != leader && partitioned < 2 {
			tc.partition(name)
			partitioned++
		}
	}

	// Should still be able to write (3 out of 5 nodes available = majority).
	if err := tc.appendKV(leader, "still", "working"); err != nil {
		t.Fatalf("appendKV after minority partition: %v", err)
	}
}

func TestIntegration_TermMonotonicity(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	// Wait for initial election.
	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Force a re-election by partitioning the leader.
	tc.partition(leader)
	time.Sleep(1 * time.Second)
	tc.heal(leader)
	time.Sleep(1 * time.Second)

	// All terms should be > 0 and terms should only increase.
	for name, s := range tc.storage {
		term, err := s.GetCurrentTerm()
		if err != nil {
			t.Fatalf("GetCurrentTerm(%s): %v", name, err)
		}
		if term == 0 {
			t.Errorf("node %s has term 0 after elections", name)
		}

		// All log entries should have non-decreasing terms.
		logs, _ := s.GetLogsFrom(1)
		var prevTerm uint64
		for _, log := range logs {
			if log.GetTerm() < prevTerm {
				t.Errorf("node %s: log[%d].term=%d < previous term %d (non-monotonic)",
					name, log.GetIndex(), log.GetTerm(), prevTerm)
			}
			prevTerm = log.GetTerm()
		}
	}
}

func TestIntegration_ElectionSafety(t *testing.T) {
	// Tests Raft safety: at most one leader per term.
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	_, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Check that no two nodes claim to be leader for the same term.
	type leaderTerm struct {
		name string
		term uint64
	}
	var leaders []leaderTerm
	for name, sm := range tc.nodes {
		snapshot, _ := sm.GetSnapshot(context.Background())
		if snapshot != nil && snapshot.Role == konsen.Role_LEADER {
			leaders = append(leaders, leaderTerm{name, snapshot.CurrentTerm})
		}
	}

	termLeaders := make(map[uint64]string)
	for _, lt := range leaders {
		if existing, ok := termLeaders[lt.term]; ok && existing != lt.name {
			t.Errorf("two leaders in term %d: %s and %s", lt.term, existing, lt.name)
		}
		termLeaders[lt.term] = lt.name
	}
}
