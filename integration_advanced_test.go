package konsen_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lizhaoliu/konsen/v2/core"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
)

// ============================================================================
// Additional test infrastructure for production-grade scenarios
// ============================================================================

// activeNodes returns a snapshot of non-stopped nodes, safe for concurrent use.
func (tc *testCluster) activeNodes() map[string]*core.StateMachine {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	result := make(map[string]*core.StateMachine, len(tc.nodes))
	for name, sm := range tc.nodes {
		if !tc.stopped[name] {
			result[name] = sm
		}
	}
	return result
}

// isStopped checks whether a node has been stopped (thread-safe).
func (tc *testCluster) isStopped(name string) bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.stopped[name]
}

// waitFor polls a condition until it returns true or timeout expires.
func (tc *testCluster) waitFor(timeout time.Duration, desc string, condition func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("%s: condition not met within %v", desc, timeout)
}

// waitForValue waits until a key has the expected value in a node's storage.
// Reads directly from storage so it works on both leaders and followers.
func (tc *testCluster) waitForValue(nodeName, key, expected string, timeout time.Duration) error {
	return tc.waitFor(timeout, fmt.Sprintf("waitForValue(%s,%s=%s)", nodeName, key, expected), func() bool {
		v, err := tc.storage[nodeName].GetValue([]byte(key))
		return err == nil && string(v) == expected
	})
}

// waitForAllValues waits until all non-stopped nodes' storage has the expected value.
func (tc *testCluster) waitForAllValues(key, expected string, timeout time.Duration) error {
	return tc.waitFor(timeout, fmt.Sprintf("waitForAllValues(%s=%s)", key, expected), func() bool {
		for name, s := range tc.storage {
			if tc.isStopped(name) {
				continue
			}
			v, err := s.GetValue([]byte(key))
			if err != nil || string(v) != expected {
				return false
			}
		}
		return true
	})
}

// waitForNewLeader waits for exactly one leader (excluding the given node) to exist.
func (tc *testCluster) waitForNewLeader(timeout time.Duration, exclude string) (string, error) {
	var leader string
	err := tc.waitFor(timeout, "waitForNewLeader", func() bool {
		active := tc.activeNodes()
		found := ""
		for name, sm := range active {
			if name == exclude {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			snapshot, err := sm.GetSnapshot(ctx)
			cancel()
			if err != nil {
				continue
			}
			if snapshot.Role == konsen.Role_LEADER {
				if found != "" {
					return false // transient multi-leader, keep polling
				}
				found = name
			}
		}
		if found != "" {
			leader = found
			return true
		}
		return false
	})
	return leader, err
}

// countLeaders counts nodes that believe they are leader (thread-safe).
func (tc *testCluster) countLeaders() int {
	active := tc.activeNodes()
	count := 0
	for _, sm := range active {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		snapshot, err := sm.GetSnapshot(ctx)
		cancel()
		if err != nil {
			continue
		}
		if snapshot.Role == konsen.Role_LEADER {
			count++
		}
	}
	return count
}

// getLeader returns the name of the current leader (thread-safe).
func (tc *testCluster) getLeader() (string, error) {
	active := tc.activeNodes()
	for name, sm := range active {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		snapshot, err := sm.GetSnapshot(ctx)
		cancel()
		if err != nil {
			continue
		}
		if snapshot.Role == konsen.Role_LEADER {
			return name, nil
		}
	}
	return "", fmt.Errorf("no leader found")
}

// followers returns the names of non-leader, non-stopped nodes.
func (tc *testCluster) followers(leader string) []string {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	var result []string
	for name := range tc.nodes {
		if name != leader && !tc.stopped[name] {
			result = append(result, name)
		}
	}
	sort.Strings(result)
	return result
}

// stopNode simulates a node crash. Safe to call on an already-stopped node (no-op).
func (tc *testCluster) stopNode(name string) {
	tc.mu.Lock()
	if tc.stopped[name] {
		tc.mu.Unlock()
		return
	}
	cancel := tc.cancels[name]
	sm := tc.nodes[name]
	tc.stopped[name] = true
	tc.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	sm.Close()
}

// restartNode restarts a stopped node, preserving its storage (simulates persistence).
func (tc *testCluster) restartNode(name string) error {
	// Phase 1: read shared state under lock.
	tc.mu.Lock()
	if !tc.stopped[name] {
		tc.mu.Unlock()
		return fmt.Errorf("node %s is not stopped", name)
	}

	servers := make(map[string]string)
	for n := range tc.nodes {
		servers[n] = "localhost:10000"
	}

	type peerInfo struct {
		target   *core.StateMachine
		blocked  bool
		delay    time.Duration
		dropRate float64
	}
	peers := make(map[string]peerInfo)
	oldClients := tc.clients[name]
	for peer := range tc.nodes {
		if peer != name {
			pi := peerInfo{target: tc.nodes[peer]}
			if old, ok := oldClients[peer]; ok {
				old.mu.RLock()
				pi.blocked = old.blocked
				pi.delay = old.delay
				pi.dropRate = old.dropRate
				old.mu.RUnlock()
			}
			peers[peer] = pi
		}
	}
	storage := tc.storage[name]
	tc.mu.Unlock()

	// Phase 2: create new SM and clients without holding the lock.
	clientMap := make(map[string]*core.PeerClient)
	inProcClients := make(map[string]*inProcessClient)
	for peer, pi := range peers {
		ipc := &inProcessClient{target: pi.target}
		// Preserve network fault state from the old client so that
		// active partitions/delays/drops survive a node restart.
		ipc.blocked = pi.blocked
		ipc.delay = pi.delay
		ipc.dropRate = pi.dropRate
		inProcClients[peer] = ipc
		clientMap[peer] = &core.PeerClient{Raft: ipc, KV: ipc}
	}

	sm, err := core.NewStateMachine(core.StateMachineConfig{
		Storage:     storage,
		Cluster:     &core.ClusterConfig{Servers: servers, LocalServerName: name},
		Clients:     clientMap,
		MinTimeout:  testMinTimeout,
		TimeoutSpan: testTimeoutSpan,
		Heartbeat:   testHeartbeat,
	})
	if err != nil {
		return err
	}

	// Phase 3: install new SM and update shared state under lock.
	tc.mu.Lock()
	tc.nodes[name] = sm
	tc.clients[name] = inProcClients
	tc.stopped[name] = false

	// Update other nodes' clients to point to the new SM.
	for otherName, peerClients := range tc.clients {
		if otherName != name {
			if client, ok := peerClients[name]; ok {
				client.mu.Lock()
				client.target = sm
				client.mu.Unlock()
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	tc.cancels[name] = cancel
	tc.mu.Unlock()

	go sm.Run(ctx)

	return nil
}

// partitionAsymmetric blocks messages from `from` to `to` only (one-way).
func (tc *testCluster) partitionAsymmetric(from, to string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if clients, ok := tc.clients[from]; ok {
		if client, ok := clients[to]; ok {
			client.setBlocked(true)
		}
	}
}

// healAsymmetric restores one-way connectivity from `from` to `to`.
func (tc *testCluster) healAsymmetric(from, to string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if clients, ok := tc.clients[from]; ok {
		if client, ok := clients[to]; ok {
			client.setBlocked(false)
		}
	}
}

// partitionGroup isolates two groups of nodes from each other.
func (tc *testCluster) partitionGroup(group1, group2 []string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for _, a := range group1 {
		for _, b := range group2 {
			if clients, ok := tc.clients[a]; ok {
				if client, ok := clients[b]; ok {
					client.setBlocked(true)
				}
			}
			if clients, ok := tc.clients[b]; ok {
				if client, ok := clients[a]; ok {
					client.setBlocked(true)
				}
			}
		}
	}
}

// healAll restores all network connections and removes delays/drops.
func (tc *testCluster) healAll() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for _, peerClients := range tc.clients {
		for _, client := range peerClients {
			client.setBlocked(false)
			client.setDelay(0)
			client.setDropRate(0)
		}
	}
}

// setNodeDelay adds latency to all connections involving a node.
func (tc *testCluster) setNodeDelay(name string, delay time.Duration) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for otherName, peerClients := range tc.clients {
		if otherName != name {
			if client, ok := peerClients[name]; ok {
				client.setDelay(delay)
			}
		}
	}
	for _, client := range tc.clients[name] {
		client.setDelay(delay)
	}
}

// setNodeDropRate sets probabilistic message dropping for a node's connections.
func (tc *testCluster) setNodeDropRate(name string, rate float64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for otherName, peerClients := range tc.clients {
		if otherName != name {
			if client, ok := peerClients[name]; ok {
				client.setDropRate(rate)
			}
		}
	}
	for _, client := range tc.clients[name] {
		client.setDropRate(rate)
	}
}

// ============================================================================
// Crash Recovery Tests
// ============================================================================

func TestIntegration_FollowerCrashRecovery(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Write initial data.
	if err := tc.appendKV(leader, "before-crash", "v1"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}
	if err := tc.waitForAllValues("before-crash", "v1", 3*time.Second); err != nil {
		t.Fatalf("replication failed: %v", err)
	}

	// Crash a follower.
	follower := tc.followers(leader)[0]
	t.Logf("Crashing follower: %s", follower)
	tc.stopNode(follower)

	// Write more data while follower is down.
	if err := tc.appendKV(leader, "during-crash", "v2"); err != nil {
		t.Fatalf("appendKV during crash: %v", err)
	}

	// Restart the follower.
	t.Logf("Restarting follower: %s", follower)
	if err := tc.restartNode(follower); err != nil {
		t.Fatalf("restartNode: %v", err)
	}

	// Follower should catch up with all data.
	if err := tc.waitForValue(follower, "before-crash", "v1", 5*time.Second); err != nil {
		t.Errorf("follower missing pre-crash data: %v", err)
	}
	if err := tc.waitForValue(follower, "during-crash", "v2", 5*time.Second); err != nil {
		t.Errorf("follower did not catch up: %v", err)
	}
}

func TestIntegration_LeaderCrashRecovery(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader1, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Write data via first leader.
	if err := tc.appendKV(leader1, "pre-crash", "data1"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}
	if err := tc.waitForAllValues("pre-crash", "data1", 3*time.Second); err != nil {
		t.Fatalf("replication failed: %v", err)
	}

	// Crash the leader.
	t.Logf("Crashing leader: %s", leader1)
	tc.stopNode(leader1)

	// Wait for new leader.
	leader2, err := tc.waitForNewLeader(5*time.Second, leader1)
	if err != nil {
		t.Fatalf("no new leader after crash: %v", err)
	}
	t.Logf("New leader: %s", leader2)

	// Write more data via new leader.
	if err := tc.appendKV(leader2, "post-crash", "data2"); err != nil {
		t.Fatalf("appendKV to new leader: %v", err)
	}

	// Restart the old leader.
	t.Logf("Restarting old leader: %s", leader1)
	if err := tc.restartNode(leader1); err != nil {
		t.Fatalf("restartNode: %v", err)
	}

	// Old leader should catch up with all data.
	if err := tc.waitForValue(leader1, "pre-crash", "data1", 5*time.Second); err != nil {
		t.Errorf("restarted leader missing pre-crash data: %v", err)
	}
	if err := tc.waitForValue(leader1, "post-crash", "data2", 5*time.Second); err != nil {
		t.Errorf("restarted leader did not catch up: %v", err)
	}

	// Cluster should have exactly one leader.
	if err := tc.waitFor(3*time.Second, "single leader", func() bool {
		return tc.countLeaders() == 1
	}); err != nil {
		t.Errorf("expected 1 leader, got %d", tc.countLeaders())
	}
}

func TestIntegration_FullClusterRestart(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Write data.
	for i := range 5 {
		if err := tc.appendKV(leader, fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)); err != nil {
			t.Fatalf("appendKV(key%d): %v", i, err)
		}
	}
	if err := tc.waitForAllValues("key4", "val4", 3*time.Second); err != nil {
		t.Fatalf("replication failed: %v", err)
	}

	// Stop all nodes (simulates full cluster outage).
	t.Log("Stopping all nodes")
	for _, name := range []string{"node1", "node2", "node3"} {
		tc.stopNode(name)
	}

	// Restart all nodes.
	t.Log("Restarting all nodes")
	for _, name := range []string{"node1", "node2", "node3"} {
		if err := tc.restartNode(name); err != nil {
			t.Fatalf("restartNode(%s): %v", name, err)
		}
	}

	// Wait for a new leader.
	newLeader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no leader after full restart: %v", err)
	}
	t.Logf("New leader: %s", newLeader)

	// All pre-restart data should be preserved.
	for i := range 5 {
		key := fmt.Sprintf("key%d", i)
		expected := fmt.Sprintf("val%d", i)
		if err := tc.waitForValue(newLeader, key, expected, 5*time.Second); err != nil {
			t.Errorf("data lost after full restart: %v", err)
		}
	}

	// Should be able to write new data.
	if err := tc.appendKV(newLeader, "post-restart", "works"); err != nil {
		t.Fatalf("appendKV after restart: %v", err)
	}
	val, err := tc.getValue(newLeader, "post-restart")
	if err != nil {
		t.Fatalf("getValue after restart: %v", err)
	}
	if val != "works" {
		t.Errorf("getValue(post-restart) = %q, want %q", val, "works")
	}
}

// ============================================================================
// Advanced Partition Tests
// ============================================================================

func TestIntegration_SplitBrain_FiveNodes(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3", "node4", "node5"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	if err := tc.appendKV(leader, "pre-split", "ok"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}

	// Split into minority {node1, node2} and majority {node3, node4, node5}.
	minority := []string{"node1", "node2"}
	majority := []string{"node3", "node4", "node5"}
	tc.partitionGroup(minority, majority)
	t.Log("Partitioned: minority={node1,node2} majority={node3,node4,node5}")

	// Wait for the majority side to elect a leader.
	var majorityLeader string
	err = tc.waitFor(5*time.Second, "majority leader", func() bool {
		active := tc.activeNodes()
		for _, name := range majority {
			sm, ok := active[name]
			if !ok {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			snapshot, e := sm.GetSnapshot(ctx)
			cancel()
			if e != nil {
				continue
			}
			if snapshot.Role == konsen.Role_LEADER {
				majorityLeader = name
				return true
			}
		}
		return false
	})
	if err != nil {
		t.Fatalf("majority side failed to elect leader: %v", err)
	}

	// Write through the majority leader.
	if err := tc.appendKV(majorityLeader, "majority-write", "ok"); err != nil {
		t.Fatalf("appendKV to majority leader: %v", err)
	}

	// Minority should NOT be able to commit (can't get 3/5 votes).
	for _, name := range minority {
		tc.mu.Lock()
		sm := tc.nodes[name]
		tc.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		snapshot, _ := sm.GetSnapshot(ctx)
		cancel()
		if snapshot != nil && snapshot.Role == konsen.Role_LEADER {
			err := tc.appendKV(name, "minority-write", "should-fail")
			if err == nil {
				t.Errorf("minority leader %s committed without quorum — split-brain violation", name)
			}
		}
	}

	// Heal the partition.
	tc.healAll()
	t.Log("Healed partition")

	// Cluster should converge to exactly one leader.
	if err := tc.waitFor(5*time.Second, "single leader after heal", func() bool {
		return tc.countLeaders() == 1
	}); err != nil {
		t.Errorf("expected 1 leader after heal, got %d", tc.countLeaders())
	}

	// All nodes should eventually have the majority-written data.
	if err := tc.waitForAllValues("majority-write", "ok", 5*time.Second); err != nil {
		t.Errorf("data not converged after heal: %v", err)
	}
}

func TestIntegration_CascadingPartitions(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3", "node4", "node5"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	if err := tc.appendKV(leader, "k0", "v0"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}

	// Partition node1 — 4 nodes remain, quorum (3/5) intact.
	tc.partition("node1")
	t.Log("Partitioned node1")

	err = tc.waitFor(5*time.Second, "leader after first partition", func() bool {
		active := tc.activeNodes()
		for name, sm := range active {
			if name == "node1" {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			snapshot, e := sm.GetSnapshot(ctx)
			cancel()
			if e != nil {
				continue
			}
			if snapshot.Role == konsen.Role_LEADER {
				leader = name
				return true
			}
		}
		return false
	})
	if err != nil {
		t.Fatalf("no leader after partitioning node1: %v", err)
	}

	if err := tc.appendKV(leader, "k1", "v1"); err != nil {
		t.Fatalf("appendKV after first partition: %v", err)
	}

	// Partition node2 — 3 nodes remain, quorum (3/5) still intact.
	tc.partition("node2")
	t.Log("Partitioned node2")

	err = tc.waitFor(5*time.Second, "leader after second partition", func() bool {
		active := tc.activeNodes()
		for name, sm := range active {
			if name == "node1" || name == "node2" {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			snapshot, e := sm.GetSnapshot(ctx)
			cancel()
			if e != nil {
				continue
			}
			if snapshot.Role == konsen.Role_LEADER {
				leader = name
				return true
			}
		}
		return false
	})
	if err != nil {
		t.Fatalf("no leader after partitioning node2: %v", err)
	}

	if err := tc.appendKV(leader, "k2", "v2"); err != nil {
		t.Fatalf("appendKV after second partition: %v", err)
	}

	// Heal all partitions.
	tc.healAll()
	t.Log("Healed all partitions")

	if err := tc.waitFor(5*time.Second, "single leader after heal", func() bool {
		return tc.countLeaders() == 1
	}); err != nil {
		t.Errorf("expected 1 leader after heal, got %d", tc.countLeaders())
	}

	// All nodes should have all data.
	for i := range 3 {
		key := fmt.Sprintf("k%d", i)
		expected := fmt.Sprintf("v%d", i)
		if err := tc.waitForAllValues(key, expected, 5*time.Second); err != nil {
			t.Errorf("not all nodes converged on %s=%s: %v", key, expected, err)
		}
	}
}

func TestIntegration_AsymmetricPartition(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Asymmetric partition: follower -> leader blocked, leader -> follower works.
	follower := tc.followers(leader)[0]
	tc.partitionAsymmetric(follower, leader)
	t.Logf("Asymmetric partition: %s -> %s blocked", follower, leader)

	// Write data through the leader.
	if err := tc.appendKV(leader, "asym-test", "value"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}

	// Heal and verify convergence.
	tc.healAll()
	if err := tc.waitFor(5*time.Second, "single leader", func() bool {
		return tc.countLeaders() == 1
	}); err != nil {
		t.Errorf("expected 1 leader after healing, got %d", tc.countLeaders())
	}

	if err := tc.waitForAllValues("asym-test", "value", 5*time.Second); err != nil {
		t.Errorf("data not converged: %v", err)
	}
}

func TestIntegration_QuorumLoss(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Write data while quorum is healthy.
	if err := tc.appendKV(leader, "healthy", "yes"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}

	// Partition both followers — leader loses quorum.
	for _, f := range tc.followers(leader) {
		tc.partition(f)
	}
	t.Log("Both followers partitioned — quorum lost")

	// Writes should fail (can't commit without quorum).
	err = tc.appendKV(leader, "should-fail", "yes")
	if err == nil {
		t.Error("write succeeded without quorum — expected failure")
	} else {
		t.Logf("Write correctly failed without quorum: %v", err)
	}

	// Heal and verify recovery.
	tc.healAll()
	newLeader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no leader after healing: %v", err)
	}

	val, err := tc.getValue(newLeader, "healthy")
	if err != nil {
		t.Fatalf("getValue: %v", err)
	}
	if val != "yes" {
		t.Errorf("getValue(healthy) = %q, want %q", val, "yes")
	}
}

// ============================================================================
// Stress Tests
// ============================================================================

func TestIntegration_SlowFollower(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Slow down one follower (100ms latency on all connections).
	slowFollower := tc.followers(leader)[0]
	tc.setNodeDelay(slowFollower, 100*time.Millisecond)
	t.Logf("Slowed follower: %s", slowFollower)

	// Write multiple entries.
	for i := range 10 {
		if err := tc.appendKV(leader, fmt.Sprintf("slow-%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("appendKV(slow-%d): %v", i, err)
		}
	}

	// Leader should still be the leader.
	if err := tc.waitFor(3*time.Second, "leader stability", func() bool {
		l, _ := tc.getLeader()
		return l == leader
	}); err != nil {
		t.Errorf("leader changed due to slow follower: %v", err)
	}

	// Remove delay and verify the slow follower catches up.
	tc.setNodeDelay(slowFollower, 0)
	if err := tc.waitForValue(slowFollower, "slow-9", "v9", 10*time.Second); err != nil {
		t.Errorf("slow follower did not catch up: %v", err)
	}
}

func TestIntegration_WriteDuringLeaderTransition(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader1, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	var (
		wg         sync.WaitGroup
		successes  atomic.Int64
		failures   atomic.Int64
		successMap sync.Map
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Background writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key := fmt.Sprintf("transition-%d", i)
			value := fmt.Sprintf("v%d", i)

			currentLeader, err := tc.getLeader()
			if err != nil {
				failures.Add(1)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if err := tc.appendKV(currentLeader, key, value); err != nil {
				failures.Add(1)
			} else {
				successes.Add(1)
				successMap.Store(key, value)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait until some writes go through.
	if err := tc.waitFor(5*time.Second, "initial writes", func() bool {
		return successes.Load() >= 3
	}); err != nil {
		cancel()
		wg.Wait()
		t.Fatalf("no initial writes succeeded: %v", err)
	}

	// Partition the leader to force a transition.
	tc.partition(leader1)
	t.Logf("Partitioned leader: %s", leader1)

	leader2, err := tc.waitForNewLeader(5*time.Second, leader1)
	if err != nil {
		cancel()
		wg.Wait()
		t.Fatalf("no new leader: %v", err)
	}
	t.Logf("New leader: %s", leader2)

	// Wait for a few more writes to go through under the new leader.
	startSuccesses := successes.Load()
	if err := tc.waitFor(5*time.Second, "writes under new leader", func() bool {
		return successes.Load() >= startSuccesses+3
	}); err != nil {
		t.Logf("warning: few writes under new leader: %v", err)
	}

	// Heal and wait for single leader.
	tc.healAll()
	if err := tc.waitFor(5*time.Second, "single leader after heal", func() bool {
		return tc.countLeaders() == 1
	}); err != nil {
		t.Logf("warning: slow convergence after heal: %v", err)
	}

	cancel()
	wg.Wait()
	t.Logf("Successes: %d, Failures: %d", successes.Load(), failures.Load())

	// Every successful write must be readable.
	finalLeader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no leader after stabilization: %v", err)
	}

	// Trigger commitment of any pending entries from previous terms.
	if err := tc.appendKV(finalLeader, "commit-trigger", "done"); err != nil {
		t.Fatalf("commit trigger failed: %v", err)
	}

	var verified int
	successMap.Range(func(k, v any) bool {
		key := k.(string)
		expected := v.(string)
		val, err := tc.getValue(finalLeader, key)
		if err != nil {
			t.Errorf("getValue(%s) failed: %v", key, err)
		} else if val != expected {
			t.Errorf("getValue(%s) = %q, want %q", key, val, expected)
		} else {
			verified++
		}
		return true
	})
	t.Logf("Verified %d successful writes are durable", verified)
	if verified == 0 {
		t.Error("no successful writes were verified")
	}
}

func TestIntegration_RepeatedElections(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	const rounds = 5
	for round := range rounds {
		leader, err := tc.waitForLeader(5 * time.Second)
		if err != nil {
			t.Fatalf("round %d: no leader: %v", round, err)
		}

		key := fmt.Sprintf("round-%d", round)
		if err := tc.appendKV(leader, key, "ok"); err != nil {
			t.Fatalf("round %d: appendKV: %v", round, err)
		}

		// Force re-election.
		tc.partition(leader)
		t.Logf("Round %d: partitioned leader %s", round, leader)

		newLeader, err := tc.waitForNewLeader(5*time.Second, leader)
		if err != nil {
			t.Fatalf("round %d: no new leader: %v", round, err)
		}
		t.Logf("Round %d: new leader %s", round, newLeader)

		tc.healAll()
		if err := tc.waitFor(3*time.Second, "single leader", func() bool {
			return tc.countLeaders() == 1
		}); err != nil {
			t.Errorf("round %d: expected 1 leader after heal, got %d", round, tc.countLeaders())
		}
	}

	// After all rounds, verify all data.
	leader, _ := tc.waitForLeader(5 * time.Second)

	// Write a new entry to trigger commitment of pending entries from previous terms
	// (Raft leaders only advance commitIndex for entries from their own term).
	if err := tc.appendKV(leader, "commit-trigger", "ok"); err != nil {
		t.Fatalf("appendKV(commit-trigger): %v", err)
	}

	for round := range rounds {
		key := fmt.Sprintf("round-%d", round)
		if err := tc.waitForValue(leader, key, "ok", 5*time.Second); err != nil {
			t.Errorf("data from round %d not found: %v", round, err)
		}
	}

	// Verify term monotonicity.
	for name, s := range tc.storage {
		logs, _ := s.GetLogsFrom(1)
		var prevTerm uint64
		for _, log := range logs {
			if log.GetTerm() < prevTerm {
				t.Errorf("node %s: non-monotonic terms (index %d: term %d < %d)",
					name, log.GetIndex(), log.GetTerm(), prevTerm)
			}
			prevTerm = log.GetTerm()
		}
	}
}

func TestIntegration_MessageDrops(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Set 30% message drop rate on one follower.
	follower := tc.followers(leader)[0]
	tc.setNodeDropRate(follower, 0.3)
	t.Logf("Set 30%% drop rate on %s", follower)

	// Writes should still succeed despite drops (leader retries via heartbeat).
	for i := range 10 {
		if err := tc.appendKV(leader, fmt.Sprintf("drop-%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("appendKV(drop-%d): %v", i, err)
		}
	}

	// Remove drops and verify full replication.
	tc.setNodeDropRate(follower, 0)
	if err := tc.waitForAllValues("drop-9", "v9", 10*time.Second); err != nil {
		t.Errorf("data not fully replicated: %v", err)
	}
}

func TestIntegration_ChaosMonkey(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos monkey test skipped in short mode")
	}

	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	tc := newTestCluster(t, nodes)
	tc.start()
	defer tc.stop()

	if _, err := tc.waitForLeader(5 * time.Second); err != nil {
		t.Fatalf("initial leader election failed: %v", err)
	}

	const chaosDuration = 10 * time.Second

	var (
		writeSuccesses atomic.Int64
		writeFailures  atomic.Int64
		successKeys    sync.Map
	)

	chaosCtx, chaosCancel := context.WithTimeout(context.Background(), chaosDuration)
	defer chaosCancel()

	// Separate context for the writer so we can stop it before cleanup.
	writerCtx, writerCancel := context.WithCancel(chaosCtx)
	defer writerCancel()

	// Writer goroutine.
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for i := 0; ; i++ {
			select {
			case <-writerCtx.Done():
				return
			default:
			}

			currentLeader, err := tc.getLeader()
			if err != nil {
				writeFailures.Add(1)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			key := fmt.Sprintf("chaos-%d", i)
			value := fmt.Sprintf("v%d", i)
			if err := tc.appendKV(currentLeader, key, value); err != nil {
				writeFailures.Add(1)
			} else {
				writeSuccesses.Add(1)
				successKeys.Store(key, value)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Chaos goroutine.
	var chaosWg sync.WaitGroup
	chaosWg.Add(1)
	go func() {
		defer chaosWg.Done()
		for {
			select {
			case <-chaosCtx.Done():
				return
			default:
			}

			action := rand.IntN(4)
			node := nodes[rand.IntN(len(nodes))]

			switch action {
			case 0:
				tc.partition(node)
			case 1:
				tc.heal(node)
			case 2:
				tc.healAll()
			case 3:
				if !tc.isStopped(node) {
					tc.stopNode(node)
					time.Sleep(200 * time.Millisecond)
					if err := tc.restartNode(node); err != nil {
						t.Logf("CHAOS: restart %s failed: %v", node, err)
					}
				}
			}

			time.Sleep(time.Duration(200+rand.IntN(500)) * time.Millisecond)
		}
	}()

	<-chaosCtx.Done()

	// Stop writer first, then wait for chaos goroutine to finish,
	// so that cleanup (healAll + restart) doesn't race with writes.
	writerCancel()
	writerWg.Wait()
	chaosWg.Wait()

	// Restore the cluster.
	tc.healAll()
	for _, name := range nodes {
		if tc.isStopped(name) {
			if err := tc.restartNode(name); err != nil {
				t.Fatalf("restartNode(%s): %v", name, err)
			}
		}
	}

	t.Logf("Chaos results: %d successes, %d failures", writeSuccesses.Load(), writeFailures.Load())

	// Wait for leader election.
	finalLeader, err := tc.waitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("no leader after chaos: %v", err)
	}

	// Exactly one leader.
	if count := tc.countLeaders(); count != 1 {
		t.Errorf("expected 1 leader after chaos, got %d", count)
	}

	// Trigger commitment of pending entries from previous terms.
	// After chaos the leader may still be reconciling logs, so retry with re-election.
	if err := tc.waitFor(15*time.Second, "commit trigger", func() bool {
		leader, err := tc.getLeader()
		if err != nil {
			return false
		}
		return tc.appendKV(leader, "commit-trigger", "done") == nil
	}); err != nil {
		t.Fatalf("commit trigger failed: %v", err)
	}

	// Every successfully written key must be readable.
	var verified, missing int
	successKeys.Range(func(k, v any) bool {
		key := k.(string)
		expected := v.(string)
		if err := tc.waitForValue(finalLeader, key, expected, 5*time.Second); err != nil {
			missing++
			t.Errorf("chaos write lost: key=%s expected=%s: %v", key, expected, err)
		} else {
			verified++
		}
		return true
	})
	t.Logf("Verified %d writes, %d missing", verified, missing)

	// Verify term monotonicity.
	for name, s := range tc.storage {
		logs, _ := s.GetLogsFrom(1)
		var prevTerm uint64
		for _, l := range logs {
			if l.GetTerm() < prevTerm {
				t.Errorf("node %s: non-monotonic terms (index %d: term %d < %d)",
					name, l.GetIndex(), l.GetTerm(), prevTerm)
			}
			prevTerm = l.GetTerm()
		}
	}
}

// ============================================================================
// Consistency Tests
// ============================================================================

func TestIntegration_SequentialConsistency(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Sequential writes and reads: every read must see the most recent write.
	key := "seq-key"
	for i := range 20 {
		value := fmt.Sprintf("v%d", i)
		if err := tc.appendKV(leader, key, value); err != nil {
			t.Fatalf("appendKV(%s=%s): %v", key, value, err)
		}

		got, err := tc.getValue(leader, key)
		if err != nil {
			t.Fatalf("getValue(%s): %v", key, err)
		}
		if got != value {
			t.Errorf("stale read: wrote %q, got %q", value, got)
		}
	}
}

func TestIntegration_NoStaleReadsFromLeader(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Overwrite the same key repeatedly; immediate reads must always see the latest value.
	for i := range 10 {
		expected := fmt.Sprintf("%d", i)
		if err := tc.appendKV(leader, "version", expected); err != nil {
			t.Fatalf("appendKV(version=%s): %v", expected, err)
		}

		val, err := tc.getValue(leader, "version")
		if err != nil {
			t.Fatalf("getValue(version): %v", err)
		}
		if val != expected {
			t.Errorf("stale read at iteration %d: wrote %q, got %q", i, expected, val)
		}
	}
}

func TestIntegration_ElectionSafetyUnderChurn(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"})
	tc.start()
	defer tc.stop()

	// Force several rapid elections.
	for i := range 5 {
		leader, err := tc.waitForLeader(5 * time.Second)
		if err != nil {
			t.Fatalf("iteration %d: no leader: %v", i, err)
		}
		tc.partition(leader)
		time.Sleep(500 * time.Millisecond)
		tc.healAll()
		time.Sleep(500 * time.Millisecond)
	}

	// After churn, exactly one leader.
	if err := tc.waitFor(5*time.Second, "single leader", func() bool {
		return tc.countLeaders() == 1
	}); err != nil {
		t.Errorf("expected 1 leader after churn, got %d", tc.countLeaders())
	}

	// Verify term monotonicity across all nodes.
	for name, s := range tc.storage {
		logs, _ := s.GetLogsFrom(1)
		var prevTerm uint64
		for _, l := range logs {
			if l.GetTerm() < prevTerm {
				t.Errorf("node %s: term decreased (index %d: %d < %d)",
					name, l.GetIndex(), l.GetTerm(), prevTerm)
			}
			prevTerm = l.GetTerm()
		}
	}
}
