package konsen_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lizhaoliu/konsen/v2/core"
	"github.com/lizhaoliu/konsen/v2/datastore"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// Snapshot-aware test cluster: uses a low snapshot threshold to trigger compaction.
// ============================================================================

func newSnapshotTestCluster(t *testing.T, nodeNames []string, snapshotThreshold uint64) *testCluster {
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
		servers[name] = fmt.Sprintf("localhost:%d", 10000)
	}

	for _, name := range nodeNames {
		tc.storage[name] = datastore.NewMemStorage()
	}

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

		sm, err := core.NewStateMachine(core.StateMachineConfig{
			Storage:           tc.storage[name],
			Cluster:           cluster,
			Clients:           clientMap,
			MinTimeout:        testMinTimeout,
			TimeoutSpan:       testTimeoutSpan,
			Heartbeat:         testHeartbeat,
			SnapshotThreshold: snapshotThreshold,
			SnapshotChunkSize: 512, // Small chunks for testing chunked transfer
		})
		if err != nil {
			t.Fatalf("NewStateMachine(%s): %v", name, err)
		}
		tc.nodes[name] = sm
	}

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
// Integration Tests: Snapshot / Log Compaction
// ============================================================================

func TestSnapshot_TriggersCompaction(t *testing.T) {
	threshold := uint64(5)
	tc := newSnapshotTestCluster(t, []string{"node1", "node2", "node3"}, threshold)
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Write threshold+1 entries to trigger a snapshot.
	for i := 0; i < int(threshold)+1; i++ {
		if err := tc.appendKV(leader, fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)); err != nil {
			t.Fatalf("appendKV(%d): %v", i, err)
		}
	}

	// Wait for snapshot to be taken.
	time.Sleep(500 * time.Millisecond)

	// Verify the leader's storage has snapshot metadata set.
	tc.mu.Lock()
	leaderStorage := tc.storage[leader]
	tc.mu.Unlock()

	snapIdx, snapTerm, err := leaderStorage.GetSnapshotMeta()
	if err != nil {
		t.Fatalf("GetSnapshotMeta: %v", err)
	}
	if snapIdx == 0 {
		t.Fatal("expected snapshot to be taken (snapIdx > 0)")
	}
	t.Logf("Snapshot at index %d, term %d", snapIdx, snapTerm)

	// Verify log entries before the snapshot index have been deleted.
	for i := uint64(1); i <= snapIdx; i++ {
		logEntry, err := leaderStorage.GetLog(i)
		if err != nil {
			t.Fatalf("GetLog(%d): %v", i, err)
		}
		if logEntry != nil {
			t.Errorf("log entry at index %d should have been deleted after snapshot", i)
		}
	}

	// Verify snapshot file exists.
	snapFile, err := leaderStorage.LoadSnapshotFile()
	if err != nil {
		t.Fatalf("LoadSnapshotFile: %v", err)
	}
	if snapFile == nil {
		t.Fatal("expected snapshot file to exist")
	}

	// Verify data is still readable.
	for i := 0; i < int(threshold)+1; i++ {
		val, err := tc.getValue(leader, fmt.Sprintf("key%d", i))
		if err != nil {
			t.Fatalf("getValue(key%d): %v", i, err)
		}
		if val != fmt.Sprintf("val%d", i) {
			t.Errorf("getValue(key%d) = %q, want %q", i, val, fmt.Sprintf("val%d", i))
		}
	}
}

func TestSnapshot_SlowFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	threshold := uint64(5)
	tc := newSnapshotTestCluster(t, []string{"node1", "node2", "node3"}, threshold)
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Find a follower and partition it.
	var slowFollower string
	for name := range tc.nodes {
		if name != leader {
			slowFollower = name
			break
		}
	}
	tc.partition(slowFollower)

	// Write enough entries on the majority to trigger compaction.
	for i := 0; i < int(threshold)+2; i++ {
		if err := tc.appendKV(leader, fmt.Sprintf("snap-key%d", i), fmt.Sprintf("snap-val%d", i)); err != nil {
			t.Fatalf("appendKV(%d): %v", i, err)
		}
	}

	// Wait for snapshot to be taken on the leader.
	time.Sleep(500 * time.Millisecond)

	tc.mu.Lock()
	leaderSnapIdx, _, _ := tc.storage[leader].GetSnapshotMeta()
	tc.mu.Unlock()
	if leaderSnapIdx == 0 {
		t.Fatal("expected leader to have taken a snapshot")
	}
	t.Logf("Leader snapshot at index %d", leaderSnapIdx)

	// Heal the partition -- the slow follower should catch up via InstallSnapshot.
	tc.heal(slowFollower)

	// Wait for the follower to catch up.
	time.Sleep(2 * time.Second)

	// Verify the slow follower has the data.
	for i := 0; i < int(threshold)+2; i++ {
		tc.mu.Lock()
		s := tc.storage[slowFollower]
		tc.mu.Unlock()
		val, err := s.GetValue([]byte(fmt.Sprintf("snap-key%d", i)))
		if err != nil {
			t.Fatalf("storage[%s].GetValue(snap-key%d): %v", slowFollower, i, err)
		}
		if string(val) != fmt.Sprintf("snap-val%d", i) {
			t.Errorf("storage[%s].GetValue(snap-key%d) = %q, want %q", slowFollower, i, val, fmt.Sprintf("snap-val%d", i))
		}
	}

	// Verify the slow follower has snapshot metadata.
	tc.mu.Lock()
	followerSnapIdx, _, _ := tc.storage[slowFollower].GetSnapshotMeta()
	tc.mu.Unlock()
	if followerSnapIdx == 0 {
		t.Error("expected slow follower to have installed a snapshot")
	}
	t.Logf("Slow follower snapshot at index %d", followerSnapIdx)
}

func TestSnapshot_NormalReplicationUnaffectedAfterCompaction(t *testing.T) {
	threshold := uint64(5)
	tc := newSnapshotTestCluster(t, []string{"node1", "node2", "node3"}, threshold)
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Trigger compaction.
	for i := 0; i < int(threshold)+1; i++ {
		if err := tc.appendKV(leader, fmt.Sprintf("pre%d", i), fmt.Sprintf("preval%d", i)); err != nil {
			t.Fatalf("appendKV pre(%d): %v", i, err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	// Write more entries after compaction -- should replicate via normal AppendEntries.
	for i := 0; i < 3; i++ {
		if err := tc.appendKV(leader, fmt.Sprintf("post%d", i), fmt.Sprintf("postval%d", i)); err != nil {
			t.Fatalf("appendKV post(%d): %v", i, err)
		}
	}
	time.Sleep(300 * time.Millisecond)

	// Verify all nodes have the post-compaction data.
	for name, s := range tc.storage {
		for i := 0; i < 3; i++ {
			val, err := s.GetValue([]byte(fmt.Sprintf("post%d", i)))
			if err != nil {
				t.Fatalf("storage[%s].GetValue(post%d): %v", name, i, err)
			}
			if string(val) != fmt.Sprintf("postval%d", i) {
				t.Errorf("storage[%s].GetValue(post%d) = %q, want %q", name, i, val, fmt.Sprintf("postval%d", i))
			}
		}
	}
}

func TestSnapshot_LeaderRestartsWithSnapshot(t *testing.T) {
	threshold := uint64(5)
	tc := newSnapshotTestCluster(t, []string{"node1", "node2", "node3"}, threshold)
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Trigger compaction.
	for i := 0; i < int(threshold)+1; i++ {
		if err := tc.appendKV(leader, fmt.Sprintf("persist%d", i), fmt.Sprintf("val%d", i)); err != nil {
			t.Fatalf("appendKV(%d): %v", i, err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	// Verify snapshot was taken.
	tc.mu.Lock()
	snapIdx, _, _ := tc.storage[leader].GetSnapshotMeta()
	tc.mu.Unlock()
	if snapIdx == 0 {
		t.Fatal("expected snapshot before restart")
	}

	// Stop and restart the leader.
	tc.stopNode(leader)
	time.Sleep(200 * time.Millisecond)

	if err := tc.restartNode(leader); err != nil {
		t.Fatalf("restartNode(%s): %v", leader, err)
	}

	// Wait for a new leader to be elected (may or may not be the same node).
	newLeader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election after restart failed: %v", err)
	}
	t.Logf("New leader: %s", newLeader)

	// Write more data to verify the cluster works.
	if err := tc.appendKV(newLeader, "after-restart", "works"); err != nil {
		t.Fatalf("appendKV after restart: %v", err)
	}

	val, err := tc.getValue(newLeader, "after-restart")
	if err != nil {
		t.Fatalf("getValue after restart: %v", err)
	}
	if val != "works" {
		t.Errorf("getValue(after-restart) = %q, want %q", val, "works")
	}

	// Verify pre-compaction data is still accessible.
	for i := 0; i < int(threshold)+1; i++ {
		val, err := tc.getValue(newLeader, fmt.Sprintf("persist%d", i))
		if err != nil {
			t.Fatalf("getValue(persist%d): %v", i, err)
		}
		if val != fmt.Sprintf("val%d", i) {
			t.Errorf("getValue(persist%d) = %q, want %q", i, val, fmt.Sprintf("val%d", i))
		}
	}
}

func TestSnapshot_InstallSnapshotOverwritesStaleState(t *testing.T) {
	threshold := uint64(5)
	tc := newSnapshotTestCluster(t, []string{"node1", "node2", "node3"}, threshold)
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Write initial data.
	if err := tc.appendKV(leader, "color", "red"); err != nil {
		t.Fatalf("appendKV: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// Partition a follower.
	var follower string
	for name := range tc.nodes {
		if name != leader {
			follower = name
			break
		}
	}
	tc.partition(follower)

	// Overwrite the key and add more entries to trigger compaction.
	if err := tc.appendKV(leader, "color", "blue"); err != nil {
		t.Fatalf("appendKV overwrite: %v", err)
	}
	for i := 0; i < int(threshold); i++ {
		if err := tc.appendKV(leader, fmt.Sprintf("pad%d", i), "x"); err != nil {
			t.Fatalf("appendKV pad(%d): %v", i, err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	// Verify the follower still has the old value.
	tc.mu.Lock()
	followerStorage := tc.storage[follower]
	tc.mu.Unlock()
	val, _ := followerStorage.GetValue([]byte("color"))
	if string(val) != "red" {
		t.Logf("follower color = %q (expected red before heal)", val)
	}

	// Heal -- follower gets InstallSnapshot which should overwrite its KV state.
	tc.heal(follower)
	time.Sleep(2 * time.Second)

	// Verify follower now has the updated value.
	val, err = followerStorage.GetValue([]byte("color"))
	if err != nil {
		t.Fatalf("followerStorage.GetValue: %v", err)
	}
	if string(val) != "blue" {
		t.Errorf("follower color after heal = %q, want %q", val, "blue")
	}
}

func TestSnapshot_MultiChunkTransfer(t *testing.T) {
	// Use a very small chunk size (64 bytes) to force multiple chunks.
	threshold := uint64(5)
	tc := &testCluster{
		t:       t,
		nodes:   make(map[string]*core.StateMachine),
		storage: make(map[string]*datastore.MemStorage),
		clients: make(map[string]map[string]*inProcessClient),
		cancels: make(map[string]context.CancelFunc),
		stopped: make(map[string]bool),
	}

	nodeNames := []string{"node1", "node2", "node3"}
	servers := make(map[string]string)
	for _, name := range nodeNames {
		servers[name] = "localhost:10000"
	}

	for _, name := range nodeNames {
		tc.storage[name] = datastore.NewMemStorage()
	}

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

		sm, err := core.NewStateMachine(core.StateMachineConfig{
			Storage:           tc.storage[name],
			Cluster:           cluster,
			Clients:           clientMap,
			MinTimeout:        testMinTimeout,
			TimeoutSpan:       testTimeoutSpan,
			Heartbeat:         testHeartbeat,
			SnapshotThreshold: threshold,
			SnapshotChunkSize: 64, // Very small -- will require many chunks
		})
		if err != nil {
			t.Fatalf("NewStateMachine(%s): %v", name, err)
		}
		tc.nodes[name] = sm
	}

	for _, name := range nodeNames {
		for peer, client := range tc.clients[name] {
			client.mu.Lock()
			client.target = tc.nodes[peer]
			client.mu.Unlock()
		}
	}

	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Partition a follower.
	var follower string
	for name := range tc.nodes {
		if name != leader {
			follower = name
			break
		}
	}
	tc.partition(follower)

	// Write enough data to trigger a snapshot that's larger than 64 bytes.
	for i := 0; i < int(threshold)+2; i++ {
		largeVal := fmt.Sprintf("value-%d-padding-to-make-it-bigger-for-chunking", i)
		if err := tc.appendKV(leader, fmt.Sprintf("chunk-key%d", i), largeVal); err != nil {
			t.Fatalf("appendKV(%d): %v", i, err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	// Verify snapshot was taken.
	tc.mu.Lock()
	snapIdx, _, _ := tc.storage[leader].GetSnapshotMeta()
	snapFile, _ := tc.storage[leader].LoadSnapshotFile()
	tc.mu.Unlock()
	if snapIdx == 0 {
		t.Fatal("expected snapshot to be taken")
	}
	t.Logf("Snapshot at index %d, file size %d bytes (chunk size 64)", snapIdx, len(snapFile))
	if len(snapFile) <= 64 {
		t.Logf("Warning: snapshot file is small (%d bytes), may not test multi-chunk", len(snapFile))
	}

	// Heal and let the follower catch up via multi-chunk InstallSnapshot.
	tc.heal(follower)
	time.Sleep(2 * time.Second)

	// Verify the follower has all the data.
	tc.mu.Lock()
	followerStorage := tc.storage[follower]
	tc.mu.Unlock()
	for i := 0; i < int(threshold)+2; i++ {
		val, err := followerStorage.GetValue([]byte(fmt.Sprintf("chunk-key%d", i)))
		if err != nil {
			t.Fatalf("follower GetValue(chunk-key%d): %v", i, err)
		}
		expected := fmt.Sprintf("value-%d-padding-to-make-it-bigger-for-chunking", i)
		if string(val) != expected {
			t.Errorf("follower GetValue(chunk-key%d) = %q, want %q", i, val, expected)
		}
	}
}

func TestSnapshot_PartialTransferInterruptedByPartition(t *testing.T) {
	threshold := uint64(5)
	tc := newSnapshotTestCluster(t, []string{"node1", "node2", "node3"}, threshold)
	tc.start()
	defer tc.stop()

	leader, err := tc.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Partition a follower.
	var follower string
	for name := range tc.nodes {
		if name != leader {
			follower = name
			break
		}
	}
	tc.partition(follower)

	// Write enough to trigger compaction.
	for i := 0; i < int(threshold)+2; i++ {
		if err := tc.appendKV(leader, fmt.Sprintf("retry-key%d", i), fmt.Sprintf("retry-val%d", i)); err != nil {
			t.Fatalf("appendKV(%d): %v", i, err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	// Briefly heal then re-partition to simulate interrupted transfer.
	tc.heal(follower)
	time.Sleep(100 * time.Millisecond) // Brief window -- may or may not complete transfer
	tc.partition(follower)
	time.Sleep(200 * time.Millisecond)

	// Now heal permanently and let it complete.
	tc.heal(follower)
	time.Sleep(2 * time.Second)

	// Verify data converged regardless of the interrupted attempt.
	tc.mu.Lock()
	followerStorage := tc.storage[follower]
	tc.mu.Unlock()
	for i := 0; i < int(threshold)+2; i++ {
		val, err := followerStorage.GetValue([]byte(fmt.Sprintf("retry-key%d", i)))
		if err != nil {
			t.Fatalf("follower GetValue(retry-key%d): %v", i, err)
		}
		if string(val) != fmt.Sprintf("retry-val%d", i) {
			t.Errorf("follower GetValue(retry-key%d) = %q, want %q", i, val, fmt.Sprintf("retry-val%d", i))
		}
	}
}

// Helpers to use the restartNode from integration_advanced_test.go
// (defined in the same package, so available directly).

// Verify SnapshotKVData/RestoreKVData round-trip at the storage level
// (complementary to the unit tests in datastore/storage_test.go, but using proto).
func TestSnapshot_KVRoundTrip(t *testing.T) {
	s := datastore.NewMemStorage()

	s.SetValue([]byte("alpha"), []byte("1"))
	s.SetValue([]byte("beta"), []byte("2"))
	s.SetValue([]byte("gamma"), []byte("3"))

	data, err := s.SnapshotKVData()
	if err != nil {
		t.Fatalf("SnapshotKVData: %v", err)
	}

	// Restore into a fresh storage.
	s2 := datastore.NewMemStorage()
	if err := s2.RestoreKVData(data); err != nil {
		t.Fatalf("RestoreKVData: %v", err)
	}

	for _, key := range []string{"alpha", "beta", "gamma"} {
		v1, _ := s.GetValue([]byte(key))
		v2, _ := s2.GetValue([]byte(key))
		if string(v1) != string(v2) {
			t.Errorf("key %q: original=%q, restored=%q", key, v1, v2)
		}
	}
}

// TestSnapshot_InstallFailureLeavesMetadataUnchanged is a regression test for the
// crash-safety ordering in handleInstallSnapshot. If RestoreKVData fails while
// installing a snapshot, the node must NOT persist the snapshot metadata: doing so
// would leave it permanently believing it is caught up (snapshotIndex advanced) while
// its KV state is stale and the compacted logs are gone -- an unrecoverable state.
// The metadata must only become durable after the KV state has been restored.
func TestSnapshot_InstallFailureLeavesMetadataUnchanged(t *testing.T) {
	inner := datastore.NewMemStorage()
	fs := newFaultyStorage(inner)
	fs.SetFault("RestoreKVData", alwaysFail(fmt.Errorf("injected RestoreKVData failure")))

	cluster := &core.ClusterConfig{
		Servers:         map[string]string{"node1": "localhost:10000"},
		LocalServerName: "node1",
	}
	sm, err := core.NewStateMachine(core.StateMachineConfig{
		Storage:     fs,
		Cluster:     cluster,
		Clients:     map[string]*core.PeerClient{},
		MinTimeout:  testMinTimeout,
		TimeoutSpan: testTimeoutSpan,
		Heartbeat:   testHeartbeat,
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		sm.Close()
	}()
	go sm.Run(ctx)
	time.Sleep(50 * time.Millisecond) // let the message loop initialize

	// A complete (done=true) snapshot payload. Term is well above the node's term so
	// it is accepted (the node converts to follower and processes the install).
	kvData, err := proto.Marshal(&konsen.KVList{
		KvList: []*konsen.KV{{Key: []byte("k"), Value: []byte("v")}},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	req := &konsen.InstallSnapshotReq{
		Term:              100,
		LeaderId:          "leader",
		LastIncludedIndex: 42,
		LastIncludedTerm:  7,
		Offset:            0,
		Data:              kvData,
		Done:              true,
	}

	// The handler swallows the internal error and replies with the current term, so the
	// RPC itself returns without error even though the restore failed.
	if _, err := sm.InstallSnapshot(ctx, req); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}

	// Metadata must remain at the old boundary (0, 0) so the node can recover by
	// receiving the snapshot again on a later heartbeat cycle.
	idx, term, err := inner.GetSnapshotMeta()
	if err != nil {
		t.Fatalf("GetSnapshotMeta: %v", err)
	}
	if idx != 0 || term != 0 {
		t.Errorf("snapshot metadata advanced to (%d, %d) after a failed RestoreKVData; want (0, 0)", idx, term)
	}

	// KV state must be untouched.
	if v, _ := inner.GetValue([]byte("k")); v != nil {
		t.Errorf("KV state was modified despite RestoreKVData failure: got %q, want nil", v)
	}
}
