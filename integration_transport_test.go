package konsen_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lizhaoliu/konsen/v2/core"
	"github.com/lizhaoliu/konsen/v2/datastore"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"github.com/lizhaoliu/konsen/v2/rpc"
	"github.com/lizhaoliu/konsen/v2/web/httpserver"
	"google.golang.org/protobuf/proto"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// ============================================================================
// Transport test cluster: uses real gRPC and HTTP servers
// ============================================================================

type transportTestCluster struct {
	t           *testing.T
	nodes       map[string]*core.StateMachine
	grpcServers map[string]*rpc.RaftGRPCServer
	httpServers map[string]*httpserver.Server
	clients     map[string]map[string]*core.PeerClient
	grpcAddrs   map[string]string
	httpAddrs   map[string]string
	cancels     map[string]context.CancelFunc
	storage     map[string]*datastore.MemStorage
}

func newTransportTestCluster(t *testing.T, nodeNames []string) *transportTestCluster {
	t.Helper()

	tc := &transportTestCluster{
		t:           t,
		nodes:       make(map[string]*core.StateMachine),
		grpcServers: make(map[string]*rpc.RaftGRPCServer),
		httpServers: make(map[string]*httpserver.Server),
		clients:     make(map[string]map[string]*core.PeerClient),
		grpcAddrs:   make(map[string]string),
		httpAddrs:   make(map[string]string),
		cancels:     make(map[string]context.CancelFunc),
		storage:     make(map[string]*datastore.MemStorage),
	}

	// Step 1: Create all listeners to get real addresses.
	grpcListeners := make(map[string]net.Listener)
	httpListeners := make(map[string]net.Listener)
	for _, name := range nodeNames {
		gl, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen gRPC for %s: %v", name, err)
		}
		hl, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			gl.Close()
			t.Fatalf("listen HTTP for %s: %v", name, err)
		}
		grpcListeners[name] = gl
		httpListeners[name] = hl
		tc.grpcAddrs[name] = gl.Addr().String()
		tc.httpAddrs[name] = hl.Addr().String()
	}

	// Step 2: Build cluster config with real addresses.
	servers := make(map[string]string)
	httpServersMap := make(map[string]string)
	for _, name := range nodeNames {
		servers[name] = tc.grpcAddrs[name]
		httpServersMap[name] = tc.httpAddrs[name]
	}

	// Step 3: Create gRPC clients (real connections).
	for _, name := range nodeNames {
		tc.clients[name] = make(map[string]*core.PeerClient)
		for _, peer := range nodeNames {
			if peer != name {
				client, err := rpc.NewPeerGRPCClient(rpc.PeerGRPCClientConfig{
					Endpoint: tc.grpcAddrs[peer],
				})
				if err != nil {
					t.Fatalf("NewPeerGRPCClient(%s→%s): %v", name, peer, err)
				}
				tc.clients[name][peer] = client
			}
		}
	}

	// Step 4: Create storage and state machines.
	for _, name := range nodeNames {
		tc.storage[name] = datastore.NewMemStorage()
		cluster := &core.ClusterConfig{
			Servers:         servers,
			HttpServers:     httpServersMap,
			LocalServerName: name,
		}
		sm, err := core.NewStateMachine(core.StateMachineConfig{
			Storage:     tc.storage[name],
			Cluster:     cluster,
			Clients:     tc.clients[name],
			MinTimeout:  testMinTimeout,
			TimeoutSpan: testTimeoutSpan,
			Heartbeat:   testHeartbeat,
		})
		if err != nil {
			t.Fatalf("NewStateMachine(%s): %v", name, err)
		}
		tc.nodes[name] = sm
	}

	// Step 5: Create and start gRPC servers.
	for _, name := range nodeNames {
		grpcSrv := rpc.NewRaftGRPCServer(rpc.RaftGRPCServerConfig{
			Endpoint:     tc.grpcAddrs[name],
			StateMachine: tc.nodes[name],
		})
		tc.grpcServers[name] = grpcSrv
		lis := grpcListeners[name]
		go func() {
			if err := grpcSrv.ServeListener(lis); err != nil {
				// Ignore errors from server shutdown.
			}
		}()
	}

	// Step 6: Create and start HTTP servers.
	for _, name := range nodeNames {
		httpSrv := httpserver.NewServer(httpserver.ServerConfig{
			StateMachine:    tc.nodes[name],
			Address:         tc.httpAddrs[name],
			LocalServerName: name,
		})
		tc.httpServers[name] = httpSrv
		lis := httpListeners[name]
		go func() {
			if err := httpSrv.RunListener(lis); err != nil {
				// Ignore errors from server shutdown.
			}
		}()
	}

	// Step 7: Start state machines.
	for _, name := range nodeNames {
		ctx, cancel := context.WithCancel(context.Background())
		tc.cancels[name] = cancel
		go tc.nodes[name].Run(ctx)
	}

	// Let message loops initialize.
	time.Sleep(50 * time.Millisecond)

	// Register cleanup in reverse order.
	t.Cleanup(func() {
		// Cancel state machine contexts.
		for _, cancel := range tc.cancels {
			cancel()
		}
		// Close state machines.
		for _, sm := range tc.nodes {
			sm.Close()
		}
		// Stop gRPC servers.
		for _, srv := range tc.grpcServers {
			srv.Stop()
		}
		// Shutdown HTTP servers.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		for _, srv := range tc.httpServers {
			srv.Shutdown(shutdownCtx)
		}
		// Close gRPC clients.
		for _, peerMap := range tc.clients {
			for _, client := range peerMap {
				client.Close()
			}
		}
	})

	return tc
}

func (tc *transportTestCluster) waitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := ""
		multi := false
		for name, sm := range tc.nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			snapshot, err := sm.GetSnapshot(ctx)
			cancel()
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

func (tc *transportTestCluster) followers(leader string) []string {
	var result []string
	for name := range tc.nodes {
		if name != leader {
			result = append(result, name)
		}
	}
	return result
}

// ============================================================================
// HTTP helpers
// ============================================================================

var httpClient = &http.Client{Timeout: 10 * time.Second}

func httpGet(addr, path string, params url.Values) (*http.Response, error) {
	u := fmt.Sprintf("http://%s%s", addr, path)
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	return httpClient.Get(u)
}

func httpPostJSON(addr, path string, body interface{}) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	u := fmt.Sprintf("http://%s%s", addr, path)
	return httpClient.Post(u, "application/json", bytes.NewReader(data))
}

func readBody(resp *http.Response) string {
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func parseJSON(resp *http.Response) map[string]interface{} {
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result
}

// ============================================================================
// Transport Integration Tests
// ============================================================================

func TestTransport_LeaderElection(t *testing.T) {
	tc := newTransportTestCluster(t, []string{"node1", "node2", "node3"})

	leader, err := tc.waitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election over real gRPC failed: %v", err)
	}
	t.Logf("Leader elected over real gRPC: %s", leader)

	// Verify only one leader.
	leaderCount := 0
	for name, sm := range tc.nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		snapshot, err := sm.GetSnapshot(ctx)
		cancel()
		if err != nil {
			t.Fatalf("GetSnapshot(%s): %v", name, err)
		}
		if snapshot.Role == konsen.Role_LEADER {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected 1 leader, got %d", leaderCount)
	}
}

func TestTransport_HTTPWriteRead(t *testing.T) {
	tc := newTransportTestCluster(t, []string{"node1", "node2", "node3"})

	leader, err := tc.waitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	leaderHTTP := tc.httpAddrs[leader]

	// Write via JSON API.
	resp, err := httpPostJSON(leaderHTTP, "/api/kv", map[string]string{
		"key": "color", "value": "blue",
	})
	if err != nil {
		t.Fatalf("HTTP POST /api/kv: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, readBody(resp))
	}
	resp.Body.Close()

	// Read via JSON API.
	resp, err = httpGet(leaderHTTP, "/api/kv", url.Values{"key": {"color"}})
	if err != nil {
		t.Fatalf("HTTP GET /api/kv: %v", err)
	}
	result := parseJSON(resp)
	if result["found"] != true {
		t.Fatalf("expected found=true, got %v", result)
	}
	if result["value"] != "blue" {
		t.Fatalf("expected value=blue, got %v", result["value"])
	}
}

func TestTransport_GRPCKVService(t *testing.T) {
	tc := newTransportTestCluster(t, []string{"node1", "node2", "node3"})

	leader, err := tc.waitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Use the state machine's Put directly (which goes through real gRPC for replication).
	kvList := &konsen.KVList{
		KvList: []*konsen.KV{{Key: []byte("fruit"), Value: []byte("apple")}},
	}
	data, err := proto.Marshal(kvList)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := tc.nodes[leader].Put(ctx, &konsen.PutReq{Data: data})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Put failed: %s", resp.GetErrorMessage())
	}

	// Verify replication to a follower via direct storage read.
	time.Sleep(500 * time.Millisecond)
	for _, name := range tc.followers(leader) {
		val, err := tc.storage[name].GetValue([]byte("fruit"))
		if err != nil {
			t.Errorf("storage.GetValue(%s): %v", name, err)
			continue
		}
		if string(val) != "apple" {
			t.Errorf("%s: expected apple, got %q", name, string(val))
		}
	}
}

func TestTransport_HealthReady(t *testing.T) {
	tc := newTransportTestCluster(t, []string{"node1", "node2", "node3"})

	leader, err := tc.waitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Health check on leader.
	resp, err := httpGet(tc.httpAddrs[leader], "/health", nil)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/health: expected 200, got %d", resp.StatusCode)
	}
	result := parseJSON(resp)
	if result["status"] != "healthy" {
		t.Errorf("expected healthy, got %v", result["status"])
	}
	if result["role"] != "LEADER" {
		t.Errorf("expected LEADER role, got %v", result["role"])
	}

	// Ready check on leader.
	resp, err = httpGet(tc.httpAddrs[leader], "/ready", nil)
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/ready on leader: expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Ready check on follower — should also be 200 since it knows the leader.
	followers := tc.followers(leader)
	if len(followers) > 0 {
		// Give follower time to learn who the leader is.
		time.Sleep(500 * time.Millisecond)
		resp, err = httpGet(tc.httpAddrs[followers[0]], "/ready", nil)
		if err != nil {
			t.Fatalf("GET /ready on follower: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body := readBody(resp)
			t.Errorf("/ready on follower: expected 200, got %d: %s", resp.StatusCode, body)
		} else {
			resp.Body.Close()
		}
	}
}

func TestTransport_StatusEndpoint(t *testing.T) {
	tc := newTransportTestCluster(t, []string{"node1", "node2", "node3"})

	leader, err := tc.waitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	resp, err := httpGet(tc.httpAddrs[leader], "/api/status", nil)
	if err != nil {
		t.Fatalf("GET /api/status: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	result := parseJSON(resp)

	if result["node"] != leader {
		t.Errorf("expected node=%s, got %v", leader, result["node"])
	}
	if result["role"] != "LEADER" {
		t.Errorf("expected LEADER, got %v", result["role"])
	}
	// currentTerm should be > 0 after election.
	if term, ok := result["currentTerm"].(float64); !ok || term < 1 {
		t.Errorf("expected currentTerm >= 1, got %v", result["currentTerm"])
	}
}

func TestTransport_FollowerHTTPForwarding(t *testing.T) {
	tc := newTransportTestCluster(t, []string{"node1", "node2", "node3"})

	leader, err := tc.waitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	followers := tc.followers(leader)
	if len(followers) == 0 {
		t.Fatal("no followers")
	}
	followerHTTP := tc.httpAddrs[followers[0]]

	// Wait for the follower to learn who the leader is via heartbeats.
	time.Sleep(500 * time.Millisecond)

	// Write through a follower's HTTP endpoint — should be forwarded to leader.
	resp, err := httpPostJSON(followerHTTP, "/api/kv", map[string]string{
		"key": "animal", "value": "cat",
	})
	if err != nil {
		t.Fatalf("HTTP POST to follower: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from follower forwarding, got %d: %s", resp.StatusCode, readBody(resp))
	}
	resp.Body.Close()

	// Read through the follower — should also be forwarded to leader.
	resp, err = httpGet(followerHTTP, "/api/kv", url.Values{"key": {"animal"}})
	if err != nil {
		t.Fatalf("HTTP GET from follower: %v", err)
	}
	result := parseJSON(resp)
	if result["found"] != true {
		t.Fatalf("expected found=true from follower read, got %v", result)
	}
	if result["value"] != "cat" {
		t.Fatalf("expected cat, got %v", result["value"])
	}
}

func TestTransport_HTTPBadRequest(t *testing.T) {
	tc := newTransportTestCluster(t, []string{"node1", "node2", "node3"})

	leader, err := tc.waitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}
	leaderHTTP := tc.httpAddrs[leader]

	// Missing key on GET.
	resp, err := httpGet(leaderHTTP, "/api/kv", nil)
	if err != nil {
		t.Fatalf("GET /api/kv: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing key: expected 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Invalid JSON on POST.
	u := fmt.Sprintf("http://%s/api/kv", leaderHTTP)
	resp, err = httpClient.Post(u, "application/json", strings.NewReader("{invalid"))
	if err != nil {
		t.Fatalf("POST invalid JSON: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid JSON: expected 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Empty key on POST.
	resp, err = httpPostJSON(leaderHTTP, "/api/kv", map[string]string{
		"key": "", "value": "test",
	})
	if err != nil {
		t.Fatalf("POST empty key: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty key: expected 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestTransport_HTTPBodyLimit(t *testing.T) {
	tc := newTransportTestCluster(t, []string{"node1", "node2", "node3"})

	leader, err := tc.waitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}
	leaderHTTP := tc.httpAddrs[leader]

	// Send body exceeding 1MB.
	bigBody := strings.NewReader(strings.Repeat("x", 2*1024*1024))
	u := fmt.Sprintf("http://%s/api/kv", leaderHTTP)
	resp, err := httpClient.Post(u, "application/json", bigBody)
	if err != nil {
		t.Fatalf("POST big body: %v", err)
	}
	// Should be rejected — either 400 or 413.
	if resp.StatusCode == http.StatusOK {
		t.Error("expected rejection for oversized body, got 200")
	} else {
		t.Logf("oversized body correctly rejected with status %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestTransport_GRPCConnectionFailure(t *testing.T) {
	// Create a client pointing to an address where nothing is listening.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadAddr := lis.Addr().String()
	lis.Close() // Close immediately — nothing will be listening.

	client, err := rpc.NewPeerGRPCClient(rpc.PeerGRPCClientConfig{
		Endpoint: deadAddr,
	})
	if err != nil {
		t.Fatalf("NewPeerGRPCClient: %v", err)
	}
	defer client.Close()

	// Attempt an RPC with a short timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, rpcErr := client.Raft.AppendEntries(ctx, &konsen.AppendEntriesReq{})
	if rpcErr == nil {
		t.Fatal("expected error when connecting to dead address")
	}
	t.Logf("gRPC connection failure handled correctly: %v", rpcErr)
}
