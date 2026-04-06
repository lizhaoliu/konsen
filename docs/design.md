# Konsen Design Document

## 1. Overview

Konsen is a distributed key-value store built on the [Raft consensus protocol](https://raft.github.io/raft.pdf). It provides **strong consistency** (linearizable reads and writes) and **partition tolerance** for a cluster of N nodes, tolerating up to N/2 node failures.

Clients interact with the cluster over HTTP. Internally, nodes communicate via gRPC to replicate state. The core Raft state machine uses an actor-model concurrency design to guarantee thread-safe state transitions without locks.

## 2. Architecture

```
                          ┌────────────────────────────────────────────────────────────┐
                          │                        KONSEN NODE                         │
                          │                                                            │
  Client ──HTTP──────────>│  ┌──────────────────┐                                     │
  (GET/POST /konsen)      │  │   HTTP Server     │                                     │
                          │  │   (Gin)           │                                     │
                          │  └────────┬─────────┘                                     │
                          │           │                                                │
                          │           ▼                                                │
                          │  ┌──────────────────────────────────────────────────────┐  │
                          │  │               StateMachine (Actor Model)             │  │
                          │  │                                                      │  │
                          │  │   ┌────────────┐ ┌──────────────┐ ┌──────────────┐  │  │
                          │  │   │  Message    │ │  Election    │ │  Heartbeat   │  │  │
                          │  │   │  Loop       │ │  Loop        │ │  Loop        │  │  │
                          │  │   │ (main)      │ │ (timeout)    │ │ (leader only)│  │  │
                          │  │   └────────────┘ └──────────────┘ └──────────────┘  │  │
                          │  └───────────┬───────────────────────────┬──────────────┘  │
                          │              │                           │                  │
                          │              ▼                           ▼                  │
                          │  ┌──────────────────┐       ┌──────────────────────┐       │
  Other Nodes ──gRPC─────>│  │  gRPC Server     │       │  Storage Layer       │       │
                          │  │  (Raft RPCs)     │       │  (Badger / BoltDB)   │       │
                          │  └──────────────────┘       └──────────────────────┘       │
                          └────────────────────────────────────────────────────────────┘
```

### Component Summary

| Component | Package | Role |
|---|---|---|
| StateMachine | `core/` | Raft consensus logic, leader election, log replication, log application |
| Storage | `datastore/` | Persistent state (term, votedFor, logs) and KV data |
| gRPC Transport | `rpc/` | Inter-node Raft RPCs (AppendEntries, RequestVote) and KV RPCs (Put, Get) |
| HTTP Server | `web/httpserver/` | Client-facing REST API for reads and writes |
| Proto Definitions | `proto/` | Protocol Buffer message and service definitions |
| Bootstrap | `cmd/` | Server initialization and graceful shutdown |

## 3. Raft Protocol Implementation

The implementation follows the Raft paper faithfully. This section describes how each sub-protocol maps to code.

### 3.1 State

Every node maintains the following state, matching Raft Figure 2:

**Persistent (survives restarts) -- stored via `Storage` interface:**

| Field | Description |
|---|---|
| `currentTerm` | Latest term the server has seen (monotonically increasing) |
| `votedFor` | Candidate ID that received vote in the current term (empty if none) |
| `log[]` | Log entries, each containing an index, term, and data payload |

`currentTerm` and `votedFor` are always updated atomically via `SetTermAndVotedFor()` to prevent split-brain after a crash between two separate writes.

**Volatile (all servers):**

| Field | Description |
|---|---|
| `commitIndex` | Index of the highest log entry known to be committed |
| `lastApplied` | Index of the highest log entry applied to the state machine |

**Volatile (leaders only, reinitialized after each election):**

| Field | Description |
|---|---|
| `nextIndex[]` | For each follower, the next log index to send (initialized to leader's last log index + 1) |
| `matchIndex[]` | For each follower, the highest log index known to be replicated (initialized to 0) |

### 3.2 Leader Election

1. Each node starts as a **Follower**. An election timer fires after a randomized timeout of **1000-2000ms**.
2. If the timer expires without receiving a valid AppendEntries or granting a vote, the node becomes a **Candidate**.
3. The candidate increments its term, votes for itself (atomically persisted), and sends `RequestVote` RPCs to all other nodes.
4. A node grants a vote if: (a) the candidate's term is current, (b) the node hasn't already voted for someone else this term, and (c) the candidate's log is at least as up-to-date as the voter's.
5. A candidate becomes **Leader** upon receiving votes from a majority (`N/2 + 1`).
6. The leader immediately begins sending periodic heartbeats (AppendEntries with no entries) every **100ms** to maintain authority and prevent new elections.

**Log up-to-dateness** is determined by comparing last log terms first, then last log indices -- a higher term always wins; equal terms defer to the longer log.

### 3.3 Log Replication

1. Client write requests are serialized as `KVList` protobufs and appended to the leader's log.
2. The leader includes new entries in the next AppendEntries RPC to each follower, along with `prevLogIndex` and `prevLogTerm` for consistency checking.
3. Followers validate the consistency check:
   - If the local log at `prevLogIndex` has a different term, the RPC is rejected.
   - If a conflict is found (same index, different term), the follower deletes the conflicting entry and everything after it, then appends the new entries.
   - If the entry already exists with a matching term, it is skipped (idempotent).
4. On a successful response, the leader advances `nextIndex` and `matchIndex` for that follower.
5. On a failed response (log inconsistency), the leader decrements `nextIndex` and retries on the next heartbeat.
6. Once a log entry is replicated on a majority and its term matches the leader's current term, the leader advances `commitIndex`.
7. Each node applies committed-but-unapplied logs to the KV store in order.

**Safety property:** The leader only commits entries from its own term. Entries from prior terms are committed indirectly when a current-term entry after them is committed.

### 3.4 Follower Request Forwarding

When a non-leader node receives a client write, it forwards the `Put` RPC to the current leader via the `KVService` gRPC service and relays the response back to the client.

Read requests (`Get`) are also forwarded to the leader when received by a follower, ensuring linearizable reads regardless of which node the client contacts.

## 4. Concurrency Model: Actor Pattern

The `StateMachine` is designed around an **actor model** to avoid lock-based concurrency:

```
                  ┌──────────────────────────────────────────────┐
   gRPC Server ──>│                                              │
                  │           msgCh (unbuffered channel)         │
   HTTP Server ──>│   ─────────────────────────────────────────> │──> Message Loop
                  │                                              │    (single goroutine)
   Election    ──>│                                              │
   Loop           └──────────────────────────────────────────────┘
   Heartbeat   ──>
   Loop
```

**Key principle:** All mutable state (commitIndex, lastApplied, nextIndex, matchIndex, condMap, role transitions) is only accessed by the single message loop goroutine.

External callers (gRPC handlers, HTTP handlers) submit typed messages to `msgCh` with a buffered response channel, then block waiting for the reply. The message loop processes one message at a time, guaranteeing serialized access to state.

### Message Types

| Message | Source | Purpose |
|---|---|---|
| `appendEntriesWrap` | gRPC server | Incoming AppendEntries from another node |
| `appendEntriesRespWrap` | RPC goroutine | Response to an AppendEntries we sent |
| `requestVoteWrap` | gRPC server | Incoming RequestVote from a candidate |
| `*RequestVoteResp` | RPC goroutine | Response to a RequestVote we sent |
| `putMsg` | gRPC/HTTP | Client write request |
| `getValueMsg` | gRPC/HTTP | Client read request |
| `electionTimeoutMsg` | Election loop | Election timer expired |
| `appendEntriesMsg` | Heartbeat loop | Time to send heartbeats |
| `registerCondMsg` | Message loop | Register a waiter for log application |
| `unregisterCondMsg` | Waiter goroutine | Clean up waiter after log applied or timeout |
| `getSnapshotMsg` | Debug | Generate state snapshot |

### Goroutine Architecture

The state machine runs three long-lived goroutines:

1. **Message Loop** -- The core event processor. Reads from `msgCh`, dispatches to handlers, and calls `maybeApplyLogs()` after each message to apply newly committed entries.

2. **Election Loop** -- Manages the randomized election timer. Resets on `resetTimerCh` signals (sent when a valid heartbeat arrives or a vote is granted). On timeout, sends `electionTimeoutMsg` to the message loop. A double-select after the timer fires allows a concurrent reset to cancel the timeout, preventing spurious elections.

3. **Heartbeat Loop** (leader only) -- Created when a node becomes leader; destroyed when it steps down. Sends `appendEntriesMsg` every 100ms. Uses `atomic.Int32` for a safe concurrent role check.

### Client Write Flow

```
Client POST /konsen ──> HTTP Server
                            │
                            ▼
                     SetKeyValue() ──marshal KVList──> Put()
                            │
                            ▼
                     msgCh ──> Message Loop: handlePut()
                            │
                    ┌───────┴────────────────────┐
                    │ Follower?                   │ Leader?
                    │ Forward to leader via gRPC  │ writeToLogs() -> persist entry
                    │                             │ replyWhenLogApplied():
                    │                             │   register condMap[index]
                    │                             │   spawn waiter goroutine
                    │                             │
                    │                             │  ... heartbeat replicates ...
                    │                             │  ... majority reached ...
                    │                             │  ... commitIndex advances ...
                    │                             │  ... maybeApplyLogs() fires ...
                    │                             │  ... close(condCh) ...
                    │                             │
                    │                             ▼
                    └─────────────────────────> Response to client
```

## 5. Storage Layer

The `Storage` interface (`datastore/storage.go`) abstracts all persistence, splitting into two concerns:

1. **Raft state** -- term, votedFor, and log entries (required by the protocol).
2. **Application state** -- key-value pairs (the replicated state machine's output).

### Interface

```go
type Storage interface {
    // Raft persistent state
    GetCurrentTerm() (uint64, error)
    SetCurrentTerm(term uint64) error
    GetVotedFor() (string, error)
    SetVotedFor(candidateID string) error
    SetTermAndVotedFor(term uint64, candidateID string) error  // atomic

    // Log operations
    GetLog(logIndex uint64) (*konsen.Log, error)
    GetLogsFrom(minLogIndex uint64) ([]*konsen.Log, error)
    GetLogTerm(logIndex uint64) (uint64, error)
    WriteLog(log *konsen.Log) error
    WriteLogs(logs []*konsen.Log) error
    LastLogIndex() (uint64, error)
    LastLogTerm() (uint64, error)
    DeleteLogsFrom(minLogIndex uint64) error

    // Application state
    SetValue(key []byte, value []byte) error
    GetValue(key []byte) ([]byte, error)

    Close() error
}
```

### Implementations

**Badger (`datastore/badger_storage.go`)** -- The production storage backend. Uses two separate Badger databases:
- `logDB`: Stores Raft log entries keyed by their uint64 index (big-endian encoded).
- `stateDB`: Stores `currentTerm`, `votedFor`, and KV application data. KV keys are prefixed with `"kv:"` to avoid collisions with Raft state keys.

`SetTermAndVotedFor` writes both values in a single Badger transaction for crash safety.

**BoltDB (`datastore/boltdb_storage.go`)** -- An alternative backend using three buckets: `logs`, `states`, and `kv`. Offers the same transactional guarantees via BoltDB's MVCC.

## 6. gRPC Transport

### Protocol Buffers

Two gRPC services are defined across separate proto files:

**`proto/raft.proto`** -- The Raft consensus service with two RPCs:

```protobuf
service Raft {
  rpc AppendEntries(AppendEntriesReq) returns (AppendEntriesResp);
  rpc RequestVote(RequestVoteReq)     returns (RequestVoteResp);
}
```

**`proto/kv.proto`** -- The application-level KV service:

```protobuf
service KVService {
  rpc Put(PutReq) returns (PutResp);
  rpc Get(GetReq) returns (GetResp);
}
```

`AppendEntries` and `RequestVote` are the two core Raft RPCs. `Put` and `Get` are application-level RPCs that allow any node to accept client reads/writes and forward them to the leader.

A separate `konsen.proto` defines `KV` and `KVList` messages used as the log entry payload format.

### Service Interfaces (`core/raft_service.go`, `core/kv_service.go`)

```go
type RaftService interface {
    AppendEntries(ctx context.Context, in *AppendEntriesReq) (*AppendEntriesResp, error)
    RequestVote(ctx context.Context, in *RequestVoteReq) (*RequestVoteResp, error)
}

type KVService interface {
    Put(ctx context.Context, in *PutReq) (*PutResp, error)
    Get(ctx context.Context, in *GetReq) (*GetResp, error)
}
```

These interfaces decouple the state machine from the transport. The `StateMachine` holds a `map[string]*PeerClient` of clients -- one per remote node. Each `PeerClient` groups a `RaftService` and `KVService` over a shared connection. This makes it possible to swap transport implementations or inject mocks for testing.

### gRPC Server (`rpc/grpc_server.go`)

Wraps the `StateMachine` and implements both the `RaftServer` and `KVServiceServer` gRPC interfaces. Each incoming RPC is forwarded to the corresponding `StateMachine` method with a 10-second timeout.

### gRPC Client (`rpc/grpc_client.go`)

Implements both `RaftService` and `KVService` over a single shared gRPC connection, returned as a `PeerClient`. Uses `grpc.NewClient` for lazy connection establishment -- the connection is created on the first RPC call, not at client creation time. `WaitForReady(false)` is set on all calls to fail fast when a node is unreachable rather than blocking. gRPC keepalive is enabled for connection health checking.

## 7. HTTP API

The HTTP layer (`web/httpserver/server.go`) uses the Gin framework and exposes two endpoints:

### `GET /konsen?key=<key>`

Read a value. If the request reaches a follower, it is automatically forwarded to the current leader. This provides linearizable reads (the leader has the most up-to-date committed state) while allowing clients to contact any node.

### `POST /konsen`

Write key-value pairs. Accepts `application/x-www-form-urlencoded` body. Form fields are parsed as key-value pairs, serialized into a `KVList` protobuf, and submitted to the Raft log via `SetKeyValue()`. The request blocks until the entry is committed and applied, or times out.

## 8. Configuration

### Cluster Configuration (`conf/cluster.yml`)

```yaml
servers:
  node1: 192.168.4.86:10001    # gRPC endpoints for Raft RPCs
  node2: 192.168.4.86:10002
  node3: 192.168.4.86:10003
httpServers:
  node1: 192.168.4.86:20001    # HTTP endpoints for client API
  node2: 192.168.4.86:20002
  node3: 192.168.4.86:20003
```

Each node is started with a copy of this file that includes a `localServerName` field identifying which entry it corresponds to. The cluster enforces an **odd number of nodes** for clean majority quorum calculations.

### Server Startup Sequence (`cmd/main.go`)

1. Parse cluster config from YAML and database directory from flags.
2. Initialize Badger storage (separate directories for logs and state).
3. Create one gRPC client per remote node (lazy connections).
4. Construct `StateMachine` with storage, cluster config, and client map.
5. Start four goroutines in parallel:
   - gRPC server (listens for Raft RPCs)
   - `StateMachine.Run()` (message loop + election loop)
   - HTTP server (client API)
   - pprof debug server on `localhost:6060`
6. Wait for `SIGINT` or `SIGTERM`, then gracefully shut down all components.

### Shutdown Order

```
Signal received
  -> HTTP server shutdown (with 10s timeout for in-flight requests)
  -> StateMachine.Close() (signals all goroutines via stopCh, waits on WaitGroup)
  -> gRPC server graceful stop
  -> Close all gRPC clients
  -> Close storage
```

## 9. Timing Parameters

| Parameter | Value | Purpose |
|---|---|---|
| Election timeout | 1000-2000ms (randomized) | Prevents synchronized elections across nodes |
| Heartbeat interval | 100ms | Leader authority maintenance; must be << election timeout |
| RPC timeout (state machine) | 5s | Outgoing AppendEntries/RequestVote calls |
| RPC timeout (gRPC server) | 10s | Incoming RPC processing deadline |
| HTTP read/write timeout | 10s | Client request deadline |

## 10. Safety Properties

The implementation enforces the following Raft safety guarantees:

- **Election Safety:** At most one leader per term -- ensured by persisting `votedFor` and requiring majority votes.
- **Leader Append-Only:** The leader never overwrites or deletes its own log entries.
- **Log Matching:** If two logs contain an entry with the same index and term, the logs are identical up to that index -- enforced by the `prevLogIndex`/`prevLogTerm` consistency check.
- **Leader Completeness:** If a log entry is committed, it will be present in the logs of all future leaders -- enforced by the election restriction (candidates must have an up-to-date log to win votes).
- **State Machine Safety:** If a server has applied a log entry at a given index, no other server will apply a different entry at that index.
- **Current-Term Commit Rule:** The leader only advances `commitIndex` for entries whose term matches the leader's current term, preventing the "Figure 8" scenario from the Raft paper.
- **Atomic Term/VotedFor Persistence:** `SetTermAndVotedFor()` writes both fields in a single transaction to prevent split-brain after a crash between two writes.

## 11. Project Structure

```
konsen/
  cmd/
    main.go                     # Entry point, component wiring, graceful shutdown
  core/
    state_machine.go            # Raft state machine (actor model, ~1120 lines)
    raft_service.go             # RaftService interface definition
    kv_service.go               # KVService interface and PeerClient definition
    cluster.go                  # Cluster configuration parsing and validation
  datastore/
    storage.go                  # Storage interface
    badger_storage.go           # Badger DB implementation (production)
    boltdb_storage.go           # BoltDB implementation (alternative)
    utils.go                    # Encoding helpers and key constants
  proto/
    raft.proto                  # Raft consensus RPC definitions (AppendEntries, RequestVote)
    kv.proto                    # KV application RPC definitions (Put, Get)
    konsen.proto                # KV store payload definitions (KV, KVList)
    *.pb.go / *_grpc.pb.go      # Generated Go code
  rpc/
    grpc_server.go              # gRPC server wrapping StateMachine (Raft + KV services)
    grpc_client.go              # gRPC client implementing RaftService and KVService
  web/httpserver/
    server.go                   # HTTP REST API (Gin)
  conf/
    cluster.yml                 # Example cluster topology
  scripts/
    bootstrap.sh                # Per-node startup script
```

## 12. Known Limitations and Future Work

- **No log compaction / snapshotting:** The log grows unboundedly. Raft snapshots (Section 7 of the paper) are not yet implemented.
- **No cluster membership changes:** The cluster size is fixed at startup. Dynamic reconfiguration (Section 6 of the paper) is not supported.
- **No TLS:** gRPC connections use insecure credentials.
- **No unit tests:** Correctness relies on manual/integration testing.
- **No client request deduplication:** Retried writes could result in duplicate log entries.
