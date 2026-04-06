# konsen

Konsen is a [Raft](https://raft.github.io/raft.pdf) consensus protocol implementation in Go, built on a replicated state machine architecture. It provides strong consistency and partition tolerance — a cluster of N nodes can tolerate up to N/2 failures.

## Features

- **Leader election** with randomized timeouts
- **Log replication** with follower consistency checks
- **Distributed key-value store** with linearizable reads and writes
- **Actor-model concurrency** — all mutable state accessed by a single message loop goroutine, no explicit locks
- **Pluggable storage backends** — Badger DB, BoltDB, and in-memory (for testing)
- **gRPC transport** for inter-node communication
- **HTTP REST API** for client access
- **Configurable election timeouts and heartbeat intervals**

## Getting Started

### Install

```bash
go get github.com/lizhaoliu/konsen/v2
```

### Programmatic Usage

```go
import (
    "github.com/lizhaoliu/konsen/v2/core"
    "github.com/lizhaoliu/konsen/v2/datastore"
    "github.com/lizhaoliu/konsen/v2/rpc"
)

// Create storage.
storage, _ := datastore.NewBadger(datastore.BadgerConfig{
    LogDir:   "db/logs",
    StateDir: "db/state",
})
defer storage.Close()

// Create gRPC clients to remote nodes.
clients := make(map[string]*core.PeerClient)
for name, endpoint := range cluster.Servers {
    if name != cluster.LocalServerName {
        clients[name], _ = rpc.NewPeerGRPCClient(
            rpc.PeerGRPCClientConfig{Endpoint: endpoint},
        )
    }
}

// Create and start the state machine.
sm, _ := core.NewStateMachine(core.StateMachineConfig{
    Storage: storage,
    Cluster: cluster,
    Clients: clients,
    // Optional: override default timeouts.
    MinTimeout:  1000 * time.Millisecond,
    TimeoutSpan: 1000 * time.Millisecond,
    Heartbeat:   100 * time.Millisecond,
})
go sm.Run(ctx)
defer sm.Close()

// Write key-value pairs.
sm.SetKeyValue(ctx, &konsen.KVList{
    KvList: []*konsen.KV{
        {Key: []byte("key"), Value: []byte("value")},
    },
})

// Read a value (forwarded to the leader if this node is a follower).
value, _ := sm.GetValue(ctx, []byte("key"))
```

### HTTP API

The HTTP server (built with Gin) exposes the following endpoints:

```bash
# Write key-value pairs (non-leaders forward to the leader).
curl -X POST http://localhost:20001/konsen -d "key1=value1&key2=value2"

# Read a value (leader-only).
curl "http://localhost:20001/konsen?key=key1"

# Health check — returns 200 if the node's message loop is responsive.
curl http://localhost:20001/health

# Readiness check — returns 200 if a leader is known.
curl http://localhost:20001/ready
```

## Deployment

### Cluster Configuration

`servers` defines gRPC endpoints for inter-node Raft RPCs. `httpServers` defines HTTP endpoints that accept client reads and writes. The cluster must have an odd number of nodes.

### Docker Compose

Runs a 3-node cluster on a single host:

```bash
docker compose up -d
```

The HTTP API is available at `localhost:20001`, `localhost:20002`, and `localhost:20003`. Node configs are in `conf/docker/`.

### Kubernetes (k3s)

Deploys a 3-node StatefulSet with stable DNS identities and persistent storage. An init container injects each pod's name as `localServerName`, so a single ConfigMap covers all nodes.

Two manifest settings are critical for Raft bootstrapping:

- **`podManagementPolicy: Parallel`** (StatefulSet) — all pods must start simultaneously so the cluster can form a quorum and elect a leader. The default `OrderedReady` policy would deadlock because each pod waits for the previous one to become ready.
- **`publishNotReadyAddresses: true`** (headless Service) — pods must resolve each other's DNS names before they're ready, otherwise they can't communicate to hold an election.

```bash
# Build and push the image to your container registry
docker build -t <registry>/konsen:latest .
docker push <registry>/konsen:latest

# Update the image field in conf/k8s/statefulset.yml to match

# Apply manifests
kubectl apply -f conf/k8s/namespace.yml
kubectl apply -f conf/k8s/configmap.yml
kubectl apply -f conf/k8s/service.yml
kubectl apply -f conf/k8s/statefulset.yml
```

The HTTP API is exposed via NodePort at `<node-ip>:30001`. Pods use the `local-path` storage class (k3s default) for persistent volumes.

Manifests are in `conf/k8s/`.

### Local (bare-metal)

Edit `conf/cluster.yml`:

```yaml
servers:
  node1: 192.168.86.25:10001
  node2: 192.168.86.25:10002
  node3: 192.168.86.25:10003
  node4: 192.168.86.25:10004
  node5: 192.168.86.25:10005
httpServers:
  node1: 192.168.86.25:20001
  node2: 192.168.86.25:20002
  node3: 192.168.86.25:20003
  node4: 192.168.86.25:20004
  node5: 192.168.86.25:20005
```

Build and run:

```bash
python build_cluster.py
```

This builds a release for each node:

```text
output/
  node1/
    bootstrap.sh
    cluster.yml
    konsen
  node2/
    ...
```

Start each node by running its `bootstrap.sh`.

### (Optional) Regenerate Protobuf Code

```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/*.proto
```

## Architecture

```text
Client ─── HTTP API ─── StateMachine ─── gRPC ─── Remote Nodes
                              │
                          Storage
                        (Badger/BoltDB)
```

See [docs/design.md](docs/design.md) for the full design document covering the Raft implementation, concurrency model, storage layer, and safety properties.

### Storage Backends

| Backend        | Use Case                                                            |
| -------------- | ------------------------------------------------------------------- |
| **Badger DB**  | Production — high-performance, uses separate DBs for logs and state |
| **BoltDB**     | Alternative production backend — single-file, uses three buckets    |
| **In-memory**  | Testing only                                                        |

### Default Timing

| Parameter                      | Default |
| ------------------------------ | ------- |
| Election timeout (min)         | 1000ms  |
| Election timeout (random span) | 1000ms  |
| Heartbeat interval             | 100ms   |

## Testing

```bash
# Run all tests.
go test ./...

# Run integration tests only.
go test -run TestIntegration -v

# Run unit tests for a specific package.
go test ./core/ -v
go test ./datastore/ -v
```

Integration tests spin up in-process 3-node clusters with fast election timeouts (150ms) and simulate network partitions.

## Benchmark

**Setup:** 5 nodes on a local machine, AMD 3900x 12 cores, 32GB RAM, 256GB SSD. Benchmark tool: [Vegeta](https://github.com/tsenart/vegeta).

**Write** — all writes are forwarded to the leader, which replicates to a majority before responding:

```text
Requests      [total, rate, throughput]  2500, 500.21, 495.11
Duration      [total, attack, wait]      5.049s, 4.998s, 51.449ms
Latencies     [mean, 50, 95, 99, max]    82.19ms, 79.21ms, 113.41ms, 126.34ms, 154.05ms
Success       [ratio]                    100.00%
```

**Read** — reads are forwarded to the leader (clients can contact any node):

```text
Requests      [total, rate, throughput]  50000, 9994.47, 9990.42
Duration      [total, attack, wait]      5.005s, 5.003s, 2.029ms
Latencies     [mean, 50, 95, 99, max]    484.30µs, 218.46µs, 662.54µs, 9.18ms, 42.32ms
Success       [ratio]                    100.00%
```

## Roadmap

- [ ] Log compaction / snapshotting
- [ ] Dynamic cluster membership changes
- [ ] Distributed transactions
- [ ] TLS for gRPC and HTTP
- [ ] CI/CD pipeline
