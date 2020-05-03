# konsen
### Overview
Konsen is an implementation of [Raft](https://raft.github.io/raft.pdf) consensus protocol based upon replicated state
machine. Konson offers strong consistency and partition tolerance. For a cluster of N nodes, it can tolerate up to N/2
failures.
### Features
- [x] Consensus with replicated state machine.
- [x] Leader election.
- [x] Distributed key-value store.
- [ ] Distributed transactions.
### Build Local Cluster
#### Cluster configuration
Edit the cluster config in `conf/cluster.yml`, for example: 
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
This creates a cluster of 5 nodes, where "httpServers" define the HTTP servers that accept client request such as writes
and reads.
#### (Optional) Regenerate protobuf code
```shell script
protoc -I=proto --go_out=plugins=grpc:proto_gen proto/*.proto
```
#### Build cluster
```shell script
python build_cluster.py
```
This will build a release for each node in the cluster config. 
```
output/
  node1/
    bootstrap.sh
    cluster.yml
    konsen
  node2/
    bootstrap.sh
    cluster.yml
    konsen
  node3/
    bootstrap.sh
    cluster.yml
    konsen
  ...
```
### Benchmark
#### Setup
* go version go1.14.2 linux/amd64.
* 5 nodes on local machine, AMD 3900x 12 cores + 32GB Ram + 256GB SSD.
* Ubuntu 19.10.
* Benchmark tool: [Vegeta](https://github.com/tsenart/vegeta).
#### Write 
All write requests will be redirected to the leader node: leader writes to local log and then replicates it to all (at
least majority of) nodes such that it applies to local state machine, and responds to the request.
```shell script
Requests      [total, rate, throughput]  2500, 500.21, 495.11
Duration      [total, attack, wait]      5.049347643s, 4.997898254s, 51.449389ms
Latencies     [mean, 50, 95, 99, max]    82.190365ms, 79.205946ms, 113.411489ms, 126.342645ms, 154.046802ms
Bytes In      [total, mean]              0, 0.00
Bytes Out     [total, mean]              37500, 15.00
Success       [ratio]                    100.00%
Status Codes  [code:count]               200:2500
```
#### Read
All read requests will be redirected to the leader node.
```shell script
Requests      [total, rate, throughput]  50000, 9994.47, 9990.42
Duration      [total, attack, wait]      5.004796803s, 5.002768278s, 2.028525ms
Latencies     [mean, 50, 95, 99, max]    484.303µs, 218.459µs, 662.541µs, 9.176152ms, 42.321458ms
Bytes In      [total, mean]              0, 0.00
Bytes Out     [total, mean]              0, 0.00
Success       [ratio]                    100.00%
Status Codes  [code:count]               200:50000  
```
### TODO
- [ ] Unit tests.
- [ ] Supervisors.
- [ ] Log compaction.
- [ ] Cluster resize.
- [ ] HTTP server.
- [ ] CI/CD pipeline.
