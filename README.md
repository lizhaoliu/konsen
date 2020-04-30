# konsen
### Overview
konsen is an implementation of [Raft](https://raft.github.io/raft.pdf) consensus protocol based upon replicated state
machine. konson offers strong consistency and partition tolerance. For a cluster of N nodes, it can tolerate up to N/2
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
endpoints:
  - 192.168.86.25:10001
  - 192.168.86.25:10002
  - 192.168.86.25:10003
```
This creates a cluster of 3 nodes.
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
  node_0/
    bootstrap.sh
    cluster.yml
    konsen
  node_1/
    bootstrap.sh
    cluster.yml
    konsen
  node_2/
    bootstrap.sh
    cluster.yml
    konsen
  ...
```
#### Run (key-value store example)
Start a cluster of 5 nodes, and one becomes a leader
```
INFO[2020-04-29T21:54:57-07:00] Cluster endpoints: ["192.168.86.25:10001" "192.168.86.25:10002" "192.168.86.25:10003" "192.168.86.25:10004" "192.168.86.25:10005"] 
INFO[2020-04-29T21:54:57-07:00] BoltDB storage file set to: ...
INFO[2020-04-29T21:54:57-07:00] Start konsen server on: "192.168.86.25:10003" 
INFO[2020-04-29T21:54:59-07:00] Term - 5, leader - "192.168.86.25:10003".
```
Set a key-value from a non-leader node
```
INFO[2020-04-29T21:54:59-07:00] Cluster endpoints: ["192.168.86.25:10001" "192.168.86.25:10002" "192.168.86.25:10003" "192.168.86.25:10004" "192.168.86.25:10005"] 
INFO[2020-04-29T21:54:59-07:00] BoltDB storage file set to: ...
INFO[2020-04-29T21:54:59-07:00] Start konsen server on: "192.168.86.25:10005" 
» set name=Lizhao Liu
```
Get value by key from another node
```
INFO[2020-04-29T21:54:58-07:00] Cluster endpoints: ["192.168.86.25:10001" "192.168.86.25:10002" "192.168.86.25:10003" "192.168.86.25:10004" "192.168.86.25:10005"] 
INFO[2020-04-29T21:54:58-07:00] BoltDB storage file set to: ...
INFO[2020-04-29T21:54:58-07:00] Start konsen server on: "192.168.86.25:10004" 
» get name
INFO[2020-04-29T21:55:43-07:00] Lizhao Liu 
```
### TODO
- [ ] Unit tests.
- [ ] Log compaction.
- [ ] Cluster resize.
- [ ] HTTP server.
- [ ] CI/CD pipeline.
