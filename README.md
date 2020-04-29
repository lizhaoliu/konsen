# konsen
### Introduction
konsen is an implementation of [Raft](https://raft.github.io/raft.pdf) consensus algorithm, that aims to be 
highly scalable, fast and minimal of size. For a cluster of N nodes, it can tolerate up to N/2 failures. 
### Features
- [x] Consensus with replicated state machine.
- [x] Distributed leader election.
- [ ] Distributed key-value storage.
- [ ] Distributed transactions.
### Build
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
```
### TODO
- [ ] Log compaction.
- [ ] Cluster resize.
- [ ] Tests, more tests.
- [ ] CI/CD pipeline.
- [ ] Easy automatic deployment.
