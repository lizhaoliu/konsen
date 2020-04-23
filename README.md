# konsen
### Generate code
```shell script
protoc -I=proto --go_out=plugins=grpc:proto_gen proto/*.proto
```