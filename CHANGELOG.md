# 0.21.3
- add retry logic in `TunnelRecordReader`
- add `TunnelBufferedWriter`
- add `InstanceTunnel` for downloading result of select statement
- default logview host change to logview.odps.aliyun-inc.com
- default connection timeout change from 5s to 10s
- add `Function.getProject`, `Volume.getProject` and `StreamJob.getProject`
- add `UploadSession.writeBlock`, `ProtobufRecordStreamWriter.write(RecordPack)` is deprecated
- add `InstanceTunnel`
- fix `Function.getResourceNames` returning wrong resource name
- return partition info in `PackReader.read`
- add `ServerTask` support

# 0.20.7
- security improvement
- fix pipeline combiner
- add ArrayRecord.clear()
- add onInstanceCreated hook
- array|map is supported in TableTunnel
- add volumefs sdk
- add Table.getTableID()

# 0.19.3
- fix bug of table tunnel download with columns
- fix DateUtils threads bug.
- add matrix sdk
- add volume resource sdk
- `StreamRecordPack.clear()` do not throw exception any more
- add `StreamClient.loadShard(long)`, deprecate `StreamClient.loadShard(int)`
- add `StreamClient.waitForShardLoad()`
- add `SQLTask.getResult`
- add `XFlows.getXFlowInstance`
- add `ArrayRecord(TableSchema)`
- add `CheckPermission`
- CopyTask support GroupAccount
 
# 0.18.0
- remove odps-sdk-ml(unused code)

# 0.17.8
- decimal range check in sdk
- support TableSchema set Columns
- Tables.create 支持comment

# 0.17.4
- fix domain account bug

# 0.17.2
- fix WritableRecord.getBytes

# 0.17.0
- fix get resource as streammulti
- tmp resource
- Get Project Clusters
- public CheckSum
- Table support if (not) exists

# 0.16.6
- project cluster API

# 0.16.5
- RecordPack memory size
