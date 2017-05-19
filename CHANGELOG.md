# 0.26.5
- remove jcabi, fix 0.26.2-public duplicated field bug

# 0.26.4
- fix mr secondary partition bug

# 0.26.2
- fastjson update to 1.2.25

# 0.26.0 
- add SQLTask.getResultSet
- copytask support new type
- mv xflow, xflows, ml from core-internal to core


# 0.25.2
- refactor Date io in tunnel
- refactor Java classes for new type
- fix graph classloader bug in sandbox

# 0.25.0
- add `Instance.getTaskInfo`
- add `SQLTASK.getSqlWarning`
- add `Instance.getTaskQuotaJson`
- specify 'Asia/Shanghai' to be default timezon in DateUtils
- add type info system and extend `OdpsType` to support more data type
- refactor tunnel sdk to support more data type

# 0.24.0
- improve sync instance
- support external table
- copy task support tunnel endpoint router 
- fix `user.reload` bug, mk it work
- optimize mr multi output
- add `BearerTokenAccount`

# 0.23.4
- always do reload in onlinemodel

# 0.23.3
- use utf-8 charset in event notification

# 0.23.2
- make `OdpsHooks` thread safe

# 0.23.1
- add `tables.loadTables` and `tables.reloadTables`

# 0.22.3                                                                            
- add `listRolesForUserName` and `listRolesForUserID`                               
- refine `OnlineModel`                                                              

# 0.22.2                                                                            
- fix mr get summary hang  

# 0.22.1
- revert instance retry, rm guid in job model

# 0.22.0 
- `SecurityConfiguration` support AclV2 and PackageV2
- add `Instance.getTaskCost`
- switch to fastjson for parsing json, almost
- remove dependency bouncycastle

# 0.21.2
- add retry logic in `TunnelRecordReader`
- add `TunnelBufferedWriter`
- add `InstanceTunnel` for downloading result of select statement
- default logview host change to logview.odps.aliyun-inc.com
- default connection timeout change from 5s to 10s

# 0.21.1 
- add `Function.getProject`, `Volume.getProject` and `StreamJob.getProject`
- add `UploadSession.writeBlock`, `ProtobufRecordStreamWriter.write(RecordPack)` is deprecated
- add `InstanceTunnel`
- fix `Function.getResourceNames` returning wrong resource name
- return partition info in `PackReader.read`
- add `ServerTask` support

# 0.20.7
- security improvement

# 0.20.1
- fix pipeline combiner

# 0.20.0
- add ArrayRecord.clear()
- add onInstanceCreated hook
- array|map is supported in TableTunnel
- add volumefs sdk
- add Table.getTableID()

# 0.19.3
- fix tunnel download with specified columns

# 0.19.2
- fix DateUtils threads bug.

# 0.19.1
- add matrix sdk

# 0.19.0
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
