---
title: Upsert
sidebar_position: 5
---
对于 Transactional 表，用户可以使用 `UpsertSession` 来执行数据插入或更新操作。

## 目录
我们通过两个核心类，来实现完整的，可支撑高并发的 Upsert 操作，并提供了一个完整的使用示例。
* [UpsertSession](#upsertsession)
* [UpsertStream](#upsertstream)
* [使用示例](#使用示例)
* [注意事项](#注意事项)

## UpsertSession

### 概述

`UpsertSession` 接口用于管理和操作数据插入或更新的会话。通过该接口，用户可以获取会话的相关信息、提交或中止会话、创建新的记录对象等。

### 初始化
通常由[TableTunnel#buildUpsertSession](TableTunnel.md#初始化upsertsession)方法创建 Builder，
[Builder接口](UpsertSession.md#builder-接口)的build方法来构建 UpsertSession。

### 方法
#### 获取Session ID

```java
String getId();
```

返回当前会话的ID，可以基于这个ID进行重建会话操作。

#### 获取当前 Quota

```java
public String getQuotaName();
```

返回当前使用的Quota名称，注意不是QuotaNickName。

#### 获取Session状态

```java
String getStatus() throws TunnelException;
```

返回当前会话的状态码，可能的状态包括：
- normal 正常
- committing 提交中
- committed 已提交
- expired 过期
- critical 错误
- aborted 已中止

#### 获取表结构

```java
TableSchema getSchema();
```

返回当前会话写入的表的表结构。

#### 提交UpsertSession

```java
void commit(boolean async) throws TunnelException;
```

提交当前会话。可以选择是否异步提交。
当选择异步提交时，提交操作像服务端发送提交请求后立即返回，不会等待数据提交完成，这也意味着数据不会立即可见。

#### 中止UpsertSession

```java
void abort() throws TunnelException;
```

中止当前会话。

#### 清理客户端本地资源

```java
void close();
```

清理客户端本地资源。

#### 创建一个Record对象

```java
Record newRecord();
```

创建并返回一个新的 `Record` 对象。

这个`Record`是`UpsertRecord`的实例，这个实例包含执行upsert操作必要的一些信息。
因此当使用Upsert操作时，永远记得使用这个方法，来获取`Record`的实例。

#### 构建UpsertStream

```java
UpsertStream.Builder buildUpsertStream();
```

返回一个用于构建 `UpsertStream` 的 `Builder` 对象。

`UpsertStream` 是用于执行数据插入或更新操作的核心接口，详见 [UpsertStream](#UpsertStream)。

### Builder 接口

`UpsertSession.Builder` 接口用于构建 `UpsertSession` 对象。

通常由[TableTunnel#buildUpsertSession](TableTunnel.md#初始化upsertsession) 方法创建。

#### 获取和设置Upsert ID

```java
String getUpsertId();
UpsertSession.Builder setUpsertId(String upsertId);
```

获取和设置 `Upsert ID`。

#### 获取和设置Schema名称

```java
String getSchemaName();
UpsertSession.Builder setSchemaName(String schemaName);
```

获取和设置 `Schema` 名称。

#### 获取和设置PartitionSpec

```java
String getPartitionSpec();
UpsertSession.Builder setPartitionSpec(PartitionSpec spec);
UpsertSession.Builder setPartitionSpec(String spec);
```

获取和设置 `PartitionSpec`。

#### 获取和设置SlotNum

```java
long getSlotNum();
UpsertSession.Builder setSlotNum(long slotNum);
```

获取和设置 `SlotNum`。

#### 获取和设置CommitTimeout

```java
long getCommitTimeout();
UpsertSession.Builder setCommitTimeout(long commitTimeoutMs);
```

获取和设置提交超时时间（毫秒）。

#### 设置Netty进行网络IO的线程数

```java
UpsertSession.Builder setNetworkThreadNum(int threadNum);
```

设置Netty进行网络IO的线程池（EventLoop）的线程数，默认为1。

#### 设置最大并发数

```java
UpsertSession.Builder setConcurrentNum(int concurrentNum);
```

设置最大并发数（允许同时存在的Channel数量），默认为20，设为小于0为无限制。

#### 设置连接超时时间

```java
UpsertSession.Builder setConnectTimeout(long timeout);
```

设置建立连接的超时时间（毫秒），默认为180 * 1000。

#### 设置请求响应超时时间

```java
UpsertSession.Builder setReadTimeout(long timeout);
```

设置请求响应的超时时间（毫秒），默认为300 * 1000。

#### 获取和设置Session生命周期

```java
long getLifecycle();
UpsertSession.Builder setLifecycle(long lifecycle);
```

获取和设置Session生命周期（小时），有效取值范围为1 - 24，指定有效取值范围以外的值该参数会被忽略，使用服务端默认值。

#### 构建UpsertSession

```java
UpsertSession build() throws TunnelException, IOException;
```

构建并返回一个 `UpsertSession` 对象。

### 总结

通过 `UpsertSession` 接口和其 `Builder`，用户可以方便地管理数据插入或更新的会话，获取会话信息，提交或中止会话，以及创建新的记录对象。



## UpsertStream

### 概述

在当前版本，`UpsertStreamImpl` 类是 `UpsertStream`接口唯一的实现类，因此本文主要介绍`UpsertStreamImpl`
的类接口。
通过该类，用户可以对`Delta Table`进行数据的插入、更新、删除操作，并支持数据的缓冲和批量提交。

### 构造方法

#### UpsertStreamImpl(Builder builder)

```java
public UpsertStreamImpl(Builder builder)throws IOException,TunnelException
```

通过 `Builder` 对象构建 `UpsertStreamImpl`
实例的构造方法，通常由 [`UpsertSession#buildUpsertStream()`](UpsertSession.md#构建upsertstream) 方法调用。

### 方法

#### 插入或更新记录

```java
@Override
public void upsert(Record record)throws IOException,TunnelException
```

插入或更新一条记录。

参数列表：
record：**注：参数`record`应当是`UpsertRecord`的实例**
，这个实例可以通过 [`UpsertSession#newRecord()`](UpsertSession.md#创建一个record对象) 方法创建。

:::info
`UpsertRecord`内部维护了一些元数据隐藏列，因此用户不应当手动创建`ArrayRecord`对象将其传入。
:::

#### 插入或更新部分列

```java
@Override
public void upsert(Record record,List<String> upsertCols)throws IOException,TunnelException
```

插入或更新一条记录的部分列。

record : `UpsertRecord`
的实例，这个实例可以通过 [`UpsertSession#newRecord()`](UpsertSession.md#创建一个record对象) 方法创建。

upsertCols : 一个包含要更新的列名的列表。

#### 删除记录

```java
@Override
public void delete(Record record)throws IOException,TunnelException
```

删除一条记录。

record : `UpsertRecord`
的实例，这个实例可以通过 [`UpsertSession#newRecord()`](UpsertSession.md#创建一个record对象) 方法创建。

#### 刷新缓冲区

```java
@Override
public void flush()throws IOException,TunnelException
```

刷新缓冲区，将缓冲区中的数据提交到服务器。

#### 关闭流

```java
@Override
public void close()throws IOException,TunnelException
```

关闭流，提交所有未提交的数据。

#### 重置缓冲区

```java
@Override
public void reset()throws IOException
```

重置缓冲区，清空所有未提交的数据。

### 重试逻辑

UpsertStream 通过用户传入 `Listener` 监听器来处理重试逻辑。当用户不传入 `Listener` 时，UpsertStream
将使用一个默认的 `Listener` 实例(`UpsertSessionImpl.DefaultUpsertSteamListener`)

这个默认的`Listener`实例，遵循`TableTunnel`默认的[重试逻辑](RetryLogic.md)。

:::tip
十分推荐当用户实现自定义`Listener`时，继承`UpsertSessionImpl.DefaultUpsertSteamListener`
，以使`TableTunnel`的重试逻辑生效。
:::


### Builder 类

`UpsertStreamImpl.Builder` 类是构建 `UpsertStreamImpl` 实例的基石，它提供了灵活的配置选项以便用户根据需求定制 `UpsertStream` 的行为。
此构造器主要通过 [`UpsertSession#buildUpsertStream()`](UpsertSession.md#构建upsertstream) 方法被调用，允许用户在创建 `UpsertStream` 时精确控制多个参数，
以优化数据插入或更新的性能和资源使用。

#### 方法详情

##### 设置会话对象

- **方法签名**:
  ```java
  public Builder setSession(UpsertSessionImpl session)
  ```
- **功能说明**:
  为 `UpsertStream` 指定关联的 `UpsertSessionImpl` 实例。这是UpsertSession#buildUpsertStream()的默认步骤，通常不需要用户手动调用。

##### 获取会话对象

- **方法签名**:
  ```java
  public UpsertSessionImpl getSession()
  ```
- **功能说明**:
  返回已设置的 `UpsertSessionImpl` 实例，供外部查询或验证使用。

##### 设置最大缓冲区大小

- **方法签名**:
  ```java
  public Builder setMaxBufferSize(long maxBufferSize)
  ```
- **功能说明**:
  允许用户设定数据缓冲的最大容量（单位：字节）。这直接影响到内存使用和批量写入的效率，较大的缓冲区可以减少I/O操作次数，但会占用更多内存。

##### 获取最大缓冲区大小

- **方法签名**:
  ```java
  public long getMaxBufferSize()
  ```
- **功能说明**:
  查询当前设置的最大缓冲区大小。

##### 设置槽缓冲区大小

- **方法签名**:
  ```java
  public Builder setSlotBufferSize(long slotBufferSize)
  ```
- **功能说明**:
  为每个桶设置缓冲区大小（单位：字节）。

##### 获取槽缓冲区大小

- **方法签名**:
  ```java
  public long getSlotBufferSize()
  ```
- **功能说明**:
  获取当前设置的每个桶的缓冲区大小。

##### 压缩选项配置

- **获取压缩选项**:
  ```java
  @Override public CompressOption getCompressOption()
  ```
- **设置压缩选项**:
  ```java
  @Override public Builder setCompressOption(CompressOption compressOption)
  ```
- **功能说明**:
  控制数据在传输前的压缩策略。通过选择合适的压缩算法，可以在减少网络传输成本和存储空间的同时，可能会增加一定的CPU使用。

##### 设置监听器

- **获取监听器**:
  ```java
  @Override public Listener getListener()
  ```
- **设置监听器**:
  ```java
  @Override public Builder setListener(Listener listener)
  ```
- **功能说明**:
  注册一个监听器，用于接收 `UpsertStream` 在数据写入过程中的事件通知，和重试逻辑的配置。UpsertSession#buildUpsertStream() 方法会为 UpsertStream 配置一个默认的 Listener 实例。详情参考[重试逻辑](#重试逻辑)


##### 构建 UpsertStream 实例

- **方法签名**:
  ```java
  @Override public UpsertStream build() throws IOException, TunnelException
  ```
- **功能说明**:
  根据之前设置的所有配置项构建并返回一个 `UpsertStream` 实例。

### 总结

通过 `UpsertStreamImpl` 类，用户可以方便地进行数据的插入、更新和删除操作，并支持数据的缓冲和批量提交。该类还提供了丰富的配置选项和错误处理机制，确保数据操作的高效和可靠。


## 使用示例

### 初始化 Upsert 会话
首先，您需通过 `TableTunnel` 的 `buildUpsertSession` 方法初始化一个 Upsert 会话，并配置必要的参数，如项目名、表名、模式名（如果适用）以及分区规格等。
```java
TableTunnel tunnel = odps.tableTunnel();
UpsertSession upsert = tunnel.buildUpsertSession(projectName, tableName)
    .setSchemaName(schemaName)
    .setPartitionSpec(partitionSpec)
    .build();
System.out.println("Upsert Session ID: " + upsert.getId());
```
若需要重新加载已有的 Upsert 会话，可以使用会话ID进行重建。
```java
if(reload) {
    String id = upsert.getId();
    upsert = tunnel.buildUpsertSession(projectName, tableName)
        .setSchemaName(schemaName)
        .setPartitionSpec(partitionSpec)
        .setUpsertId(id)
        .build();
}
```

### 执行 Upsert 操作
接下来，创建 `UpsertStream` 实例，并利用它执行具体的插入或更新记录操作。
```java
    UpsertStream stream = upsert.buildUpsertStream().build()
    Record record = upsert.newRecord();
    record.setString("key", "0");
    record.setString("value", "v1");
    stream.upsert(record); // 执行 Upsert
    stream.delete(record); // 删除记录示例
    stream.flush(); // 刷新到缓冲区
```

### 提交并关闭会话
```java
long startTime = System.currentTimeMillis();
upsert.commit(false);
long elapsedTime = System.currentTimeMillis() - startTime;
System.out.println("Upsert Session ID: " + upsert.getId() + ", Commit Time: " + elapsedTime + "ms");
upsert.close();
```

:::info
只有当 commit 方法被调用时，数据才可见。记得调用 close 方法，来进行资源清理。
:::

### 完整代码示例
下面是一个综合示例，展示了从初始化到执行 Upsert 操作的完整流程。
```java
import ...; // 导入必要的包

public class UpsertDeltaTableDemo {
  public static void main(String[] args) {
    try {
      Account account = new AliyunAccount("<Your_Access_ID>", "<Your_Access_Key>");
      Odps odps = new Odps(account);
      
      TableTunnel tunnel = odps.tableTunnel();
      String projectName = "<Your_Project_Name>";
      String tableName = "<Your_Table_Name>";
      try (TableTunnel.UpsertSession upsert = tunnel.buildUpsertSession(projectName, tableName)
          .setSchemaName("<Your_Schema_Name>")
          .setPartitionSpec("<Your_Partition_Spec>")
          .build()) {
        UpsertStream stream = upsert.buildUpsertStream().build();
        Record record = upsert.newRecord();
        record.setString("key", "exampleKey");
        record.setString("value", "exampleValue");
        stream.upsert(record);
        stream.flush();

        upsert.commit(false);
        System.out.println("Commit successful.");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
```

## 注意事项
- 由于主键表的写入特性，我们在并发写入同一张表（分区）时，应当谨慎地控制写入逻辑。如果存在多个并发，同时对同一主键进行写入，则可能发生不可预期的行为。
  常用的方案是使用 shuffle by pk 操作，将相同主键的记录分配到同一个线程中进行写入。
- 尽管 MaxCompute 服务端具备异步Compaction能力，UpsertSession 的 commit
  频率不应当过高（推荐commit间隔每分区不低于一分钟），否则会产生大量小文件，影响查询，还有可能触发服务端限流错误。
- 数据只有在 commit 后才可见。记得调用 close 方法，来进行资源清理。每个 Session 只能被 commit 一次，继续写入需要重建 Session。

