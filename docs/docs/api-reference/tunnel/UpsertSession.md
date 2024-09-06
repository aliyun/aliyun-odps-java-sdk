---
title: UpsertSession
sidebar_position: 4
---

# UpsertSession

## 概述

`UpsertSession` 接口用于管理和操作数据插入或更新的会话。通过该接口，用户可以获取会话的相关信息、提交或中止会话、创建新的记录对象等。

## 初始化
通常由[TableTunnel#buildUpsertSession](TableTunnel.md#初始化upsertsession)方法创建 Builder，
[Builder接口](UpsertSession.md#builder-接口)的build方法来构建 UpsertSession。

## 方法
### 获取Session ID

```java
String getId();
```

返回当前会话的ID，可以基于这个ID进行重建会话操作。

### 获取当前 Quota

```java
public String getQuotaName();
```

返回当前使用的Quota名称，注意不是QuotaNickName。

### 获取Session状态

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

### 获取表结构

```java
TableSchema getSchema();
```

返回当前会话写入的表的表结构。

### 提交UpsertSession

```java
void commit(boolean async) throws TunnelException;
```

提交当前会话。可以选择是否异步提交。
当选择异步提交时，提交操作像服务端发送提交请求后立即返回，不会等待数据提交完成，这也意味着数据不会立即可见。

### 中止UpsertSession

```java
void abort() throws TunnelException;
```

中止当前会话。

### 清理客户端本地资源

```java
void close();
```

清理客户端本地资源。

### 创建一个Record对象

```java
Record newRecord();
```

创建并返回一个新的 `Record` 对象。

这个`Record`是`UpsertRecord`的实例，这个实例包含执行upsert操作必要的一些信息。
因此当使用Upsert操作时，永远记得使用这个方法，来获取`Record`的实例。

### 构建UpsertStream

```java
UpsertStream.Builder buildUpsertStream();
```

返回一个用于构建 `UpsertStream` 的 `Builder` 对象。

`UpsertStream` 是用于执行数据插入或更新操作的核心接口，详见 [UpsertStream 接口文档](UpsertStream.md)。

## Builder 接口

`UpsertSession.Builder` 接口用于构建 `UpsertSession` 对象。

通常由[TableTunnel#buildUpsertSession](TableTunnel.md#初始化upsertsession) 方法创建。

### 获取和设置Upsert ID

```java
String getUpsertId();
UpsertSession.Builder setUpsertId(String upsertId);
```

获取和设置 `Upsert ID`。

### 获取和设置Schema名称

```java
String getSchemaName();
UpsertSession.Builder setSchemaName(String schemaName);
```

获取和设置 `Schema` 名称。

### 获取和设置PartitionSpec

```java
String getPartitionSpec();
UpsertSession.Builder setPartitionSpec(PartitionSpec spec);
UpsertSession.Builder setPartitionSpec(String spec);
```

获取和设置 `PartitionSpec`。

### 获取和设置SlotNum

```java
long getSlotNum();
UpsertSession.Builder setSlotNum(long slotNum);
```

获取和设置 `SlotNum`。

### 获取和设置CommitTimeout

```java
long getCommitTimeout();
UpsertSession.Builder setCommitTimeout(long commitTimeoutMs);
```

获取和设置提交超时时间（毫秒）。

### 设置Netty进行网络IO的线程数

```java
UpsertSession.Builder setNetworkThreadNum(int threadNum);
```

设置Netty进行网络IO的线程池（EventLoop）的线程数，默认为1。

### 设置最大并发数

```java
UpsertSession.Builder setConcurrentNum(int concurrentNum);
```

设置最大并发数（允许同时存在的Channel数量），默认为20，设为小于0为无限制。

### 设置连接超时时间

```java
UpsertSession.Builder setConnectTimeout(long timeout);
```

设置建立连接的超时时间（毫秒），默认为180 * 1000。

### 设置请求响应超时时间

```java
UpsertSession.Builder setReadTimeout(long timeout);
```

设置请求响应的超时时间（毫秒），默认为300 * 1000。

### 获取和设置Session生命周期

```java
long getLifecycle();
UpsertSession.Builder setLifecycle(long lifecycle);
```

获取和设置Session生命周期（小时），有效取值范围为1 - 24，指定有效取值范围以外的值该参数会被忽略，使用服务端默认值。

### 构建UpsertSession

```java
UpsertSession build() throws TunnelException, IOException;
```

构建并返回一个 `UpsertSession` 对象。

## 总结

通过 `UpsertSession` 接口和其 `Builder`，用户可以方便地管理数据插入或更新的会话，获取会话信息，提交或中止会话，以及创建新的记录对象。