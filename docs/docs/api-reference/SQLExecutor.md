---
title: SQLExecutor
sidebar_position: 9
toc_max_heading_level: 4
---

## 概述

`SQLExecutor` 是 MaxCompute 为执行 SQL 提供的统一的接口，用户可以通过这一接口，提交离线作业，MCQA和MaxQA
作业，并提供了丰富的配置和方法，来满足用户多样的需求。

## 目录

1. [构建SQLExecutor实例](#构建SQLExecutor实例)
2. [执行SQL查询](#执行SQL查询)
3. [获取查询结果](#获取查询结果)
4. [其他接口](#其他接口)

## 构建SQLExecutor实例

odps-sdk 使用`SQLExecutorBuilder`类来构建`SQLExecutor`实例，`SQLExecutorBuilder`
采用构造者模式，提供了一系列方法来对`SQLExecutor`进行配置。
通过这些方法，用户可以设置执行模式、任务名称、服务名称等参数，通常每个参数都有默认值，用户可以按需使用。

### 构造器方法

使用`SQLExecutorBuilder.builder`方法可以构建一个`SQLExecutorBuilder`实例。调用`build`
方法可以构建`SQLExecutor`实例。全部配置项参考[配置选项](#配置选项)。

```java
public SQLExecutor build()throws OdpsException
```

**返回值**：

- `SQLExecutor`实例。

**示例代码**：

```java
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .taskName("my_task")
    .build();
```

### 配置选项

`SQLExecutorBuilder`类提供了多种配置选项，以下是各配置选项的详细说明。

#### 设置ODPS对象（必选）

使用`odps`
方法可以设置ODPS对象。构建ODPS对象的方式请参考[构建 ODPS 客户端](../core-concept/init-odps-client.md)。

```java
public SQLExecutorBuilder odps(Odps odps)
```

**参数**：

- `odps`：`Odps`对象。

#### 设置任务名称

使用`taskName`方法可以设置任务名称。对于 MCQA 作业，默认 taskName 为 `console_sqlrt_task`。
对于 MaxQA 作业，默认 taskName 为 `AnonymousMCQATask`。
对于离线作业，无法配置 taskName，会使用 `AnonymousSQLTask` 作为 taskName。

```java
public SQLExecutorBuilder taskName(String taskName)
```

**参数**：

- `taskName`：任务名称。

#### 设置Tunnel Endpoint

使用`tunnelEndpoint`方法可以设置Tunnel Endpoint。

这个接口常用于当用户网络环境受限，需要进行网络打通时，由odps endpoint自动路由的tunnel
endpoint无法直接访问，需要设置指定tunnel endpoint时使用。

```java
public SQLExecutorBuilder tunnelEndpoint(String tunnelEndpoint)
```

**参数**：

- `tunnelEndpoint`：Tunnel Endpoint。

#### 设置Quota名称

使用`quotaName`方法可以设置计算资源组的名称。
对于MaxQA的作业，此项为必填项，需要指定相应的交互式资源组名称，否则在创建`SQLExecutor`时将抛出异常。

```java
public SQLExecutorBuilder quotaName(String quotaName)
```

**参数**：

- `quotaName`：Quota名称（quota nick name）。

#### 设置执行模式

使用`executeMode`方法可以设置执行模式。

```java
public SQLExecutorBuilder executeMode(ExecuteMode executeMode)
```

**参数**：

- `executeMode`：执行模式。可选项为 1. `INTERACTIVE_V2`：MaxQA；2. `INTERACTIVE`：MCQA；3. `OFFLINE`：离线执行。

#### 使用 MaxQA

```java
public SQLExecutorBuilder enableMcqaV2(boolean mcqaV2)
```

- **参数**：`mcqaV2` - 布尔值，表示是否启用 MaxQA。

#### 启用命令API

使用`enableCommandApi`方法可以启用命令API。

`CommandApi` 是`SQLExecutor`
在标准SQL语法之外，额外提供的一组命令支持。用来执行像`desc table`,`list table`这样的非标准SQL命令。
目前 MaxCompute SQL 也在逐步支持类似扩展命令，因此当前版本部分扩展命令，开启这个选项后，行为会与不开启这个选项后不一致。

```java
public SQLExecutorBuilder enableCommandApi(boolean useCommandApi)
```

**参数**：

- `useCommandApi`：是否启用命令API。

#### 启用三层模型支持

使用`enableOdpsNamespaceSchema`方法可以启用ODPS命名空间三层模型（Schema）。

三层模型是 MaxCompute 推出的新模式，将原`project.table`扩展为为`project.schema.table`模式

```java
public SQLExecutorBuilder enableOdpsNamespaceSchema(boolean odpsNamespaceSchema)
```

**参数**：

- `odpsNamespaceSchema`：是否开启三层模型支持。

#### 是否启用 InstanceTunnel

在获取作业结果时，是否使用 instance tunnel 来取结果。默认值为`true`。

当不开启`instance tunnel`时，获取结果时的`Record`的中每条数据类型均为`String`
，忽略各列在ODPS中的真实类型，且会有最大长度限制（通常为10000），且无法指定`offset`，`limit`等信息。
但通常执行速度更快，用户可以根据使用场景选择是否关闭`instance tunnel`。

```java
public SQLExecutorBuilder useInstanceTunnel(boolean useInstanceTunnel)
```

- **参数**：`useInstanceTunnel` - 布尔值，表示是否启用 InstanceTunnel。

#### 设置InstanceTunnel取结果时的链接超时时间

```java
public SQLExecutorBuilder tunnelSocketTimeout(int tunnelSocketTimeout)
```

- **参数**：`tunnelSocketTimeout` - 整数，表示超时时间（毫秒）。

#### 设置InstanceTunnel取结果时的读取超时时间

```java
public SQLExecutorBuilder tunnelReadTimeout(int tunnelReadTimeout)
```

- **参数**：`tunnelReadTimeout` - 整数，表示超时时间（毫秒）。

#### 设置使用 InstanceTunnel 获取结果的最大重试次数

```java
public SQLExecutorBuilder tunnelGetResultMaxRetryTime(int tunnelGetResultMaxRetryTime)
```

- **参数**：`tunnelGetResultMaxRetryTime` - 整数，表示最大重试次数。

#### 配置当前使用的执行器池

通常由 `SQLExecutorPool` 自动调用

```java
SQLExecutorBuilder setPool(SQLExecutorPool pool)
```

- **参数**：`pool` - `SQLExecutorPool` 对象，表示执行器池。

#### 设置恢复实例

用来从指定实例恢复`SQLExecutor`。

对于`MCQA`作业，instance为`Session`实例，这项操作表示重新链接到给定的`Session`。
对于其他类型作业，instance为`SQLTask`实例，这项操作表示重新链接到给定的`SQLTask`。

```java
public SQLExecutorBuilder recoverFrom(Instance instance)
```

- **参数**：`instance` - `Instance` 对象，表示恢复的实例。

#### 设置离线作业的优先级

```java
public SQLExecutorBuilder offlineJobPriority(Integer offlineJobPriority)
```

- **参数**：`offlineJobPriority` - 整数，表示离线作业的优先级。

#### 加速失败回退策略（仅MCQA）

当执行失败时，是否回退到离线查询，采用什么策略回退。

```java
public SQLExecutorBuilder fallbackPolicy(FallbackPolicy fallbackPolicy)
```

- **参数**：`fallbackPolicy` - `FallbackPolicy` 对象，表示回退策略。

#### 启用或禁用重新连接Session功能（仅MCQA）

在MCQA模式下，当发生当前链接找不到`Session`时（可能是`Session`被停止或超时），是否启用重新连接功能。
`Session` 是MCQA独有的概念，详细信息可以参考[MCQA](../core-concept/execute-sql/mcqav1.md)。

```java
public SQLExecutorBuilder enableReattach(boolean enableReattach)
```

- **描述**：启用或禁用重新连接功能。
- **参数**：`enableReattach` - 布尔值，表示是否启用重新连接。
- **返回值**：`SQLExecutorBuilder` 实例。

#### 设置属性（仅MCQA）

配置创建MCQA Session时，使用的的`properties`。

```java
public SQLExecutorBuilder properties(Map<String, String> properties)
```

**参数**：

- `properties`：属性的键值对。

#### 设置服务名称（仅MCQA）

使用`serviceName`方法可以设置使用MCQA时，链接的`Session`名称。
`Session` 是MCQA独有的概念，详细信息可以参考[MCQA](../core-concept/execute-sql/mcqav1.md)。

```java
public SQLExecutorBuilder serviceName(String serviceName)
```

**参数**：

- `serviceName`：`Session`名称。

#### 允许MCQA执行非Select操作（仅MCQA）

当关闭这项选项时，MCQA作业在遇到非选择操作，会回退为离线作业。

```java
public SQLExecutorBuilder sessionSupportNonSelect(boolean sessionSupportNonSelect)
```

- **参数**：`sessionSupportNonSelect` - 布尔值，表示是否支持非选择操作。

#### 设置附加超时时间（仅MCQA）

使用`attachTimeout`方法可以设置当使用MCQA时，链接`Session`时的超时时间。`Session`
是MCQA独有的概念，详细信息可以参考[MCQA](../core-concept/execute-sql/mcqav1.md)。

```java
public SQLExecutorBuilder attachTimeout(Long timeout)
```

**参数**：

- `timeout`：超时时间。单位为毫秒。

#### 设置运行集群的名称（仅MCQA）

实际上，这个接口大部分情况下无用，因为用户没有手段得知运行集群的名称。通常仅作为内部排查问题使用。

```java
public SQLExecutorBuilder runningCluster(String runningCluster)
```

- **参数**：`runningCluster` - 字符串，表示运行集群的名称。

#### 设置Quota（仅MaxQA）

与[设置Quota名称](#设置quota名称)相比，这个方法可以配置一个已经获取好的Quota示例，这可以避免通过quotaName从服务端获取Quota的过程，使通过缓存Quota来提高性能成为可能。

```java
public SQLExecutorBuilder quota(Quota quota)
```

- **参数**：`quota` - Quota 实例，通过 `Quotas#getWlmQuota` 获得，或通过 `Quota#setMcqaConnHeader(String)` 方法加载。

#### 设置RegionId（仅MaxQA）

MaxQA 作业会通过 [设置Quota名称](#设置quota名称) 获取的 quotaName 来从服务端获取 Quota，这使用的是 Project 所在的 RegionId，
然而，我们允许用户通过这个方法，指定 Quota 所在的 RegionId，尽管像其他 region 的 Quota 提及作业有可能失败。
```java
public SQLExecutorBuilder regionId(String regionId)
```

- **参数**：`regionId` - 区域名，表示 Quota 所在的区域，通常不需要设置。

#### 跳过 SQL 解析（仅MCQA，离线作业）
默认为 false，当为 true 时，会跳过 select 语句的校验，提高极限性能。 可以在请求场景主要为查询语句时使用，在处理非查询语句时，处理延时会变长。
MaxQA 采用全新协议，无需跳过 SQL 解析。
```java
public SQLExecutorBuilder skipCheckIfSelect(boolean skipCheckIfSelect)
```

## 执行SQL查询

SQLExecutor 的 run 方法可以用来运行 SQL 查询。

### 方法

```java
void run(String sql, Map<String, String> hint) throws OdpsException;
```

**参数**:

- `sql`: SQL语句。
- `hint`: 查询所需的提示参数。比如：`odps.sql.type.system.odps2 = true`等。

### 示例

```java
SQLExecutor executor = ...; // 实例化 SQLExecutor
String sql = "SELECT * FROM table_name";
Map<String, String> hint = new HashMap<>();
executor.run(sql, hint);
```

## 获取查询结果

:::info
当关闭`instance tunnel`时，得到的`ArrayRecord`结果均为`String`
类型，忽略各列在ODPS中的真实类型，且会有最大长度限制（通常为10000），且指定`offset`，`limit`
等信息不为null时，会报错。
:::

### 获取结果列表

这组接口，返回的结果为一个`ArrayRecord`列表。程序会将结果全部读到内存里，所以如果结果量太大的话，可能会导致内存溢出。

```java
List<Record> getResult() throws OdpsException, IOException;

List<Record> getResult(Long countLimit) throws OdpsException, IOException;

List<Record> getResult(Long offset, Long countLimit, Long sizeLimit) throws OdpsException, IOException;

List<Record> getResult(Long offset, Long countLimit, Long sizeLimit, boolean limitEnabled) throws OdpsException, IOException;
```

**参数**：

- `countLimit`：结果数量限制。默认无限制，但仍然会受限于`limitEnabled`参数。
- `offset`：结果偏移量。默认值为0
- `countLimit`：结果数量限制。默认无限制
- `limitEnabled`
  ：是否拉取全部结果，默认为false，即结果会被限制为1w条。如果开启，则会拉取全部结果。拉取全部结果需要用户具有更高权限（具有sql所访问的所有源表的查询、下载权限）。

### 获取结果迭代器

这组接口，返回的结果为一个`ResultSet`迭代器，`ResultSet`实现了`Iterator<Record>`和`Iterable<Record>`。
程序会将结果分批读到内存里，减少了内存溢出的可能。

```java
ResultSet getResultSet() throws OdpsException, IOException;

ResultSet getResultSet(Long countLimit) throws OdpsException, IOException;

ResultSet getResultSet(Long offset, Long countLimit, Long sizeLimit) throws OdpsException, IOException;

ResultSet getResultSet(Long offset, Long countLimit, Long sizeLimit, boolean limitEnabled) throws OdpsException, IOException;
```

**参数**：

- `countLimit`：结果数量限制。默认无限制，但仍然会受限于`limitEnabled`参数。
- `offset`：结果偏移量。默认值为0
- `countLimit`：结果数量限制。默认无限制
- `limitEnabled`
  ：是否拉取全部结果，默认为false，即结果会被限制为1w条。如果开启，则会拉取全部结果。拉取全部结果需要用户具有更高权限（具有sql所访问的所有源表的查询、下载权限）。

## 其他接口

### 关闭SQLExecutor

默认行为关闭该executor，如果为连接池模式，则归还Executor到连接池中。

```java
void close();
```

### 获取Executor的ID

每个SQLExecutor实例都有一个ID，用于标识该实例。值为创建该实例时生成的UUID。

```java
String getId();
```

### 获取当前查询的任务名称

```java
String getTaskName();
```

### 获取当前查询ID

返回 null 表示 Executor 尚未初始化，或未执行作业。 
对于 MCQA 作业，返回值为`instanceId + '_' + subqueryId`，对于其他作业，返回值为 instanceId。

```java
String getQueryId();
```
### 获取子查询ID

返回 -1 表示 Executor 尚未初始化，或未执行作业。 
对于 MCQA 作业，返回值为子查询的id
对于其他作业，返回值始终为 -1

```java
int getSubQueryId();
```

### 获取当前查询的Logview

默认Logview有效期为7天，当生成logview出错时，该接口会返回null，不会抛出异常。

```java
String getLogView();
```

### 获取当前查询的实例

当使用MCQA时，该接口返回的是`Session`实例，否则返回上一个查询的`SQLTask`实例。返回值可能为null。

```java
Instance getInstance();
```

### 检查该 Executor 是否活跃

离线查询模式，该接口永远返回`false`。
MCQA查询模式，该接口返回的是`Session`状态。
MaxQA查询模式，该接口永远返回`false`。

```java
boolean isActive();
```

### 取消当前查询

```java
void cancel() throws OdpsException;
```

### 获取当前查询的进度信息

```java
List<Instance.StageProgress> getProgress() throws OdpsException;
```

### 获取当前查询的执行日志

```java
List<String> getExecutionLog();
```

### 获取当前查询的概要信息

```java
String getSummary() throws OdpsException;
```

### 判断当前执行的SQL在语法结构上是否具备结果集

```java
boolean hasResultSet();
```

### 判断当前 SQL 是否在交互模式中

```java
boolean isRunningInInteractiveMode();
```