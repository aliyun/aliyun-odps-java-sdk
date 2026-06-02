---
title: SQLExecutor
description: MaxCompute SQL 执行统一接口，支持离线、MCQA 和 MaxQA 模式
sidebar_position: 4
module: odps-sdk-core
apis: [SQLExecutor, SQLExecutorBuilder]
keywords: [SQLExecutor, SQL, MCQA, MaxQA, 查询]
---

# SQLExecutor

`SQLExecutor` 是 MaxCompute SQL 执行的统一接口，支持离线作业、MCQA 和 MaxQA 三种执行模式。

## 获取实例

通过 `SQLExecutorBuilder` 构建：

```java
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.INTERACTIVE_V2)
    .quotaName("my_quota")
    .build();
```

### SQLExecutorBuilder 配置

| 方法 | 参数类型 | 必需 | 说明 |
|------|----------|------|------|
| `odps(Odps)` | Odps | 是 | ODPS 对象 |
| `executeMode(ExecuteMode)` | ExecuteMode | 否 | `INTERACTIVE_V2`(MaxQA) / `INTERACTIVE`(MCQA) / `OFFLINE` |
| `quotaName(String)` | String | MaxQA必填 | 计算资源组名称 |
| `taskName(String)` | String | 否 | 任务名称 |
| `tunnelEndpoint(String)` | String | 否 | Tunnel 服务端点 |
| `enableCommandApi(boolean)` | boolean | 否 | 启用扩展命令支持 |
| `enableOdpsNamespaceSchema(boolean)` | boolean | 否 | 启用三层模型 |
| `useInstanceTunnel(boolean)` | boolean | 否 | 是否使用 InstanceTunnel 获取结果（默认 true） |
| `tunnelSocketTimeout(int)` | int | 否 | Tunnel 链接超时（毫秒） |
| `tunnelReadTimeout(int)` | int | 否 | Tunnel 读取超时（毫秒） |
| `tunnelGetResultMaxRetryTime(int)` | int | 否 | 获取结果最大重试次数 |
| `recoverFrom(Instance)` | Instance | 否 | 从指定实例恢复 |
| `offlineJobPriority(Integer)` | Integer | 否 | 离线作业优先级 |
| `fallbackPolicy(FallbackPolicy)` | FallbackPolicy | 否 | MCQA 失败回退策略 |
| `enableReattach(boolean)` | boolean | 否 | MCQA 重连 Session |
| `serviceName(String)` | String | 否 | MCQA Session 名称 |
| `enableMaxQA(boolean)` | boolean | 否 | 启用 MaxQA |
| `properties(Map<String, String>)` | Map\<String, String\> | 否 | 设置额外属性 |
| `runningCluster(String)` | String | 否 | 指定运行集群 |
| `regionId(String)` | String | 否 | 设置 Region ID |
| `builder()` | — | — | 静态方法，创建 Builder 实例 |
| `build()` | — | — | 构建 SQLExecutor 实例 |

## 方法列表

### run

执行 SQL 查询。

```java
void run(String sql, Map<String, String> hint) throws OdpsException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `sql` | String | SQL 语句 |
| `hint` | Map\<String, String\> | 查询提示参数 |

**示例**：

```java
executor.run("SELECT * FROM my_table LIMIT 100", new HashMap<>());
```

---

### getResult

获取查询结果列表（全部加载到内存）。

```java
List<Record> getResult() throws OdpsException, IOException
List<Record> getResult(Long countLimit) throws OdpsException, IOException
List<Record> getResult(Long offset, Long countLimit, Long sizeLimit) throws OdpsException, IOException
List<Record> getResult(Long offset, Long countLimit, Long sizeLimit, boolean limitEnabled) throws OdpsException, IOException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `offset` | Long | 偏移量，默认 0 |
| `countLimit` | Long | 数量限制 |
| `sizeLimit` | Long | 大小限制 |
| `limitEnabled` | boolean | false 时限制 1W 条；true 拉取全部（需更高权限） |

---

### getResultSet

获取结果迭代器（分批加载，减少内存占用）。

```java
ResultSet getResultSet() throws OdpsException, IOException
ResultSet getResultSet(Long countLimit) throws OdpsException, IOException
ResultSet getResultSet(Long offset, Long countLimit, Long sizeLimit) throws OdpsException, IOException
ResultSet getResultSet(Long offset, Long countLimit, Long sizeLimit, boolean limitEnabled) throws OdpsException, IOException
```

参数同 `getResult`。`ResultSet` 实现了 `Iterator<Record>` 和 `Iterable<Record>`。

---

### cancel

取消当前查询。

```java
void cancel() throws OdpsException
```

---

### getQueryId

获取当前查询 ID。MCQA 返回 `instanceId_subqueryId`，其他返回 instanceId。

```java
String getQueryId()
```

**返回值**：查询 ID，未初始化时返回 null

---

### getLogView

获取当前查询的 Logview URL（有效期 7 天）。

```java
String getLogView()
```

---

### getExecutionLog

获取当前查询的执行日志。

```java
List<String> getExecutionLog()
```

---

### isActive

检查 Executor 是否活跃。离线和 MaxQA 模式始终返回 false，MCQA 返回 Session 状态。

```java
boolean isActive()
```

---

### getProgress

获取查询进度信息。

```java
List<Instance.StageProgress> getProgress() throws OdpsException
```

---

### getSummary

获取查询概要信息。

```java
String getSummary() throws OdpsException
```

---

### getInstance

获取当前查询的 Instance 对象。MCQA 返回 Session 实例，其他返回 SQLTask 实例。

```java
Instance getInstance()
```

---

### getId

获取 Executor 的唯一标识（UUID）。

```java
String getId()
```

---

### getTaskName

获取当前任务名称。

```java
String getTaskName()
```

---

### getSubqueryId

获取当前子查询 ID。

```java
int getSubqueryId()
```

---

### getResult (带行数和大小限制)

获取结果（指定行数和大小限制）。

```java
List<Record> getResult(Long countLimit, Long sizeLimit) throws OdpsException, IOException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `countLimit` | Long | 行数限制 |
| `sizeLimit` | Long | 大小限制 |

---

### getResultSet (带行数和大小限制)

获取 ResultSet（指定行数和大小限制）。

```java
ResultSet getResultSet(Long countLimit, Long sizeLimit) throws OdpsException, IOException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `countLimit` | Long | 行数限制 |
| `sizeLimit` | Long | 大小限制 |

---

### isRunningInInteractiveMode

判断是否运行在交互式模式。

```java
boolean isRunningInInteractiveMode()
```

---

### getExecuteMode

获取当前执行模式。

```java
ExecuteMode getExecuteMode()
```

---

### close

关闭 Executor。连接池模式下归还至池中。

```java
void close()
```
