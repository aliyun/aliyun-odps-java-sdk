---
title: Instance
description: Instance 实例对象，表示 MaxCompute 中计算任务的一次运行，提供生命周期管理、状态查询、结果获取等操作。
sidebar_position: 9
module: odps-sdk-core
apis:
  - getId
  - getStatus
  - isTerminated
  - isSuccessful
  - waitForSuccess
  - stop
  - getTaskResults
  - getTaskSummary
  - getStartTime
  - getEndTime
  - getOwner
keywords:
  - Instance
  - 实例
  - 任务执行
  - waitForSuccess
  - getTaskResults
---

# Instance

`Instance` 类表示 MaxCompute 中计算任务的一次运行实例。每个 SQL 查询、MapReduce 任务等提交后都会创建一个 Instance。通过 Instance 可以管理任务生命周期、查询执行状态、获取运行结果。

## 获取实例

通过 `Instances` 集合获取 Instance 对象：

```java
// 创建任务并获取 Instance
Instance instance = odps.instances().create(task);

// 通过 ID 获取已有 Instance
Instance instance = odps.instances().get("instance_id");
```

## 状态枚举

```java
public enum Status {
    RUNNING,     // 正在执行
    SUSPENDED,   // 被挂起
    TERMINATED   // 执行结束（包括成功、失败、取消）
}
```

## 方法

### getId

获取 Instance 的唯一标识符。

```java
public String getId()
```

**返回值**：Instance ID 字符串

---

### getStatus

获取 Instance 当前运行状态。

```java
public Status getStatus()
public Status getStatus(boolean isBlock)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `isBlock` | `boolean` | 是否使用 block 模式。启用后请求会在服务端等待约 5s 再返回状态 |

**返回值**：`Instance.Status` 枚举值

---

### isTerminated

检查 Instance 是否已执行完成。

```java
public boolean isTerminated()
```

**返回值**：`TERMINATED` 状态返回 `true`，否则返回 `false`

---

### isSuccessful

检查 Instance 是否执行成功（所有 Task 状态均为 SUCCESS）。

```java
public boolean isSuccessful() throws OdpsException
```

**返回值**：所有 Task 成功返回 `true`

---

### waitForSuccess

阻塞当前线程，直到 Instance 执行结束。若任务失败则抛出异常。

```java
public void waitForSuccess() throws OdpsException
public void waitForSuccess(long interval) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `interval` | `long` | 轮询间隔（毫秒），默认 1000ms |

**异常**：Instance 中任何 Task 失败时抛出 `OdpsException`

**示例**：

```java
Instance instance = odps.instances().create(task);
instance.waitForSuccess();
Map<String, String> results = instance.getTaskResults();
```

---

### waitForTerminated

阻塞当前线程直到 Instance 结束，不检查任务是否成功。

```java
public void waitForTerminated(long interval, boolean isBlock)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `interval` | `long` | 轮询间隔（毫秒） |
| `isBlock` | `boolean` | 是否启用 block 模式（服务端 long-polling） |

---

### stop

停止正在执行的 Instance。停止动作为异步执行。

```java
public void stop() throws OdpsException
```

> 如需确保 Instance 已停止，可在调用后轮询 `isTerminated()` 直至返回 `true`。

---

### getTaskResults

获取 Instance 中所有 Task 的运行结果。

```java
public Map<String, String> getTaskResults() throws OdpsException
```

**返回值**：`Map<String, String>`，key 为 Task 名称，value 为结果字符串

---

### getTaskResultsWithFormat

获取 Instance 中所有 Task 的运行结果（带格式信息）。

```java
public Map<String, Result> getTaskResultsWithFormat() throws OdpsException
```

**返回值**：`Map<String, Result>`，key 为 Task 名称，value 为 `Result` 对象

---

### getTaskSummary

获取指定 Task 的运行汇总信息。

```java
public TaskSummary getTaskSummary(String taskName) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `taskName` | `String` | Task 名称 |

**返回值**：`TaskSummary` 对象，包含任务运行汇总；服务端返回格式错误时返回 `null`

---

### getTaskStatus

获取 Instance 中所有 Task 的状态。

```java
public Map<String, TaskStatus> getTaskStatus() throws OdpsException
```

**返回值**：`Map<String, TaskStatus>`，key 为 Task 名称

**TaskStatus.Status 枚举**：
- `WAITING` - 等待中
- `RUNNING` - 运行中
- `SUCCESS` - 执行成功
- `FAILED` - 执行失败
- `SUSPENDED` - 被挂起
- `CANCELLED` - 已取消

---

### getStartTime

获取 Instance 开始执行时间。

```java
public Date getStartTime()
```

---

### getEndTime

获取 Instance 结束执行时间。

```java
public Date getEndTime()
```

---

### getOwner

获取 Instance 所属用户。

```java
public String getOwner()
```

---

### getProject

获取 Instance 所属项目名称。

```java
public String getProject()
```

---

## LogView

通过 `Odps.logview()` 生成 Instance 的 LogView 链接，用于在浏览器中查看任务详情。

```java
LogView logView = odps.logview();
String logViewUrl = logView.generateLogView(instance);
```

### generateLogView

```java
public String generateLogView(Instance instance) throws OdpsException
public String generateLogView(Instance instance, long hours) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `instance` | `Instance` | 目标 Instance |
| `hours` | `long` | Token 有效时长（小时） |

**返回值**：LogView URL 字符串
