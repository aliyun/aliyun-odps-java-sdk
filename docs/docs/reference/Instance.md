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
  - waitForTerminatedAndGetResult
  - getRawTaskResults
  - getTaskNames
  - getTaskCost
  - getTaskInfo
  - setInformation
  - getTaskProgress
  - getStageProgressFormattedString
  - getTaskDetailJson
  - getTaskQuotaJson
  - isSync
  - getTasks
  - getPriority
  - getJobName
  - getQueueingInfo
  - isSelect
  - getResultDescriptor
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

## Instances 集合操作

以下方法属于 `Instances` 集合管理类，通过 `odps.instances()` 访问。

### create

提交任务创建 Instance。

```java
public Instance create(Task task) throws OdpsException
public Instance create(Task task, int priority) throws OdpsException
public Instance create(Task task, int priority, String runningCluster) throws OdpsException
public Instance create(String projectName, Task task) throws OdpsException
public Instance create(String projectName, Task task, int priority) throws OdpsException
public Instance create(String projectName, Task task, int priority, String runningCluster) throws OdpsException
public Instance create(Job job) throws OdpsException
public Instance create(Task task, CreateInstanceOption option) throws OdpsException
public Instance create(List<Task> tasks, CreateInstanceOption option) throws OdpsException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `task` | `Task` | 任务对象（如 SQLTask） |
| `priority` | `int` | 任务优先级（0-9，数值越小优先级越高） |
| `runningCluster` | `String` | 指定运行集群 |
| `projectName` | `String` | 项目名称 |
| `job` | `Job` | 作业对象（包含多个 Task） |
| `option` | `CreateInstanceOption` | 创建选项 |

---

### get

获取已有 Instance 对象。

```java
public Instance get(String id)
public Instance get(String projectName, String id)
```

---

### exists

判断 Instance 是否存在。

```java
public boolean exists(String id) throws OdpsException
public boolean exists(String projectName, String id) throws OdpsException
```

---

### iterator / iterable

遍历项目中的 Instance。

```java
public Iterator<Instance> iterator()
public Iterator<Instance> iterator(String project)
public Iterator<Instance> iterator(InstanceFilter filter)
public Iterator<Instance> iterator(String project, InstanceFilter filter)
public Iterable<Instance> iterable()
public Iterable<Instance> iterable(String project)
public Iterable<Instance> iterable(InstanceFilter filter)
public Iterable<Instance> iterable(String project, InstanceFilter filter)
```

InstanceFilter 属性：

| 属性 | 说明 |
|------|------|
| `status` | 实例状态（Running / Suspended / Terminated） |
| `onlyOwner` | 是否只列出当前用户的实例 |
| `fromTime` | 起始时间 |
| `endTime` | 结束时间 |
| `quotaIndex` | Quota 索引 |

示例：
```java
InstanceFilter filter = new InstanceFilter();
filter.setStatus(Instance.Status.Running);
for (Instance inst : odps.instances().iterable(filter)) {
    System.out.println(inst.getId() + " " + inst.getStatus());
}
```

---

### iteratorQueueing

遍历排队中的 Instance 信息。

```java
public Iterator<Instance.InstanceQueueingInfo> iteratorQueueing()
public Iterator<Instance.InstanceQueueingInfo> iteratorQueueing(String project)
public Iterator<Instance.InstanceQueueingInfo> iteratorQueueing(InstanceFilter filter)
public Iterator<Instance.InstanceQueueingInfo> iteratorQueueing(String project, InstanceFilter filter)
```

---

### getDefaultRunningCluster / setDefaultRunningCluster

获取或设置默认运行集群。

```java
public String getDefaultRunningCluster()
public void setDefaultRunningCluster(String defaultRunningCluster)
```

---

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

### waitForTerminatedAndGetResult

等待任务终止并获取结果。

```java
public Instance.Result waitForTerminatedAndGetResult() throws OdpsException
```

**返回值**：`Instance.Result` 对象，包含任务执行结果

---

### getRawTaskResults

获取原始任务结果列表。

```java
public List<TaskResult> getRawTaskResults() throws OdpsException
```

**返回值**：`List<TaskResult>`，包含每个 Task 的原始结果信息

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

## 任务信息

### getTaskNames

获取所有任务名称集合。

```java
public Set<String> getTaskNames() throws OdpsException
```

**返回值**：`Set<String>`，包含 Instance 中所有 Task 的名称

---

### getTaskCost

获取指定任务的资源消耗信息。

```java
public TaskCost getTaskCost(String taskName) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `taskName` | `String` | Task 名称 |

**返回值**：`TaskCost` 对象，包含任务资源消耗详情

---

### getTaskInfo

获取任务的指定信息项。

```java
public String getTaskInfo(String taskName, String infoKey) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `taskName` | `String` | Task 名称 |
| `infoKey` | `String` | 信息项的 key |

**返回值**：对应信息项的值字符串

---

### setInformation

设置任务信息。

```java
public SetInformationResult setInformation(String taskName, String infoKey, String infoValue) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `taskName` | `String` | Task 名称 |
| `infoKey` | `String` | 信息项的 key |
| `infoValue` | `String` | 信息项的 value |

**返回值**：`SetInformationResult` 对象，包含设置操作的结果

---

## 进度与详情

### getTaskProgress

获取任务各阶段执行进度。

```java
public List<StageProgress> getTaskProgress(String taskName) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `taskName` | `String` | Task 名称 |

**返回值**：`List<StageProgress>`，包含各阶段的进度信息

---

### getStageProgressFormattedString

将进度格式化为可读字符串。

```java
public static String getStageProgressFormattedString(List<StageProgress> stages)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `stages` | `List<StageProgress>` | 阶段进度列表 |

**返回值**：格式化后的进度字符串

---

### getTaskDetailJson

获取任务详情 JSON。

```java
public String getTaskDetailJson(String taskName) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `taskName` | `String` | Task 名称 |

**返回值**：任务详情的 JSON 字符串

---

### getTaskQuotaJson

获取任务 Quota 信息 JSON。

```java
public String getTaskQuotaJson(String taskName) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `taskName` | `String` | Task 名称 |

**返回值**：任务 Quota 信息的 JSON 字符串

---

## 实例属性

### isSync

判断是否同步执行的实例。

```java
public boolean isSync()
```

**返回值**：同步执行返回 `true`，异步执行返回 `false`

---

### getTasks

获取实例中的所有 Task 列表。

```java
public List<Task> getTasks() throws OdpsException
```

**返回值**：`List<Task>`，包含 Instance 中所有 Task 对象

---

### getPriority

获取实例优先级。

```java
public int getPriority() throws OdpsException
```

**返回值**：实例优先级数值

---

### getJobName

获取作业名称。

```java
public String getJobName() throws OdpsException
```

**返回值**：作业名称字符串

---

### getQueueingInfo

获取实例排队信息。

```java
public InstanceQueueingInfo getQueueingInfo() throws OdpsException
```

**返回值**：`InstanceQueueingInfo` 对象，包含实例排队详情

---

## MCQA / Select 相关

### isSelect

判断指定任务是否为 SELECT 查询。

```java
public boolean isSelect(String taskName) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `taskName` | `String` | Task 名称 |

**返回值**：是 SELECT 查询返回 `true`，否则返回 `false`

---

### getResultDescriptor

获取 SELECT 结果的描述信息（列名、类型等）。

```java
public ResultDescriptor getResultDescriptor(String taskName) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `taskName` | `String` | Task 名称 |

**返回值**：`ResultDescriptor` 对象，包含结果集的列名、数据类型等元信息

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
