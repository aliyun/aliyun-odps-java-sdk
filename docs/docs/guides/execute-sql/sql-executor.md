---
title: SQLExecutor 高级用法
sidebar_position: 4
description: SQLExecutor 的高级配置与使用技巧，包括 Builder 选项、结果处理、任务管理和 CommandApi。
module: odps-sdk-core
task: execute-sql-advanced
apis:
  - SQLExecutor
  - SQLExecutorBuilder
  - FallbackPolicy
since: "0.50.0"
keywords:
  - SQLExecutor
  - SQLExecutorBuilder
  - FallbackPolicy
  - ResultSet
  - CommandApi
  - InstanceTunnel
  - 回退策略
---

# SQLExecutor 高级用法

:::note
SQLExecutor 是 Java SDK 特有的 MCQA 交互式查询接口。Python 用户请参考 [MCQA 交互式查询](./mcqa.md) 中的 Python 示例。Go SDK 暂不支持 MCQA。
:::

`SQLExecutor` 是 MaxCompute Java SDK 中统一的 SQL 执行接口，支持离线、MCQA v1、MaxQA 三种执行模式。本文介绍 `SQLExecutor` 的高级配置和使用技巧。

## 前置条件

- 已创建 `Odps` 客户端实例并配置好 AccessKey 和 Endpoint
- Maven 依赖中已引入 `odps-sdk-core >= 0.50.0`

```xml
<dependency>
  <groupId>com.aliyun.odps</groupId>
  <artifactId>odps-sdk-core</artifactId>
  <version>0.50.0+</version>
</dependency>
```

## 完整示例

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.utils.SQLExecutorBuilder;
import com.aliyun.odps.utils.SQLExecutor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLExecutorAdvancedExample {

  public static void main(String[] args) {
    Account account = new AliyunAccount(
        System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"));
    Odps odps = new Odps(account);
    odps.setDefaultProject("your_project");
    odps.setEndpoint("http://service.cn-hangzhou.maxcompute.aliyun.com/api");

    SQLExecutor sqlExecutor = null;
    try {
      // 使用 Builder 完整配置
      sqlExecutor = SQLExecutorBuilder.builder()
          .odps(odps)
          .executeMode(ExecuteMode.INTERACTIVE)
          .quotaName("your_interactive_quota")
          .enableMcqaV2(true)
          .useInstanceTunnel(true)
          .tunnelEndpoint("http://dt.cn-hangzhou.maxcompute.aliyun.com")
          .enableCommandApi(true)
          .enableOdpsNamespaceSchema(true)
          .build();

      // 带 hints 执行查询
      Map<String, String> hints = new HashMap<>();
      hints.put("odps.sql.type.system.odps2", "true");
      sqlExecutor.run("SELECT * FROM my_table WHERE id > 100;", hints);

      // 获取查询元信息
      System.out.println("Executor ID: " + sqlExecutor.getId());
      System.out.println("Query ID: " + sqlExecutor.getQueryId());
      System.out.println("Logview: " + sqlExecutor.getLogView());
      System.out.println("Task Name: " + sqlExecutor.getTaskName());

      // 获取执行进度
      System.out.println("Progress: " + sqlExecutor.getProgress());

      // 获取执行日志（如回退信息）
      List<String> logs = sqlExecutor.getExecutionLog();
      logs.forEach(System.out::println);

      // 迭代获取结果
      ResultSet resultSet = sqlExecutor.getResultSet();
      while (resultSet.hasNext()) {
        Record record = resultSet.next();
        System.out.println(record);
      }

      // 使用 CommandApi 执行 DDL
      sqlExecutor.run("DESC my_table;", new HashMap<>());
      ResultSet descResult = sqlExecutor.getResultSet();
      while (descResult.hasNext()) {
        System.out.println(descResult.next());
      }

    } catch (OdpsException | IOException e) {
      e.printStackTrace();
    } finally {
      if (sqlExecutor != null) {
        sqlExecutor.close();
      }
    }
  }
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
from odps import ODPS
import os

# 初始化 ODPS 客户端
o = ODPS(
    os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID'),
    os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET'),
    project='your_project',
    endpoint='http://service.cn-hangzhou.maxcompute.aliyun.com/api',
)

# 带 hints 执行交互式查询
hints = {'odps.sql.type.system.odps2': 'true'}
instance = o.execute_sql_interactive(
    'SELECT * FROM my_table WHERE id > 100',
    hints=hints,
)

# 获取 Logview
print('Logview:', instance.get_logview_address())

# 迭代获取结果
with instance.open_reader() as reader:
    for record in reader:
        print(record)

# 读取为 DataFrame
with instance.open_reader(tunnel=True) as reader:
    df = reader.to_pandas()
```

</TabItem>
</Tabs>

## 代码说明

### Builder 配置选项

`SQLExecutorBuilder` 提供丰富的配置选项，以下按用途分类说明。

#### 基础配置

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `odps(Odps)` | 设置 Odps 对象（必选） | - |
| `executeMode(ExecuteMode)` | 执行模式：`OFFLINE` / `INTERACTIVE` | `OFFLINE` |
| `taskName(String)` | 任务名称 | 按模式自动设置 |

#### 结果获取配置

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `useInstanceTunnel(boolean)` | 是否通过 InstanceTunnel 获取结果 | `true` |
| `tunnelEndpoint(String)` | 指定 Tunnel Endpoint | 自动路由 |
| `tunnelSocketTimeout(int)` | Tunnel 连接超时（毫秒） | SDK 默认值 |
| `tunnelReadTimeout(int)` | Tunnel 读取超时（毫秒） | SDK 默认值 |
| `tunnelGetResultMaxRetryTime(int)` | 获取结果最大重试次数 | SDK 默认值 |

#### MCQA v1 专用配置

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `serviceName(String)` | Session 名称 | 自动创建 |
| `attachTimeout(Long)` | Session 连接超时（毫秒） | SDK 默认值 |
| `enableReattach(boolean)` | Session 断连后自动重连 | `true` |
| `properties(Map)` | Session 属性 | 空 |
| `sessionSupportNonSelect(boolean)` | 允许执行非 SELECT 操作 | `false` |

#### MaxQA 专用配置

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `quotaName(String)` | 交互式资源组名称（必选） | - |
| `enableMcqaV2(boolean)` | 启用 MaxQA | `false` |
| `quota(Quota)` | 传入已获取的 Quota 实例（缓存优化） | - |
| `regionId(String)` | Quota 所在 Region | 项目 Region |

#### 通用高级配置

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `fallbackPolicy(FallbackPolicy)` | 加速失败回退策略 | 默认回退 |
| `enableCommandApi(boolean)` | 启用 CommandApi（DDL 支持） | `false` |
| `enableOdpsNamespaceSchema(boolean)` | 启用三层模型 | `false` |
| `offlineJobPriority(Integer)` | 离线作业优先级 | 项目默认 |
| `recoverFrom(Instance)` | 从已有实例恢复 Executor | - |
| `skipCheckIfSelect(boolean)` | 跳过 SELECT 语句校验 | `false` |

### 执行查询

#### 带 Hints 执行

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Map<String, String> hints = new HashMap<>();
hints.put("odps.sql.type.system.odps2", "true");
hints.put("odps.sql.decimal.odps2", "true");
sqlExecutor.run("SELECT * FROM my_table;", hints);
```

</TabItem>
<TabItem value="python" label="Python">

```python
hints = {
    'odps.sql.type.system.odps2': 'true',
    'odps.sql.decimal.odps2': 'true',
}
instance = o.execute_sql_interactive('SELECT * FROM my_table', hints=hints)
```

</TabItem>
</Tabs>

#### 带 Aliases 执行

Aliases 用于在 SQL 中引用资源文件的别名，常用于 UDF 场景。Aliases 可以通过 hints 传入：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Map<String, String> hints = new HashMap<>();
hints.put("odps.sql.udf.jars", "my_udf.jar");
sqlExecutor.run("SELECT my_udf(col) FROM my_table;", hints);
```

</TabItem>
<TabItem value="python" label="Python">

```python
hints = {'odps.sql.udf.jars': 'my_udf.jar'}
instance = o.execute_sql_interactive(
    'SELECT my_udf(col) FROM my_table',
    hints=hints,
)
```

</TabItem>
</Tabs>

### 结果处理

#### getResultSet 迭代获取（推荐）

`getResultSet()` 返回 `ResultSet` 迭代器，分批读取数据，内存友好：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 基础用法
ResultSet resultSet = sqlExecutor.getResultSet();
while (resultSet.hasNext()) {
  Record record = resultSet.next();
  // 处理 record
}

// 指定数量限制
ResultSet resultSet = sqlExecutor.getResultSet(1000L);

// 指定 offset 和 limit
ResultSet resultSet = sqlExecutor.getResultSet(0L, 5000L, null);

// 获取全量结果（需要更高权限）
ResultSet resultSet = sqlExecutor.getResultSet(0L, null, null, true);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 基础用法：迭代读取结果
with instance.open_reader() as reader:
    for record in reader:
        print(record)

# 使用 Tunnel 获取结果（保留列类型信息）
with instance.open_reader(tunnel=True) as reader:
    for record in reader:
        print(record)

# 读取为 pandas DataFrame
with instance.open_reader(tunnel=True) as reader:
    df = reader.to_pandas()
```

</TabItem>
</Tabs>

#### getResult 列表获取

:::info
`getResult()` 是 Java SDK 特有的方法，一次性将结果加载到内存。Python 中请使用 `open_reader()` 迭代读取。
:::

```java
// 获取全部结果（最多受 limitEnabled 限制）
List<Record> records = sqlExecutor.getResult();

// 限制数量
List<Record> records = sqlExecutor.getResult(100L);
```

:::warning
`getResult()` 将所有结果一次加载到内存。结果集较大时请使用 `getResultSet()` 避免 OOM。
:::

#### 关闭 InstanceTunnel

:::info
`useInstanceTunnel` 是 Java SDK SQLExecutorBuilder 特有的配置。Python 中通过 `open_reader()` 的 `tunnel` 参数控制是否使用 Tunnel。
:::

```java
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.OFFLINE)
    .useInstanceTunnel(false)
    .build();
```

### 查询信息获取

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 获取查询 ID（用于问题排查）
String queryId = sqlExecutor.getQueryId();

// 获取 Logview URL（有效期 7 天）
String logview = sqlExecutor.getLogView();

// 获取作业 Instance
Instance instance = sqlExecutor.getInstance();

// 获取执行进度
List<Instance.StageProgress> progress = sqlExecutor.getProgress();

// 获取执行日志（回退信息等）
List<String> logs = sqlExecutor.getExecutionLog();

// 获取作业摘要
String summary = sqlExecutor.getSummary();

// 判断是否有结果集
boolean hasResult = sqlExecutor.hasResultSet();

// 判断是否在交互模式中执行
boolean isInteractive = sqlExecutor.isRunningInInteractiveMode();
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 获取 Logview URL
logview = instance.get_logview_address()

# 获取 Instance ID（用于问题排查）
instance_id = instance.id

# 获取作业状态
status = instance.status
print('Status:', status)

# 判断是否成功
print('Success:', instance.is_successful())
```

</TabItem>
</Tabs>

### 取消查询

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
try {
  sqlExecutor.run("SELECT * FROM large_table;", new HashMap<>());
  // 在另一个线程中取消
  sqlExecutor.cancel();
} catch (OdpsException e) {
  // 处理取消后的异常
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 提交查询（不等待完成）
instance = o.run_sql_interactive('SELECT * FROM large_table')

# 取消查询
instance.stop()
```

</TabItem>
</Tabs>

### CommandApi（DDL 命令）

:::info
CommandApi 是 Java SDK SQLExecutor 特有的功能。Python 中可直接使用 `o.execute_sql()` 执行 DDL 命令。
:::

启用 `enableCommandApi(true)` 后，可以通过 `SQLExecutor` 执行 DDL 类命令：

```java
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.INTERACTIVE)
    .quotaName("your_quota")
    .enableMcqaV2(true)
    .enableCommandApi(true)
    .build();

// DESC TABLE
executor.run("DESC my_table;", new HashMap<>());
ResultSet descResult = executor.getResultSet();
while (descResult.hasNext()) {
  System.out.println(descResult.next());
}

// SHOW TABLES
executor.run("SHOW TABLES;", new HashMap<>());
ResultSet showResult = executor.getResultSet();
while (showResult.hasNext()) {
  System.out.println(showResult.next());
}
```

:::note
开启 CommandApi 后，部分扩展命令的行为可能与未开启时不一致。MaxCompute SQL 本身也在逐步支持类似扩展命令，请根据实际需要决定是否开启。
:::

## 配置选项

### 回退策略详解

`FallbackPolicy` 控制 MCQA 加速失败时的行为：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 始终回退到离线（默认行为）
.fallbackPolicy(FallbackPolicy.alwaysFallbackPolicy())

// 永不回退（加速失败直接报错）
.fallbackPolicy(FallbackPolicy.neverFallbackPolicy())
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 默认行为：加速失败时回退到离线
instance = o.execute_sql_interactive('SELECT * FROM my_table', fallback=True)

# 禁用回退（加速失败直接报错）
instance = o.execute_sql_interactive('SELECT * FROM my_table', fallback=False)
```

</TabItem>
</Tabs>

回退策略仅对 MCQA（v1 和 v2）模式生效。当回退发生时，可以通过 `getExecutionLog()` 查看回退原因。

### InstanceTunnel 配置

:::info
InstanceTunnel 精细配置是 Java SDK SQLExecutorBuilder 特有的功能。Python 中通过 ODPS 对象的 `tunnel_endpoint` 参数统一配置。
:::

当网络环境受限或需要性能优化时，可以精细配置 Tunnel 参数：

```java
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.OFFLINE)
    // 指定 Tunnel Endpoint（自动路由不可用时）
    .tunnelEndpoint("http://dt.cn-hangzhou.maxcompute.aliyun.com")
    // 连接超时 30 秒
    .tunnelSocketTimeout(30000)
    // 读取超时 60 秒
    .tunnelReadTimeout(60000)
    // 最大重试 3 次
    .tunnelGetResultMaxRetryTime(3)
    .build();
```

### 从已有实例恢复

:::info
从已有实例恢复 SQLExecutor 是 Java SDK 特有的功能，用于断点续取结果。
:::

可以从之前的 Instance 恢复 `SQLExecutor`，用于断点续取结果：

```java
// 对于 MCQA：恢复到已有 Session
Instance sessionInstance = ...; // 之前保存的 Session Instance
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.INTERACTIVE)
    .recoverFrom(sessionInstance)
    .build();

// 对于离线：恢复到已有 SQLTask Instance
Instance taskInstance = ...; // 之前保存的 SQLTask Instance
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.OFFLINE)
    .recoverFrom(taskInstance)
    .build();
```

## 注意事项

1. **资源释放**：使用完毕后务必调用 `close()` 释放资源。推荐使用 try-finally 模式。
2. **线程安全**：`SQLExecutor` 不是线程安全的，不要在多线程间共享同一个实例。多线程场景请使用 `SQLExecutorPool`。
3. **InstanceTunnel 权限**：当 `useInstanceTunnel(true)` 且获取全量结果时，用户需要具有 SQL 涉及源表的下载权限。
4. **CommandApi 兼容性**：开启 CommandApi 后部分命令行为与 SQL 模式不一致，生产环境建议充分测试。
5. **skipCheckIfSelect**：开启后会跳过 SELECT 语句校验以提升性能，但处理非查询语句时延时会变长，建议仅在请求主要为查询语句的场景使用。
6. **Logview 有效期**：通过 `getLogView()` 获取的 Logview 默认有效期为 7 天。生成失败时返回 null 而非抛出异常。
7. **getResultSet 阻塞**：`getResultSet()` 是同步接口，会阻塞当前线程直到查询完成或失败。

## 相关文档

- [执行 SQL 概览](./index.md)
- [离线模式执行 SQL](./offline.md)
- [MCQA 交互式查询](./mcqa.md)
- [SQLExecutor API 参考](../../reference/SQLExecutor.md)
