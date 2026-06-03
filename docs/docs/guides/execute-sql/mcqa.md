---
title: MCQA 交互式查询
sidebar_position: 3
description: 使用 SQLExecutor 提交 MCQA / MaxQA 交互式查询，实现毫秒~秒级响应。
module: odps-sdk-core
task: execute-sql-mcqa
apis:
  - SQLExecutor
  - SQLExecutorBuilder
since: "0.50.0"
keywords:
  - MCQA
  - MaxQA
  - 交互式查询
  - SQLExecutor
  - Session
  - FallbackPolicy
  - quotaName
---

# MCQA 交互式查询

MCQA（MaxCompute Query Acceleration）是 MaxCompute 提供的 SQL 查询加速服务，将分钟级查询缩减至秒级甚至毫秒级。SDK 支持 MCQA v1（基于 Session）和 MCQA v2 / MaxQA（无 Session）两种模式。

:::note
Go SDK 暂不支持 MCQA 交互式查询。
:::

## 前置条件

- 已创建 `Odps` 客户端实例并配置好 AccessKey 和 Endpoint
- 项目已开通 MCQA 服务或已配置交互式资源组（MaxQA）
- Maven 依赖中已引入 `odps-sdk-core >= 0.50.0`（MaxQA 需要 `>= 0.52.0`）

```xml
<dependency>
  <groupId>com.aliyun.odps</groupId>
  <artifactId>odps-sdk-core</artifactId>
  <version>0.52.0+</version>
</dependency>
```

## 完整示例

### MCQA v1（基于 Session）

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.utils.SQLExecutorBuilder;
import com.aliyun.odps.utils.SQLExecutor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class McqaV1Example {

  public static void main(String[] args) {
    Account account = new AliyunAccount(
        System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"));
    Odps odps = new Odps(account);
    odps.setDefaultProject("your_project");
    odps.setEndpoint("http://service.cn-hangzhou.maxcompute.aliyun.com/api");

    SQLExecutor sqlExecutor = null;
    try {
      // 创建 MCQA v1 模式的 Executor（基于 Session）
      sqlExecutor = SQLExecutorBuilder.builder()
          .odps(odps)
          .executeMode(ExecuteMode.INTERACTIVE)
          .build();

      // 提交查询
      sqlExecutor.run("SELECT count(1) FROM my_table;", new HashMap<>());

      // 获取 Logview
      System.out.println("Logview: " + sqlExecutor.getLogView());

      // 获取结果
      ResultSet resultSet = sqlExecutor.getResultSet();
      while (resultSet.hasNext()) {
        System.out.println(resultSet.next());
      }

      // 在同一 Session 中继续执行查询
      sqlExecutor.run("SELECT * FROM my_table LIMIT 10;", new HashMap<>());
      ResultSet resultSet2 = sqlExecutor.getResultSet();
      while (resultSet2.hasNext()) {
        System.out.println(resultSet2.next());
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
# MCQA 交互式查询（毫秒~秒级响应）
instance = o.execute_sql_interactive('SELECT * FROM my_table LIMIT 100')

# 读取结果
with instance.open_reader() as reader:
    for record in reader:
        print(record)

# 读取为 DataFrame
with instance.open_reader(tunnel=True) as reader:
    df = reader.to_pandas()
```

</TabItem>
</Tabs>

### MCQA v2 / MaxQA（无 Session）

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.utils.SQLExecutorBuilder;
import com.aliyun.odps.utils.SQLExecutor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class McqaV2Example {

  public static void main(String[] args) {
    Account account = new AliyunAccount(
        System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"));
    Odps odps = new Odps(account);
    odps.setDefaultProject("your_project");
    odps.setEndpoint("http://service.cn-hangzhou.maxcompute.aliyun.com/api");

    SQLExecutor sqlExecutor = null;
    try {
      // 创建 MCQA v2 (MaxQA) 模式的 Executor
      sqlExecutor = SQLExecutorBuilder.builder()
          .odps(odps)
          .executeMode(ExecuteMode.INTERACTIVE)
          .quotaName("your_interactive_quota_nickname")
          .enableMcqaV2(true)
          .build();

      // 提交查询
      Map<String, String> hints = new HashMap<>();
      sqlExecutor.run("SELECT count(1) FROM my_table;", hints);

      // 获取 Logview 和查询 ID
      System.out.println("Logview: " + sqlExecutor.getLogView());
      System.out.println("QueryId: " + sqlExecutor.getQueryId());

      // 获取结果
      ResultSet resultSet = sqlExecutor.getResultSet();
      while (resultSet.hasNext()) {
        System.out.println(resultSet.next());
      }

      // 继续执行下一条查询（每次独立提交，无 Session 依赖）
      sqlExecutor.run("SELECT * FROM my_table LIMIT 10;", new HashMap<>());
      ResultSet resultSet2 = sqlExecutor.getResultSet();
      while (resultSet2.hasNext()) {
        System.out.println(resultSet2.next());
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
# MCQA 交互式查询（毫秒~秒级响应）
instance = o.execute_sql_interactive('SELECT * FROM my_table LIMIT 100')

# 读取结果
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

### MCQA v1 工作原理

1. **创建 Session**：`SQLExecutorBuilder.build()` 时会在服务端创建（或连接到已有的）Session 实例
2. **提交子查询**：`run()` 将 SQL 作为子查询提交到 Session 中执行
3. **获取结果**：`getResultSet()` 阻塞等待子查询完成并返回结果迭代器
4. **Session 复用**：同一个 `SQLExecutor` 的多次 `run()` 调用共享同一个 Session

### MCQA v2 (MaxQA) 工作原理

1. **无 Session**：每次 `run()` 独立提交查询，无需维护 Session 生命周期
2. **独享资源**：通过 `quotaName` 指定交互式资源组，使用 Virtual Warehouse 独享计算资源
3. **获取结果**：与 v1 相同，通过 `getResultSet()` 获取结果

### 两种模式对比

| 特性 | MCQA v1 | MCQA v2 (MaxQA) |
|------|---------|-----------------|
| Session 管理 | 需要（自动） | 不需要 |
| 资源隔离 | 共享资源 | 独享资源组 |
| 配置复杂度 | 低 | 低（需指定 quotaName） |
| 加速稳定性 | 可能因资源不足失败 | 更稳定 |
| SDK 版本要求 | >= 0.50.0 | >= 0.52.0 |

## 配置选项

### 交互式资源组配置（MaxQA 必选）

MaxQA 模式必须指定交互式资源组名称：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.INTERACTIVE)
    .quotaName("your_interactive_quota_nickname")  // 必选
    .enableMcqaV2(true)
    .build();
```

</TabItem>
<TabItem value="python" label="Python">

```python
# PyODPS 通过 execute_sql_interactive 自动使用交互式资源组
instance = o.execute_sql_interactive('SELECT count(1) FROM my_table')
```

</TabItem>
</Tabs>

### 回退策略（Fallback Policy）

当 MCQA 加速失败时，默认会回退到离线模式执行。可以通过 `fallbackPolicy` 自定义回退策略：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 使用默认回退策略（加速失败时回退到离线）
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.INTERACTIVE)
    .fallbackPolicy(FallbackPolicy.alwaysFallbackPolicy())
    .build();

// 禁用回退（加速失败直接报错）
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.INTERACTIVE)
    .fallbackPolicy(FallbackPolicy.neverFallbackPolicy())
    .build();
```

</TabItem>
<TabItem value="python" label="Python">

```python
# PyODPS 的 execute_sql_interactive 默认在加速失败时回退到离线模式
instance = o.execute_sql_interactive('SELECT * FROM my_table')

# 禁用回退（加速失败直接报错）
instance = o.execute_sql_interactive('SELECT * FROM my_table',
    fallback=False)
```

</TabItem>
</Tabs>

### Session 相关配置（仅 MCQA v1）

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.INTERACTIVE)
    // 指定 Session 名称（连接到已有 Session）
    .serviceName("my_session")
    // Session 连接超时时间（毫秒）
    .attachTimeout(30000L)
    // 启用 Session 断连后自动重连
    .enableReattach(true)
    .build();
```

</TabItem>
<TabItem value="python" label="Python">

```python
# PyODPS 自动管理 Session 生命周期，无需手动配置
instance = o.execute_sql_interactive('SELECT * FROM my_table')
```

</TabItem>
</Tabs>

### 其他常用配置

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
SQLExecutor executor = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.INTERACTIVE)
    .quotaName("your_quota")
    .enableMcqaV2(true)
    // 自定义 Tunnel Endpoint（网络受限时使用）
    .tunnelEndpoint("http://dt.cn-hangzhou.maxcompute.aliyun.com")
    // 是否使用 InstanceTunnel 获取结果（默认 true）
    .useInstanceTunnel(true)
    .build();
```

</TabItem>
<TabItem value="python" label="Python">

```python
# PyODPS 通过 ODPS 对象配置 Tunnel Endpoint
o = ODPS(access_id, secret_access_key, project='my_project',
         endpoint=endpoint,
         tunnel_endpoint='http://dt.cn-hangzhou.maxcompute.aliyun.com')

instance = o.execute_sql_interactive('SELECT * FROM my_table')
```

</TabItem>
</Tabs>

## 注意事项

1. **版本要求**：MCQA v1 需要 SDK >= 0.50.0，MaxQA 需要 SDK >= 0.52.0。低版本可能存在兼容性问题。
2. **资源组**：MaxQA 模式必须配置有效的交互式资源组（`quotaName`），否则创建 `SQLExecutor` 时会抛出异常。
3. **数据量限制**：MCQA 适用于中小数据量查询。数据量过大时可能加速失败并回退到离线模式。
4. **回退延迟**：当加速失败回退到离线时，总体延迟会增加（加速尝试时间 + 离线执行时间）。如果确定查询不适合加速，建议直接使用离线模式。
5. **Session 超时（v1）**：MCQA v1 的 Session 有超时机制，长时间未使用可能被回收。开启 `enableReattach(true)` 可自动重连。
6. **关闭 Executor**：使用完毕后务必调用 `sqlExecutor.close()` 释放资源。

## 相关文档

- [执行 SQL 概览](./index.md)
- [离线模式执行 SQL](./offline.md)
- [SQLExecutor 高级用法](./sql-executor.md)
- [SQLExecutor API 参考](../../reference/SQLExecutor.md)
- [查询加速 MCQA（阿里云官方文档）](https://help.aliyun.com/zh/maxcompute/user-guide/maxcompute-query-acceleration)
- [MaxQA（阿里云官方文档）](https://help.aliyun.com/zh/maxcompute/user-guide/maxcompute-query-acceleration2-0)
