---
title: 离线模式执行 SQL
sidebar_position: 2
description: 使用 SQLTask 提交离线 SQL 作业，获取 Logview 和作业结果。
module: odps-sdk-core
task: execute-sql-offline
apis:
  - SQLTask
  - Instance
  - InstanceTunnel
since: "0.47.0"
keywords:
  - SQLTask
  - 离线作业
  - Instance
  - InstanceTunnel
  - Logview
  - hints
  - priority
---

# 离线模式执行 SQL

离线模式是 MaxCompute 的主要作业类型，适用于处理海量数据（GB、TB、EB 级别）的批量计算场景。本文介绍如何使用 `SQLTask` 提交离线 SQL 作业并获取结果。

## 前置条件

- 已创建 `Odps` 客户端实例并配置好 AccessKey 和 Endpoint
- 拥有目标项目的 SQL 执行权限
- Maven 依赖中已引入 `odps-sdk-core`

```xml
<dependency>
  <groupId>com.aliyun.odps</groupId>
  <artifactId>odps-sdk-core</artifactId>
  <version>0.47.0+</version>
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
import com.aliyun.odps.task.SQLTask;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OfflineSQLExample {

  public static void main(String[] args) throws OdpsException, IOException {
    // 1. 初始化 Odps 客户端
    Account account = new AliyunAccount(
        System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"));
    Odps odps = new Odps(account);
    odps.setDefaultProject("your_project");
    odps.setEndpoint("http://service.cn-hangzhou.maxcompute.aliyun.com/api");

    // 2. 提交 SQL 作业
    String sql = "SELECT * FROM my_table WHERE dt = '20240101';";
    Instance instance = SQLTask.run(odps, sql);

    // 3. 获取 Logview URL（用于作业监控）
    String logview = odps.logview().generateLogView(instance, 7 * 24);
    System.out.println("Logview: " + logview);

    // 4. 等待作业完成
    instance.waitForSuccess();

    // 5. 获取结果（方式一：小结果集直接获取）
    List<Record> records = SQLTask.getResult(instance);
    for (Record record : records) {
      System.out.println(record);
    }

    // 6. 获取结果（方式二：大结果集使用 InstanceTunnel 迭代获取）
    ResultSet resultSet = SQLTask.getResultSet(instance);
    while (resultSet.hasNext()) {
      Record record = resultSet.next();
      System.out.println(record);
    }
  }
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
from odps import ODPS

o = ODPS(access_id, secret_access_key, project='my_project', endpoint=endpoint)

# 同步执行（阻塞等待完成）
instance = o.execute_sql('SELECT * FROM my_table')

# 异步执行
instance = o.run_sql('SELECT * FROM my_table')
instance.wait_for_success()

# 获取 Logview
print(o.get_logview_address(instance.id, 24))

# 读取结果
with instance.open_reader() as reader:
    for record in reader:
        print(record)

# 读取为 pandas DataFrame
with instance.open_reader(tunnel=True) as reader:
    df = reader.to_pandas()
```

</TabItem>
<TabItem value="go" label="Go">

```go
// 执行 SQL
ins, err := odpsIns.ExecSQl("SELECT * FROM my_table;")
if err != nil {
    panic(err)
}

// 等待完成
err = ins.WaitForSuccess()

// 获取 Logview
logview := odpsIns.LogView()
logviewUrl, _ := logview.GenerateLogView(ins, 24)
fmt.Println(logviewUrl)

// 通过 Instance Tunnel 读取结果
project := odpsIns.DefaultProject()
tunnelEndpoint, _ := project.GetTunnelEndpoint()
tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)
session, _ := tunnelIns.CreateInstanceResultDownloadSession(project.Name(), ins.Id())
reader, _ := session.OpenRecordReader(0, session.RecordCount(), 0, nil)
reader.Iterator(func(record data.Record, err error) {
    fmt.Println(record)
})
```

</TabItem>
</Tabs>

## 代码说明

### 提交作业

`SQLTask.run()` 提交 SQL 作业到 MaxCompute 服务端，返回一个 `Instance` 对象代表该作业实例。该方法为异步调用，提交后立即返回。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 最简方式
Instance instance = SQLTask.run(odps, sql);

// 指定项目、hints 和 aliases
Instance instance = SQLTask.run(odps, project, sql, hints, aliases);

// 指定 taskName（用于后续通过 taskName 获取结果）
Instance instance = SQLTask.run(odps, project, sql, taskName, hints, aliases);

// 指定优先级
Instance instance = SQLTask.run(odps, project, sql, taskName, hints, aliases, priority);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 最简方式（同步执行）
instance = o.execute_sql('SELECT * FROM my_table')

# 异步执行
instance = o.run_sql('SELECT * FROM my_table')
instance.wait_for_success()

# 带 Hints
instance = o.execute_sql('SELECT * FROM my_table',
    hints={'odps.sql.type.system.odps2': 'true'})
```

</TabItem>
<TabItem value="go" label="Go">

```go
// 最简方式
ins, err := odpsIns.ExecSQl("SELECT * FROM my_table;")
err = ins.WaitForSuccess()

// 带 Hints
hints := map[string]string{"odps.sql.type.system.odps2": "true"}
ins, err := odpsIns.ExecSQlWithHints("SELECT * FROM my_table;", hints)
```

</TabItem>
</Tabs>

### 等待作业完成

`instance.waitForSuccess()` 会阻塞当前线程，直到作业成功或抛出异常（作业失败时）。

### 获取 Logview / JobInsight URL

Logview 是 MaxCompute 提供的作业监控页面，可以查看作业的执行计划、各阶段进度和资源消耗。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 生成 Logview URL，参数为有效时长（小时）
String logview = odps.logview().generateLogView(instance, 7 * 24);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 获取 Logview URL，参数为有效时长（小时）
print(o.get_logview_address(instance.id, 24))
```

</TabItem>
<TabItem value="go" label="Go">

```go
// 获取 Logview URL，参数为有效时长（小时）
logview := odpsIns.LogView()
url, _ := logview.GenerateLogView(ins, 24)
```

</TabItem>
</Tabs>

### 获取结果

SDK 提供三种获取结果的方式：

| 方式 | 方法 | 特点 |
|------|------|------|
| 快速获取 | `SQLTask.getResult()` | 最多 1 万条，所有字段为 String 类型 |
| InstanceTunnel 列表 | `SQLTask.getResultByInstanceTunnel()` | 带 Schema 信息，最多 1 万条 |
| InstanceTunnel 迭代器 | `SQLTask.getResultSet()` | 分批读取，无条数限制 |

## 配置选项

### Hints 配置

Hints 用于调整 SQL 执行行为，以键值对方式传入：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Map<String, String> hints = new HashMap<>();
// 开启 2.0 数据类型系统
hints.put("odps.sql.type.system.odps2", "true");
// 调整 mapper split 大小
hints.put("odps.sql.mapper.split.size", "128");
// 设置动态分区最大数量
hints.put("odps.sql.reshuffle.dynamicpt", "true");

Instance instance = SQLTask.run(odps, odps.getDefaultProject(), sql, hints, null);
```

</TabItem>
<TabItem value="python" label="Python">

```python
hints = {
    'odps.sql.type.system.odps2': 'true',
    'odps.sql.mapper.split.size': '128',
    'odps.sql.reshuffle.dynamicpt': 'true',
}
instance = o.execute_sql('SELECT * FROM my_table', hints=hints)
```

</TabItem>
<TabItem value="go" label="Go">

```go
hints := map[string]string{
    "odps.sql.type.system.odps2":   "true",
    "odps.sql.mapper.split.size":   "128",
    "odps.sql.reshuffle.dynamicpt": "true",
}
ins, err := odpsIns.ExecSQlWithHints("SELECT * FROM my_table;", hints)
```

</TabItem>
</Tabs>

### 优先级设置

优先级数字越小，优先级越高。默认优先级由项目配置决定。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Integer priority = 3;
Instance instance = SQLTask.run(odps, odps.getDefaultProject(), sql,
    "AnonymousSQLTask", null, null, priority);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 通过 priority 参数设置优先级
instance = o.execute_sql('SELECT * FROM my_table', priority=3)
```

</TabItem>
<TabItem value="go" label="Go">

```go
// Go SDK 通过 Hints 设置优先级
hints := map[string]string{"odps.instance.priority": "3"}
ins, err := odpsIns.ExecSQlWithHints("SELECT * FROM my_table;", hints)
```

</TabItem>
</Tabs>

### 大结果集获取（InstanceTunnel）

当结果集超过 1 万条时，需要使用 `getResultSet` 并设置 `limitHint` 为 `false` 获取全量数据：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// limitHint=false 表示获取全量结果（需要源表的下载权限）
ResultSet resultSet = SQLTask.getResultSet(instance, "AnonymousSQLTask", null, false);
while (resultSet.hasNext()) {
  Record record = resultSet.next();
  // 处理记录
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 使用 tunnel=True 获取全量结果
with instance.open_reader(tunnel=True) as reader:
    for record in reader:
        print(record)

# 读取为 pandas DataFrame
with instance.open_reader(tunnel=True) as reader:
    df = reader.to_pandas()
```

</TabItem>
<TabItem value="go" label="Go">

```go
// 通过 Instance Tunnel 读取全量结果
tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)
session, _ := tunnelIns.CreateInstanceResultDownloadSession(projectName, ins.Id())
reader, _ := session.OpenRecordReader(0, session.RecordCount(), 0, nil)
reader.Iterator(func(record data.Record, err error) {
    fmt.Println(record)
})
```

</TabItem>
</Tabs>

:::warning
当 `limitHint` 为 `false` 时，SDK 会对 SQL 涉及的每张表进行权限检查。如果项目开启了 Protection，需要提前为相应表添加 Policy Exception，否则会因权限不足而失败。
:::

## 注意事项

1. **作业超时**：离线作业的默认超时时间由项目配置决定，长时间运行的作业建议设置合理的超时。
2. **结果生命周期**：作业完成后的结果有保留时间，过期后无法通过 InstanceTunnel 获取。
3. **并发限制**：同一项目的并发作业数有上限，大量提交时注意控制并发。
4. **taskName**：如果提交时指定了自定义 `taskName`，获取结果时也必须传入相同的 `taskName`，否则无法获取到结果。
5. **内存控制**：大结果集请使用 `getResultSet()` 迭代器方式获取，避免使用 `getResult()` 将所有数据加载到内存。

## 相关文档

- [执行 SQL 概览](./index.md)
- [MCQA 交互式查询](./mcqa.md)
- [SQLExecutor 高级用法](./sql-executor.md)
- [SQLExecutor API 参考](../../reference/SQLExecutor.md)
- [MaxCompute SQL 概述](https://help.aliyun.com/zh/maxcompute/user-guide/overview-of-maxcompute-sql)
