---
title: Tunnel 下载
description: 使用 TableTunnel 批量下载表数据，支持压缩、多线程并行下载。
sidebar_position: 3
module: odps-sdk-tunnel
task: read-data
apis: [TableTunnel, DownloadSession, TunnelRecordReader]
since: "0.48.0"
keywords: [Tunnel, 下载, DownloadSession, 批量导出, 多线程]
---

# Tunnel 下载

TableTunnel 是 MaxCompute 的标准数据通道，适用于大批量数据的导出场景。支持压缩传输、多线程并行下载，无行数限制。

## 前置条件

- 已初始化 `Odps` 客户端对象
- 添加 `odps-sdk-tunnel` 模块依赖
- 对目标表具有读取权限
- SDK 版本 >= 0.48.0

## 完整示例

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

public class TunnelDownloadExample {

    public static void main(String[] args) throws Exception {
        // 假设 odps 客户端已初始化
        Odps odps = getOdpsClient();

        // 1. 创建 TableTunnel 对象
        TableTunnel tunnel = new TableTunnel(odps);

        // 2. 创建 DownloadSession
        DownloadSession session = tunnel.buildDownloadSession()
            .setProjectName("my_project")
            .setTableName("user_behavior")
            .setPartitionSpec(new PartitionSpec("dt=20250101"))
            .build();

        // 3. 获取总行数
        long totalRecords = session.getRecordCount();
        System.out.println("总记录数: " + totalRecords);

        // 4. 打开 Reader 并读取数据
        try (TunnelRecordReader reader = session.openRecordReader(0, totalRecords)) {
            while (reader.hasNext()) {
                Record record = reader.next();
                System.out.println("user_id=" + record.get("user_id")
                    + ", action=" + record.get("action"));
            }
        }
    }
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 读取整张表
with table.open_reader() as reader:
    for record in reader:
        print(record)

# 读取分区表
with table.open_reader(partition='dt=20231001') as reader:
    for record in reader:
        print(record)

# Arrow 格式读取（高性能）
with table.open_reader(arrow=True) as reader:
    for batch in reader:
        df = batch.to_pandas()

# 指定列读取
with table.open_reader(columns=['id', 'name']) as reader:
    for record in reader:
        print(record)
```

</TabItem>
<TabItem value="go" label="Go">

```go
project := odpsIns.DefaultProject()
tunnelEndpoint, _ := project.GetTunnelEndpoint()
tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)

// 创建下载会话
session, _ := tunnelIns.CreateDownloadSession(
    project.Name(), "my_table",
    tunnel.SessionCfg.WithPartitionKey("dt=20231001"),
)

// 读取数据
recordCount := session.RecordCount()
reader, _ := session.OpenRecordReader(0, recordCount, nil)
reader.Iterator(func(record data.Record, err error) {
    if err != nil {
        return
    }
    fmt.Println(record)
})
reader.Close()

// Arrow 格式读取
arrowReader, _ := session.OpenRecordArrowReader(0, recordCount, nil)
arrowReader.Iterator(func(rec array.Record, err error) {
    // 处理 Arrow RecordBatch
})
```

</TabItem>
</Tabs>

## 代码说明

1. **创建 TableTunnel**：基于已初始化的 `Odps` 对象创建 `TableTunnel` 实例，作为 Tunnel 操作的入口。
2. **构建 DownloadSession**：通过 Builder 模式设置项目名、表名、分区等参数，调用 `build()` 创建会话。会话创建时服务端会准备数据快照。
3. **获取记录总数**：`session.getRecordCount()` 返回可下载的记录总行数，用于规划分块下载。
4. **打开 Reader**：`openRecordReader(start, count)` 指定起始行号和读取行数，返回 `TunnelRecordReader`。
5. **迭代读取**：通过 `hasNext()` / `next()` 逐条读取 `Record` 对象。

## 配置选项

### DownloadSession 构建参数

| 方法 | 类型 | 必需 | 默认值 | 说明 |
|------|------|------|--------|------|
| `setProjectName(String)` | String | 是 | - | 目标项目名称 |
| `setTableName(String)` | String | 是 | - | 目标表名称 |
| `setPartitionSpec(PartitionSpec)` | PartitionSpec | 否 | null | 分区规格，分区表必须指定 |
| `setShardId(Long)` | Long | 否 | null | 指定分片 ID |
| `setAsyncMode(boolean)` | boolean | 否 | false | 异步初始化模式（大表推荐） |
| `setWaitAsyncBuild(boolean)` | boolean | 否 | false | 阻塞等待异步会话就绪 |

### openRecordReader 参数

| 参数 | 类型 | 约束 | 说明 |
|------|------|------|------|
| `start` | long | >= 0 | 读取起始行号 |
| `count` | long | >= 1 | 读取记录数量 |
| `option` | CompressOption | - | 压缩配置（默认启用 zlib 压缩） |
| `columns` | List\<Column\> | 非空 | 指定下载的列集合（列裁剪） |
| `disableModifiedCheck` | boolean | - | 禁用数据版本校验 |

### CompressOption 压缩选项

| 压缩算法 | 说明 |
|----------|------|
| `CompressOption.CompressAlgorithm.ODPS_RAW` | 不压缩 |
| `CompressOption.CompressAlgorithm.ODPS_ZLIB` | zlib 压缩（默认） |
| `CompressOption.CompressAlgorithm.ODPS_SNAPPY` | Snappy 压缩（更快，压缩比略低） |

## 多线程并行下载

对于大数据量场景，可将数据按行号范围拆分为多个块，使用线程池并行下载：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;

public class ParallelDownloadExample {

    public static void main(String[] args) throws Exception {
        Odps odps = getOdpsClient();
        TableTunnel tunnel = new TableTunnel(odps);

        DownloadSession session = tunnel.buildDownloadSession()
            .setProjectName("my_project")
            .setTableName("large_table")
            .build();

        long totalRecords = session.getRecordCount();
        int parallelism = 8; // 根据 CPU 核心数和网络带宽设置

        ExecutorService pool = Executors.newFixedThreadPool(parallelism);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < parallelism; i++) {
            long start = i * (totalRecords / parallelism);
            long count = (i == parallelism - 1)
                ? totalRecords - start
                : totalRecords / parallelism;

            futures.add(pool.submit(() -> {
                try (TunnelRecordReader reader = session.openRecordReader(start, count,
                        new CompressOption(CompressOption.CompressAlgorithm.ODPS_ZLIB, 1, 0))) {
                    while (reader.hasNext()) {
                        Record record = reader.next();
                        processRecord(record);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("下载失败, start=" + start, e);
                }
            }));
        }

        // 等待所有下载任务完成
        for (Future<?> future : futures) {
            future.get();
        }
        pool.shutdown();
        System.out.println("下载完成，共 " + totalRecords + " 条记录");
    }
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
import concurrent.futures

table = o.get_table('large_table')

# 方式1: 使用 Arrow 格式高效读取（自动批次处理，推荐）
with table.open_reader(arrow=True) as reader:
    for batch in reader:
        df = batch.to_pandas()
        process_dataframe(df)

# 方式2: 按分区并行读取
def read_partition(pt_spec):
    with table.open_reader(partition=pt_spec, arrow=True) as reader:
        for batch in reader:
            process_dataframe(batch.to_pandas())

partitions = [str(p.partition_spec) for p in table.partitions]
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    pool.map(read_partition, partitions)
print("下载完成")
```

</TabItem>
<TabItem value="go" label="Go">

```go
session, _ := tunnelIns.CreateDownloadSession(
    project.Name(), "large_table",
)

totalRecords := session.RecordCount()
parallelism := 8
chunkSize := totalRecords / int64(parallelism)

var wg sync.WaitGroup
for i := 0; i < parallelism; i++ {
    wg.Add(1)
    go func(idx int) {
        defer wg.Done()
        start := int64(idx) * chunkSize
        count := chunkSize
        if idx == parallelism-1 {
            count = totalRecords - start
        }
        reader, _ := session.OpenRecordReader(start, count, nil)
        reader.Iterator(func(record data.Record, err error) {
            if err != nil {
                return
            }
            processRecord(record)
        })
        reader.Close()
    }(i)
}
wg.Wait()
fmt.Printf("下载完成，共 %d 条记录\n", totalRecords)
```

</TabItem>
</Tabs>

## 异步创建模式（大表）

对于数据量特别大的表，Session 创建可能需要较长时间。可以使用异步模式避免阻塞：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
TableTunnel.DownloadSessionBuilder builder = tunnel.buildDownloadSession()
    .setProjectName("big_data_project")
    .setTableName("huge_table")
    .setAsyncMode(true);

DownloadSession session = builder.build();

// 轮询等待会话就绪，每 5 秒检查一次，最长等待 300 秒
boolean ready = builder.wait(session, 5, 300);
if (ready) {
    System.out.println("会话就绪，记录数: " + session.getRecordCount());
    // 开始下载...
} else {
    System.err.println("会话创建超时");
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# PyODPS 自动处理大表的会话创建，无需手动配置异步模式
table = o.get_table('huge_table')
with table.open_reader() as reader:
    print(f"会话就绪，记录数: {reader.count}")
    for record in reader:
        process_record(record)
```

</TabItem>
<TabItem value="go" label="Go">

```go
// Go SDK 在 CreateDownloadSession 时自动处理会话初始化
// 无需额外的异步模式配置
session, _ := tunnelIns.CreateDownloadSession(
    project.Name(), "huge_table",
)
recordCount := session.RecordCount()
fmt.Printf("会话就绪，记录数: %d\n", recordCount)
```

</TabItem>
</Tabs>

## 注意事项

- **不支持 Transactional 表**：Tunnel Download 目前不支持 Transactional 表的下载，请使用 [Storage API](./storage-api-read.md)。
- **超时机制**：默认 300 秒无数据传输时，服务端会断开连接。对于处理逻辑较重的场景，建议缩小每次的 `count` 值。
- **数据一致性**：DownloadSession 创建时会生成数据快照，读取期间数据不会变化。
- **分区表必须指定分区**：对于分区表，`setPartitionSpec()` 是必需的，否则会抛出异常。
- **资源释放**：务必使用 try-with-resources 语法关闭 `TunnelRecordReader`，避免连接泄漏。
- **并行度设置**：建议根据 CPU 核心数和网络带宽合理设置，一般 4-16 个线程较为合适。

## 相关文档

- [数据预览（Table.read）](./preview.md) - 适用于少量数据快速预览
- [Storage API 读取](./storage-api-read.md) - 适用于高性能场景和 Transactional 表
- [读取数据概览](./index.md) - 三种方式对比与选型
