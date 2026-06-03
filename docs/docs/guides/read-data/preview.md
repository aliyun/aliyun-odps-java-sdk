---
title: 数据预览（Table.read）
description: 使用 Table.read() 快速预览表中少量数据，适用于调试和数据验证场景。
sidebar_position: 2
module: odps-sdk-core
task: read-data
apis: [Table, RecordReader]
since: "0.47.0"
keywords: [预览, Table.read, RecordReader, 快速读取]
---

# 数据预览（Table.read）

`Table.read()` 是最简单的数据读取方式，适用于快速预览表中少量数据，无需额外配置 Tunnel 或 Storage API 客户端。

## 前置条件

- 已初始化 `Odps` 客户端对象
- 对目标表的读取列具有 `select` 权限
- SDK 版本 >= 0.47.0-public

## 完整示例

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

import java.util.Arrays;
import java.util.List;

public class TableReadExample {

    public static void main(String[] args) throws Exception {
        // 假设 odps 客户端已初始化
        Odps odps = getOdpsClient();

        // 获取表对象
        Table table = odps.tables().get("my_project", "user_info");

        // 示例1：基本读取，最多返回100行
        RecordReader reader = table.read(100);
        Record record;
        while ((record = reader.read()) != null) {
            System.out.println("id=" + record.get("id") + ", name=" + record.get("name"));
        }

        // 示例2：指定分区和列读取
        PartitionSpec partition = new PartitionSpec("dt=20250101");
        List<String> columns = Arrays.asList("id", "name", "age");
        RecordReader partitionReader = table.read(partition, columns, 500);
        while ((record = partitionReader.read()) != null) {
            System.out.println("id=" + record.get("id")
                + ", name=" + record.get("name")
                + ", age=" + record.get("age"));
        }

        // 示例3：非分区表读取全部列
        RecordReader fullReader = table.read(null, null, 1000);
        while ((record = fullReader.read()) != null) {
            // 处理每行数据...
        }
    }
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
from odps import ODPS

# 假设 odps 客户端已初始化
odps = ODPS('access_id', 'access_key', 'my_project',
            endpoint='http://service.odps.aliyun.com/api')

# 获取表对象
table = odps.get_table('user_info')

# 示例1：基本读取，最多返回100行
records = table.head(100)
for record in records:
    print(f"id={record['id']}, name={record['name']}")

# 示例2：指定分区和列读取
records = table.head(500, partition='dt=20250101',
                     columns=['id', 'name', 'age'])
for record in records:
    print(f"id={record['id']}, name={record['name']}, age={record['age']}")

# 示例3：非分区表读取全部列
records = table.head(1000)
for record in records:
    # 处理每行数据...
    pass
```

</TabItem>
</Tabs>

## 代码说明

1. **获取表对象**：通过 `odps.tables().get(projectName, tableName)` 获取 `Table` 实例。
2. **调用 read 方法**：`table.read(limit)` 是最简形式，仅指定最大行数；`table.read(partition, columns, limit)` 支持指定分区和列。
3. **迭代 RecordReader**：循环调用 `reader.read()`，返回 `null` 表示读取完毕。
4. **获取字段值**：通过 `record.get("columnName")` 或 `record.get(index)` 获取字段值，返回值类型参见下方类型映射表。

## 配置选项

| 参数 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `limit` | int | 是 | 最多读取的行数，上限为 1 万行 |
| `partition` | PartitionSpec | 否 | 分区规格，非分区表传 `null` |
| `columns` | List\<String\> | 否 | 指定读取的列名列表，传 `null` 读取全部列 |
| `timezone` | String | 否 | datetime 类型的时区设置（新接口使用 Java8 无时区类型，此参数无效） |
| `useLegacyMode` | boolean | 否 | 是否使用兼容旧接口模式，默认 `false`（不推荐开启） |
| `tunnelEndpoint` | String | 否 | 自定义 Tunnel Endpoint，通常由 SDK 自动获取 |

## 类型映射

RecordReader 返回的 Record 中，OdpsType 与 Java 类型的映射关系如下：

| OdpsType | JavaType |
|----------|----------|
| TINYINT | java.lang.Byte |
| SMALLINT | java.lang.Short |
| INT | java.lang.Integer |
| BIGINT | java.lang.Long |
| BINARY | com.aliyun.odps.data.Binary |
| FLOAT | java.lang.Float |
| DOUBLE | java.lang.Double |
| DECIMAL(precision,scale) | java.math.BigDecimal |
| VARCHAR(n) | com.aliyun.odps.data.Varchar |
| CHAR(n) | com.aliyun.odps.data.Char |
| STRING | java.lang.String |
| DATE | java.time.LocalDate |
| DATETIME | java.time.ZonedDateTime |
| TIMESTAMP | java.time.Instant |
| TIMESTAMP_NTZ | java.time.LocalDateTime |
| BOOLEAN | java.lang.Boolean |
| ARRAY | java.util.ArrayList |
| MAP | java.util.HashMap |
| STRUCT | com.aliyun.odps.data.SimpleStruct |
| JSON | com.aliyun.odps.data.JsonValue |

:::info 时间类型变更（0.47.0）
从 0.47.0-public 版本起，时间类型使用 Java 8 推荐的无时区类型：

| OdpsType | 新版本（>= 0.47.0） | 旧版本 |
|----------|---------------------|--------|
| DATE | java.time.LocalDate | java.sql.Date |
| DATETIME | java.time.ZonedDateTime | java.util.Date |
| TIMESTAMP | java.time.Instant | java.sql.Timestamp |

如需兼容旧行为，可设置 `useLegacyMode = true`，但会有额外性能开销。
:::

## 注意事项

- **行数限制**：最多返回 1 万行记录，超出部分会被截断，不会报错。
- **数据量限制**：单次预览数据量上限约 10MB。
- **不保证有序**：read 接口的返回数据不保证顺序。
- **仅适用于预览**：对于大量数据读取，应使用 [Tunnel 下载](./tunnel-download.md) 或 [Storage API](./storage-api-read.md)。
- **底层实现**：自 0.47.0 起，read 接口底层使用 TableTunnel 的 preview 方法实现。
- **Tunnel Endpoint**：通常由 SDK 自动获取，无需手动指定。

## 相关文档

- [Tunnel 下载](./tunnel-download.md) - 适用于大批量数据导出
- [Storage API 读取](./storage-api-read.md) - 适用于高性能并行读取
- [读取数据概览](./index.md) - 三种方式对比与选型
