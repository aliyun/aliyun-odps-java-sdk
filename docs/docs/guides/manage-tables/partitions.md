---
title: 分区操作
description: 使用 MaxCompute SDK 管理表分区，包括分区的创建、查询、删除和 PartitionSpec 的构造
sidebar_position: 4
module: odps-sdk-core
task: manage-tables
apis:
  - Table
  - Partition
  - PartitionSpec
since: "0.47.0"
keywords:
  - 分区
  - Partition
  - PartitionSpec
  - 创建分区
  - 删除分区
  - 分区列表
---

# 分区操作

本文介绍如何使用 MaxCompute SDK 对分区表进行分区管理，包括分区的查询、创建、删除以及分区规格的构造方法。

## 前置条件

所有分区操作都基于 `Table` 实例：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Odps odps = new Odps(...);
Table table = odps.tables().get("project_name", "partitioned_table");
```

</TabItem>
<TabItem value="python" label="Python">

```python
from odps import ODPS

odps = ODPS(...)
table = odps.get_table('partitioned_table', project='project_name')
```

</TabItem>
<TabItem value="go" label="Go">

```go
odpsClient := odps.NewOdps(...)
table := odpsClient.Table("partitioned_table")
err := table.Load()
```

</TabItem>
</Tabs>

## PartitionSpec 构造

`PartitionSpec` 用于描述一个具体的分区值，是分区操作的核心参数。

### 通过字符串解析构造

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 单级分区
PartitionSpec spec = new PartitionSpec("dt='20240101'");

// 多级分区
PartitionSpec spec = new PartitionSpec("dt='20240101',region='cn'");
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 分区规格以字符串形式表示
# 单级分区
spec = 'dt=20240101'

# 多级分区
spec = 'dt=20240101,region=cn'
```

</TabItem>
<TabItem value="go" label="Go">

```go
// 分区规格以 "key=value/key=value" 格式表示
// 单级分区
spec := "dt=20240101"

// 多级分区
spec := "dt=20240101/region=cn"
```

</TabItem>
</Tabs>

### 通过 key-value 逐个添加

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
PartitionSpec spec = new PartitionSpec();
spec.set("dt", "20240101");
spec.set("region", "cn");
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Python SDK 使用字符串形式指定分区
spec = 'dt=20240101,region=cn'
```

</TabItem>
<TabItem value="go" label="Go">

```go
// Go SDK 使用 "/" 分隔的字符串格式
spec := "dt=20240101/region=cn"
```

</TabItem>
</Tabs>

### trim 参数 (since 0.50.4)

构造 `PartitionSpec` 时，可以指定 `trim` 参数来自动去除分区值两端的空白字符：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// trim=true 会去除分区值前后的空格
PartitionSpec spec = new PartitionSpec("dt=' 20240101 '", true);
// 等价于 dt='20240101'
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Python SDK 自动处理分区值的空白字符
spec = 'dt=20240101'
```

</TabItem>
<TabItem value="go" label="Go">

```go
// Go SDK 使用标准字符串处理
import "strings"
spec := strings.TrimSpace("dt=20240101")
```

</TabItem>
</Tabs>

## 获取单个分区

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Partition partition = table.getPartition(new PartitionSpec("dt='20240101'"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
partition = table.get_partition('dt=20240101')
```

</TabItem>
<TabItem value="go" label="Go">

```go
partition, err := table.GetPartition("dt=20240101")
```

</TabItem>
</Tabs>

获取到分区对象后，可以查询该分区的详细信息：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
long size = partition.getSize();              // 分区存储大小（bytes）
long recordNum = partition.getRecordNum();    // 分区记录数
Date createdTime = partition.getCreatedTime(); // 分区创建时间
```

</TabItem>
<TabItem value="python" label="Python">

```python
partition.reload()
size = partition.size              # 分区存储大小（bytes）
record_num = partition.record_num  # 分区记录数
created_time = partition.creation_time  # 分区创建时间
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := partition.Load()
size := partition.Size()              // 分区存储大小（bytes）
recordNum := partition.RecordNum()    // 分区记录数
createdTime := partition.CreatedTime() // 分区创建时间
```

</TabItem>
</Tabs>

## 获取所有分区

### getPartitions - 获取完整分区信息

返回所有分区的详细信息列表，包括每个分区的大小、记录数等元数据：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
List<Partition> partitions = table.getPartitions();

for (Partition p : partitions) {
    System.out.println("Partition: " + p.getPartitionSpec());
    System.out.println("  Size: " + p.getSize());
    System.out.println("  Records: " + p.getRecordNum());
    System.out.println("  Created: " + p.getCreatedTime());
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
for p in table.partitions:
    print(f'Partition: {p.name}')
    p.reload()
    print(f'  Size: {p.size}')
    print(f'  Records: {p.record_num}')
    print(f'  Created: {p.creation_time}')
```

</TabItem>
<TabItem value="go" label="Go">

```go
partitions, err := table.GetPartitions()
if err != nil {
    log.Fatalf("%+v", err)
}

for _, p := range partitions {
    fmt.Printf("Partition: %s\n", p.Value())
    fmt.Printf("  Size: %d\n", p.Size())
    fmt.Printf("  Records: %d\n", p.RecordNum())
    fmt.Printf("  Created: %s\n", p.CreatedTime())
}
```

</TabItem>
</Tabs>

### getPartitionSpecs - 仅获取分区值（高性能）

与 `getPartitions()` 不同，此方法仅返回分区值，不包含分区的详细元数据信息，因此效率更高。当只需要知道有哪些分区存在时，推荐使用此方法。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
List<PartitionSpec> specs = table.getPartitionSpecs();

for (PartitionSpec spec : specs) {
    System.out.println("Partition spec: " + spec.toString());
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
for p in table.iterate_partitions():
    print(f'Partition spec: {p.name}')
```

</TabItem>
<TabItem value="go" label="Go">

```go
values, err := table.GetPartitionValues()
if err != nil {
    log.Fatalf("%+v", err)
}

for _, v := range values {
    fmt.Printf("Partition spec: %s\n", v)
}
```

</TabItem>
</Tabs>

> **性能提示**：当表分区数量较多时，`getPartitionSpecs()` 比 `getPartitions()` 性能显著更优，因为它不需要加载每个分区的元数据。

## 判断分区是否存在

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
PartitionSpec spec = new PartitionSpec("dt='20240101'");
boolean exists = table.hasPartition(spec);

if (exists) {
    System.out.println("分区已存在");
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
exists = table.exist_partition('dt=20240101')

if exists:
    print('分区已存在')
```

</TabItem>
<TabItem value="go" label="Go">

```go
// Go SDK 通过 GetPartition 返回 error 来判断分区是否存在
_, err := table.GetPartition("dt=20240101")
if err == nil {
    fmt.Println("分区已存在")
}
```

</TabItem>
</Tabs>

## 创建分区

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
PartitionSpec spec = new PartitionSpec("dt='20240115',region='cn'");
table.createPartition(spec);
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.create_partition('dt=20240115,region=cn')
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := table.AddPartition(false, "dt=20240115/region=cn")
```

</TabItem>
</Tabs>

带 `ifNotExists` 参数，分区已存在时不抛出异常：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
table.createPartition(spec, true);
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.create_partition('dt=20240115,region=cn', if_not_exists=True)
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := table.AddPartition(true, "dt=20240115/region=cn")
```

</TabItem>
</Tabs>

## 删除分区

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
PartitionSpec spec = new PartitionSpec("dt='20240101'");
table.deletePartition(spec);
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.delete_partition('dt=20240101')
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := table.DeletePartition(false, "dt=20240101")
```

</TabItem>
</Tabs>

带 `ifExists` 参数，分区不存在时不抛出异常：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
table.deletePartition(spec, true);
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.delete_partition('dt=20240101', if_exists=True)
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := table.DeletePartition(true, "dt=20240101")
```

</TabItem>
</Tabs>

## 完整示例

### 批量创建分区

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void batchCreatePartitions(Odps odps) throws OdpsException {
    Table table = odps.tables().get("my_project", "daily_events");

    // 批量创建最近 7 天的分区
    LocalDate today = LocalDate.now();
    for (int i = 0; i < 7; i++) {
        String dt = today.minusDays(i).format(DateTimeFormatter.BASIC_ISO_DATE);
        PartitionSpec spec = new PartitionSpec("dt='" + dt + "'");

        if (!table.hasPartition(spec)) {
            table.createPartition(spec);
            System.out.println("Created partition: " + spec);
        }
    }
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
from datetime import datetime, timedelta

def batch_create_partitions(odps):
    table = odps.get_table('daily_events', project='my_project')

    # 批量创建最近 7 天的分区
    today = datetime.now()
    for i in range(7):
        dt = (today - timedelta(days=i)).strftime('%Y%m%d')
        spec = f'dt={dt}'

        if not table.exist_partition(spec):
            table.create_partition(spec)
            print(f'Created partition: {spec}')
```

</TabItem>
<TabItem value="go" label="Go">

```go
func batchCreatePartitions(odpsClient *odps.Odps) error {
    table := odpsClient.Table("daily_events")

    // 批量创建最近 7 天的分区
    today := time.Now()
    specs := make([]string, 0, 7)
    for i := 0; i < 7; i++ {
        dt := today.AddDate(0, 0, -i).Format("20060102")
        specs = append(specs, fmt.Sprintf("dt=%s", dt))
    }

    return table.AddPartitions(true, specs)
}
```

</TabItem>
</Tabs>

### 清理过期分区

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void cleanExpiredPartitions(Odps odps, int retentionDays) throws OdpsException {
    Table table = odps.tables().get("my_project", "daily_events");

    LocalDate threshold = LocalDate.now().minusDays(retentionDays);
    List<PartitionSpec> specs = table.getPartitionSpecs();

    for (PartitionSpec spec : specs) {
        String dtValue = spec.get("dt");
        LocalDate partDate = LocalDate.parse(dtValue, DateTimeFormatter.BASIC_ISO_DATE);

        if (partDate.isBefore(threshold)) {
            table.deletePartition(spec, true);
            System.out.println("Deleted expired partition: " + spec);
        }
    }
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
from datetime import datetime, timedelta

def clean_expired_partitions(odps, retention_days):
    table = odps.get_table('daily_events', project='my_project')

    threshold = datetime.now() - timedelta(days=retention_days)
    for p in table.iterate_partitions():
        # 分区名格式: dt=20240101
        dt_value = p.partition_spec['dt']
        part_date = datetime.strptime(dt_value, '%Y%m%d')

        if part_date < threshold:
            table.delete_partition(p.partition_spec, if_exists=True)
            print(f'Deleted expired partition: {p.name}')
```

</TabItem>
<TabItem value="go" label="Go">

```go
func cleanExpiredPartitions(odpsClient *odps.Odps, retentionDays int) error {
    table := odpsClient.Table("daily_events")

    threshold := time.Now().AddDate(0, 0, -retentionDays)
    values, err := table.GetPartitionValues()
    if err != nil {
        return err
    }

    var expired []string
    for _, v := range values {
        // 分区值格式: dt=20240101
        parts := strings.SplitN(v, "=", 2)
        partDate, err := time.Parse("20060102", parts[1])
        if err != nil {
            continue
        }

        if partDate.Before(threshold) {
            expired = append(expired, v)
        }
    }

    if len(expired) > 0 {
        return table.DeletePartitions(true, expired)
    }
    return nil
}
```

</TabItem>
</Tabs>

### 遍历分区统计信息

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void printPartitionStats(Odps odps) throws OdpsException {
    Table table = odps.tables().get("my_project", "user_logs");

    List<Partition> partitions = table.getPartitions();
    long totalSize = 0;
    long totalRecords = 0;

    for (Partition p : partitions) {
        totalSize += p.getSize();
        totalRecords += p.getRecordNum();
    }

    System.out.println("Total partitions: " + partitions.size());
    System.out.println("Total size: " + totalSize + " bytes");
    System.out.println("Total records: " + totalRecords);
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
def print_partition_stats(odps):
    table = odps.get_table('user_logs', project='my_project')

    total_size = 0
    total_records = 0
    count = 0

    for p in table.partitions:
        p.reload()
        total_size += p.size
        total_records += p.record_num
        count += 1

    print(f'Total partitions: {count}')
    print(f'Total size: {total_size} bytes')
    print(f'Total records: {total_records}')
```

</TabItem>
<TabItem value="go" label="Go">

```go
func printPartitionStats(odpsClient *odps.Odps) error {
    table := odpsClient.Table("user_logs")

    partitions, err := table.GetPartitions()
    if err != nil {
        return err
    }

    totalSize := 0
    totalRecords := 0

    for _, p := range partitions {
        totalSize += p.Size()
        totalRecords += p.RecordNum()
    }

    fmt.Printf("Total partitions: %d\n", len(partitions))
    fmt.Printf("Total size: %d bytes\n", totalSize)
    fmt.Printf("Total records: %d\n", totalRecords)
    return nil
}
```

</TabItem>
</Tabs>
