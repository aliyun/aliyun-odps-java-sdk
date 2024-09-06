---
sidebar_position: 5
---

# 使用 Tunnel 进行 Delta Table 的 Upsert 操作

## 简介
本文档指导您如何利用 [TableTunnel](../api-reference/tunnel/TableTunnel.md) 中的 `UpsertSession` 类来高效地执行 Delta Table 的插入或更新操作（Upsert）。Upsert是一种既能插入新记录又能更新现有记录的便捷机制。

## 目录
- [初始化 Upsert 会话](#初始化-upsert-会话)
- [执行 Upsert 操作](#执行-upsert-操作)
- [提交并关闭会话](#提交并关闭会话)
- [完整代码示例](#完整代码示例)
- [注意事项](#注意事项)
- [相关链接](#相关链接)

## 初始化 Upsert 会话
首先，您需通过 `TableTunnel` 的 `buildUpsertSession` 方法初始化一个 Upsert 会话，并配置必要的参数，如项目名、表名、模式名（如果适用）以及分区规格等。
```java
TableTunnel tunnel = odps.tableTunnel();
UpsertSession upsert = tunnel.buildUpsertSession(projectName, tableName)
    .setSchemaName(schemaName)
    .setPartitionSpec(partitionSpec)
    .build();
System.out.println("Upsert Session ID: " + upsert.getId());
```
若需要重新加载已有的 Upsert 会话，可以使用会话ID进行重建。
```java
if(reload) {
    String id = upsert.getId();
    upsert = tunnel.buildUpsertSession(projectName, tableName)
        .setSchemaName(schemaName)
        .setPartitionSpec(partitionSpec)
        .setUpsertId(id)
        .build();
}
```

## 执行 Upsert 操作
接下来，创建 `UpsertStream` 实例，并利用它执行具体的插入或更新记录操作。
```java
try (UpsertStream stream = upsert.buildUpsertStream().setListener(listener).build()) {
    Record record = upsert.newRecord();
    record.setString("key", "0");
    record.setString("value", "v1");
    stream.upsert(record); // 执行 Upsert
    stream.delete(record); // 删除记录示例
    stream.flush(); // 刷新到缓冲区
} catch (Exception e) {
    e.printStackTrace();
}
```

## 提交并关闭会话
```java
long startTime = System.currentTimeMillis();
upsert.commit(false);
long elapsedTime = System.currentTimeMillis() - startTime;
System.out.println("Upsert Session ID: " + upsert.getId() + ", Commit Time: " + elapsedTime + "ms");
upsert.close();
```

:::info
只有当 commit 方法被调用时，数据才可见。记得调用 close 方法，来进行资源清理。
:::

## 完整代码示例
下面是一个综合示例，展示了从初始化到执行 Upsert 操作的完整流程。
```java
import ...; // 导入必要的包

public class UpsertDeltaTableDemo {
    public static void main(String[] args) {
        try {
            TableTunnel tunnel = TableTunnel.newInstance("<Your_ODPS_Instance>");
            String projectName = "<Your_Project_Name>";
            String tableName = "<Your_Table_Name>";
            UpsertSession upsert = tunnel.buildUpsertSession(projectName, tableName)
                .setSchemaName("<Your_Schema_Name>")
                .setPartitionSpec("<Your_Partition_Spec>")
                .setLifecycle(1)
                .build();

            try (UpsertStream stream = upsert.buildUpsertStream().build()) {
                Record record = upsert.newRecord();
                record.setString("key", "exampleKey");
                record.setString("value", "exampleValue");
                stream.upsert(record);
                stream.flush();
            }

            upsert.commit(false);
            System.out.println("Commit successful.");
            upsert.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

## 注意事项
- 由于主键表的写入特性，我们在并发写入同一张表（分区）时，应当谨慎地控制写入逻辑。如果存在多个并发，同时对同一主键进行写入，则可能发生不可预期的行为。
常用的方案是使用 shuffle by pk 操作，将相同主键的记录分配到同一个线程中进行写入。

## 相关链接
- [UpsertSession API 参考](../api-reference/tunnel/UpsertSession.md)
- [UpsertStream API 参考](../api-reference/tunnel/UpsertStream.md)
- [TableTunnel API 参考](../api-reference/tunnel/TableTunnel.md)

通过以上步骤和示例，您可以有效地使用 TableTunnel 的 Upsert 功能来管理 Delta Table 的数据插入与更新。