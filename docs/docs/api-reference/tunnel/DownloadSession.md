---
title: Download
sidebar_position: 3
---
通过 Download Session，用户可以下载任何表/分区的数据。

## 会话创建流程
我们假设用户已经初始化好了 [TableTunnel](TableTunnel.md) 对象，接下来我们介绍如何创建下载会话。
### 基础同步创建
```java
// 构建基础下载会话（立即返回）
DownloadSession session = tableTunnel.buildDownloadSession()
    .setProjectName("analytics_logs")   // 必须参数：项目名称
    .setTableName("user_behavior")      // 必须参数：表名称
    .setPartitionSpec(new PartitionSpec("dt=20231001"))    // 可选参数：分区表达式
    .build();
```

### 异步创建模式
```java
// 异步构建适用于大表场景
DownloadSessionBuilder builder = tableTunnel.buildDownloadSession()
    .setProjectName("iot_data")
    .setTableName("sensor_readings")
    .setAsyncMode(true);  // 启用异步模式

DownloadSession asyncSession = builder.build();

// 轮询等待会话就绪
boolean success = builder.wait(asyncSession, 5, 300);  // 每5秒检查/最长5分钟
if (success) {
    System.out.println("异步会话就绪，记录数：" + asyncSession.getRecordCount());
}
```

## 核心方法详解

### 1. 会话构建器（DownloadSessionBuilder）

#### 参数说明表

| 方法签名                              | 参数类型          | 必需 | 默认值   | 说明                  |
|-----------------------------------|---------------|----|-------|---------------------|
| `setProjectName(String)`          | String        | 是  | -     | 目标数据所在项目名称          |
| `setTableName(String)`            | String        | 是  | -     | 需要下载的表名称            |
| `setPartitionSpec(PartitionSpec)` | PartitionSpec | 否  | null  | 分区表达式，格式`key=value` |
| `setShardId(Long)`                | Long          | 否  | null  | 指定分片ID精确下载          |
| `setAsyncMode(boolean)`           | boolean       | 否  | false | 启用异步初始化模式           |
| `setWaitAsyncBuild(boolean)`      | boolean       | 否  | false | 是否阻塞等待异步会话就绪        |

### 2. 打开数据读取器（openRecordReader）

#### 方法重载列表
```java
// 基础形式
TunnelRecordReader openRecordReader(long start, long count)

// 带压缩选项
TunnelRecordReader openRecordReader(long start, long count, CompressOption option)

// 列投影+压缩+版本检查
TunnelRecordReader openRecordReader(
    long start, 
    long count, 
    CompressOption option,
    List<Column> columns,
    boolean disableModifiedCheck
)
```

#### 参数详解

| 参数                   | 类型             | 约束 | 说明                 |
|----------------------|----------------|----|--------------------|
| start                | long           | ≥0 | 读取起始位置（行号）         |
| count                | long           | ≥1 | 读取记录数量             |
| option               | CompressOption | -  | 压缩配置对象             |
| columns              | List           | 非空 | 需下载的列集合            |
| disableModifiedCheck | boolean        | -  | 禁用数据版本校验（可能读取过期数据） |

#### 返回值特性
- `TunnelRecordReader` 对象：提供迭代式读取接口
- 自动资源管理：建议使用try-with-resources语法
- 超时机制：默认300秒无数据触发服务端断开

### 3. 分块下载示例
```java
long totalRecords = session.getRecordCount();
int parallelism = 8;  // 根据CPU核心数设置

ExecutorService pool = Executors.newFixedThreadPool(parallelism);
for (int i = 0; i < parallelism; i++) {
    long start = i * (totalRecords / parallelism);
    long count = (i == parallelism-1) ? 
                totalRecords - start : 
                totalRecords / parallelism;
    
    pool.submit(() -> {
        try (TunnelRecordReader reader = session.openRecordReader(start, count)) {
            while (reader.hasNext()) {
                Record record = reader.next();
                processRecord(record);
            }
        }
    });
}
```

## 数据读取模式

### 标准记录读取
```java
try (TunnelRecordReader reader = session.openRecordReader(0, 1000)) {
    while (reader.hasNext()) {
        Record record = reader.next();
        System.out.println(record.get("user_id"));
    }
}
```

### 高性能Arrow读取
```java
try (ArrowRecordReader arrowReader = session.openArrowRecordReader(0, 50000)) {
    VectorSchemaRoot root = arrowReader.read();
    FieldVector idVector = root.getVector("user_id");
    
    // 使用Apache Arrow内存格式处理
    for (int i = 0; i < root.getRowCount(); i++) {
        System.out.println(idVector.getObject(i));
    }
}
```

## Transactional 表下载注意事项

对于 Transactional 表，下载会话的创建和读取方式与普通表不通。需要注意的是，对于 Transactional 表，getRecordCount() 返回的是当前表的分片，而不是表的总记录数。
对应的，每次仅支持下载1个分片，而非任意数量。