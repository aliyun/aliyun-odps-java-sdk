---
title: UDF 开发模块
description: MaxCompute 用户自定义函数（UDF/UDTF/UDAF）开发支持模块
sidebar_position: 4
module: odps-sdk-udf
keywords: [udf, udtf, udaf, aggregator, vectorized, extractor, outputer]
---

# UDF 开发模块

`odps-sdk-udf` 是 MaxCompute 用户自定义函数的开发支持模块，提供 UDF（标量函数）、UDTF（表值函数）、UDAF（聚合函数）的基类和运行时接口。该模块还支持向量化（Vectorized）批处理模式，通过基于 Apache Arrow 的批量数据处理显著提升执行性能。

## Maven 依赖

```xml
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-udf</artifactId>
    <version>${odps.sdk.version}</version>
    <scope>provided</scope>
</dependency>
```

> **注意**：UDF 开发时使用 `provided` scope，因为运行时环境由 MaxCompute 计算集群提供。

## 核心功能

| 功能 | 说明 |
|------|------|
| **标量函数 (UDF)** | 一对一映射，输入一行输出一个值 |
| **表值函数 (UDTF)** | 一对多映射，输入一行可输出多行 |
| **聚合函数 (UDAF)** | 多对一映射，对多行数据进行聚合计算 |
| **向量化处理** | 基于 Arrow 的批处理模式，减少函数调用开销 |
| **自定义 Extractor** | 自定义外部数据源读取逻辑 |
| **自定义 Outputer** | 自定义外部数据源写入逻辑 |
| **数据分片** | 通过 InputSplitter 自定义数据分片策略 |

## 核心类

| 类 | 说明 |
|------|------|
| `UDF` | 标量函数基类，实现 `evaluate` 方法定义计算逻辑 |
| `UDTF` | 表值函数基类，通过 `process` 方法处理输入并通过 `forward` 输出多行 |
| `Aggregator` | 聚合函数基类，实现 `iterate`/`merge`/`terminate` 定义聚合逻辑 |
| `VectorizedExtractor` | 向量化数据读取器，批量处理 Arrow 格式数据 |
| `VectorizedOutputer` | 向量化数据写入器，批量输出 Arrow 格式数据 |
| `VectorizedStorageHandler` | 向量化存储处理器，组合 Extractor 和 Outputer |
| `InputSplitter` | 数据分片接口，自定义外部数据的分片策略 |
| `ExecutionContext` | 运行时上下文，提供任务参数和资源访问能力 |

## 配置

### 与 Core 模块的关系

UDF 模块专注于函数开发，不依赖 `odps-sdk-core`。函数的注册和管理（如 `CREATE FUNCTION`）通过 Core 模块的 `Function` 类完成：

```java
// 使用 Core 模块注册 UDF（非 UDF 开发代码）
odps.functions().create(functionInfo);
```

### 开发环境配置

UDF 开发项目的典型依赖配置：

```xml
<dependencies>
    <dependency>
        <groupId>com.aliyun.odps</groupId>
        <artifactId>odps-sdk-udf</artifactId>
        <version>${odps.sdk.version}</version>
        <scope>provided</scope>
    </dependency>
    <!-- 向量化 UDF 需要 Arrow 依赖 -->
    <dependency>
        <groupId>org.apache.arrow</groupId>
        <artifactId>arrow-vector</artifactId>
        <version>1.0.0</version>
        <scope>provided</scope>
    </dependency>
</dependencies>
```

## 使用示例

### 标量函数 (UDF)

```java
import com.aliyun.odps.udf.UDF;

public class MyPlus extends UDF {
    public Long evaluate(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }
}
```

注册并使用：

```sql
CREATE FUNCTION my_plus AS 'com.example.MyPlus' USING 'my_udf.jar';
SELECT my_plus(col1, col2) FROM my_table;
```

### 表值函数 (UDTF)

```java
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.UDTFCollector;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({"string->string,string"})
public class SplitKV extends UDTF {
    @Override
    public void process(Object[] args) throws UDFException {
        String input = (String) args[0];
        if (input == null) return;
        String[] parts = input.split("=", 2);
        if (parts.length == 2) {
            forward(parts[0], parts[1]);
        }
    }
}
```

### 聚合函数 (UDAF)

```java
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.annotation.Resolve;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Writable;

@Resolve({"bigint->bigint"})
public class MySum extends Aggregator {
    @Override
    public Writable newBuffer() {
        return new LongWritable(0L);
    }

    @Override
    public void iterate(Writable buffer, Writable[] args) {
        LongWritable sum = (LongWritable) buffer;
        LongWritable value = (LongWritable) args[0];
        if (value != null) {
            sum.set(sum.get() + value.get());
        }
    }

    @Override
    public void merge(Writable buffer, Writable partial) {
        LongWritable sum = (LongWritable) buffer;
        LongWritable partialSum = (LongWritable) partial;
        sum.set(sum.get() + partialSum.get());
    }

    @Override
    public Writable terminate(Writable buffer) {
        return buffer;
    }
}
```

### 向量化处理

向量化模式通过批量处理 Arrow 格式数据来提升性能，适用于大数据量场景：

```java
import com.aliyun.odps.udf.VectorizedExtractor;
import org.apache.arrow.vector.VectorSchemaRoot;

public class MyVectorizedExtractor extends VectorizedExtractor {
    @Override
    public VectorSchemaRoot extract() throws IOException {
        // 批量读取并返回 Arrow 格式数据
        // 返回 null 表示数据读取完毕
        return root;
    }
}
```

## 相关文档

- [Core 核心模块](./core.md) - 函数注册和管理
- [MaxCompute UDF 开发指南](https://help.aliyun.com/document_detail/27867.html)
