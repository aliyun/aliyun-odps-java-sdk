---
title: 第一个程序
description: 编写第一个 MaxCompute Java SDK 程序，连接服务并读取表数据。
sidebar_position: 3
module: odps-sdk-core
task: getting-started
apis: [Odps, Table, RecordReader, Record]
keywords: [快速开始, 入门, 读取数据, 第一个程序, example]
---

# 第一个程序

本文通过一个完整的示例程序，带你快速体验 MaxCompute Java SDK 的基本用法：连接 MaxCompute 服务并读取表中的数据。

## 前置条件

- 已完成 [SDK 安装](./installation.md)
- 已配置 [认证凭据](./authentication.md)
- 拥有一个 MaxCompute 项目和至少一张包含数据的表

## 完整代码

```java
import com.aliyun.odps.Account;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

import java.io.IOException;

public class FirstProgram {

    public static void main(String[] args) throws OdpsException, IOException {
        // 1. 从环境变量读取凭据
        String accessKeyId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
        String accessKeySecret = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");

        if (accessKeyId == null || accessKeySecret == null) {
            System.err.println("请设置环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 ALIBABA_CLOUD_ACCESS_KEY_SECRET");
            System.exit(1);
        }

        // 2. 创建认证账号并初始化客户端
        Account account = new AliyunAccount(accessKeyId, accessKeySecret);
        Odps odps = new Odps(account);
        odps.setEndpoint("http://service.cn-shanghai.maxcompute.aliyun.com/api");
        odps.setDefaultProject("your_project");

        // 3. 获取表实例
        Table table = odps.tables().get("your_table");

        // 4. 读取前 10 行数据
        RecordReader reader = table.read(10);
        Record record;
        while ((record = reader.read()) != null) {
            System.out.println(formatRecord(record));
        }
        reader.close();

        System.out.println("读取完成");
    }

    /**
     * 将一行记录格式化为可读字符串
     */
    private static String formatRecord(Record record) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < record.getColumnCount(); i++) {
            if (i > 0) {
                sb.append("\t");
            }
            Object value = record.get(i);
            if (value instanceof byte[]) {
                sb.append(new String((byte[]) value));
            } else {
                sb.append(value != null ? value.toString() : "NULL");
            }
        }
        return sb.toString();
    }
}
```

## 运行前配置

设置环境变量：

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID="your_access_key_id"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="your_access_key_secret"
```

修改代码中的以下内容：
- `setEndpoint` - 替换为你的项目所在地域的 Endpoint
- `setDefaultProject` - 替换为你的项目名称
- `get("your_table")` - 替换为你要读取的表名

## 代码解析

### 第一步：读取凭据

```java
String accessKeyId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
String accessKeySecret = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
```

从环境变量读取 AccessKey，避免将敏感信息硬编码在代码中。更多认证方式参见[认证方式](./authentication.md)。

### 第二步：初始化客户端

```java
Account account = new AliyunAccount(accessKeyId, accessKeySecret);
Odps odps = new Odps(account);
odps.setEndpoint("http://service.cn-shanghai.maxcompute.aliyun.com/api");
odps.setDefaultProject("your_project");
```

`Odps` 是 SDK 的入口类，通过它可以访问 MaxCompute 的所有资源：表、函数、资源、任务等。创建时需要提供认证账号、服务 Endpoint 和默认项目名。

### 第三步：获取表实例

```java
Table table = odps.tables().get("your_table");
```

通过 `odps.tables()` 获取表集合管理器，再通过 `get()` 获取指定表的实例。此操作是惰性的，不会立即发起网络请求。

### 第四步：读取数据

```java
RecordReader reader = table.read(10);
Record record;
while ((record = reader.read()) != null) {
    System.out.println(formatRecord(record));
}
reader.close();
```

`table.read(10)` 从表中读取前 10 行数据。`RecordReader` 以迭代器模式逐行返回 `Record` 对象，每个 `Record` 包含一行数据的所有列值。

## 运行程序

使用 Maven 编译并运行：

```bash
mvn compile exec:java -Dexec.mainClass="FirstProgram"
```

或者先编译再运行：

```bash
mvn compile
java -cp target/classes:$(mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout) FirstProgram
```

预期输出（具体内容取决于你的表数据）：

```
value1  value2  value3
value4  value5  value6
...
读取完成
```

## 常见问题

### 连接超时

确认 Endpoint 地址正确，且网络可以访问该地址。如果在 VPC 内，需要使用 VPC Endpoint。

### 权限不足

确认 AccessKey 对应的 RAM 用户拥有目标项目和表的访问权限。

### 表不存在

确认表名拼写正确，且表属于 `setDefaultProject` 指定的项目。如果表在其他项目中，使用 `odps.tables().get("other_project", "table_name")`。

## 下一步

- [构建 ODPS 客户端](../core-concept/init-odps-client.md) - 了解更多客户端配置选项
- [数据预览](../core-concept/table-read.md) - 更多数据读取方式
- [SQL 执行](../core-concept/sql.md) - 通过 SDK 执行 SQL 语句
