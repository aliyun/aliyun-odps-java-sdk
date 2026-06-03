---
title: 第一个程序
description: 编写第一个 MaxCompute SDK 程序，连接服务并读取表数据。
sidebar_position: 3
module: odps-sdk-core
task: getting-started
apis: [Odps, Table, RecordReader, Record]
keywords: [快速开始, 入门, 读取数据, 第一个程序, example]
---

# 第一个程序

本文通过一个完整的示例程序，带你快速体验 MaxCompute SDK 的基本用法：连接 MaxCompute 服务并读取表中的数据。

## 前置条件

- 已完成 [SDK 安装](./installation.md)
- 已配置 [认证凭据](./authentication.md)
- 拥有一个 MaxCompute 项目和至少一张包含数据的表

## 完整代码

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

```python
from odps import ODPS

o = ODPS(
    access_id='your-access-id',
    secret_access_key='your-access-key',
    project='your-project',
    endpoint='http://service.odps.aliyun.com/api',
)

# 列出表
for table in o.list_tables():
    print(table.name)

# 执行 SQL
with o.execute_sql('SELECT * FROM my_table LIMIT 10').open_reader() as reader:
    for record in reader:
        print(record)
```

</TabItem>
<TabItem value="go" label="Go">

```go
package main

import (
    "fmt"
    "github.com/aliyun/aliyun-odps-go-sdk/odps"
    "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

func main() {
    aliAccount := account.NewAliyunAccount("your-access-id", "your-access-key")
    odpsIns := odps.NewOdps(aliAccount, "http://service.odps.aliyun.com/api")
    odpsIns.SetDefaultProjectName("your-project")

    // 列出表
    tables := odpsIns.Tables()
    tables.List(func(t *odps.Table, err error) {
        fmt.Println(t.Name())
    })

    // 执行 SQL
    ins, _ := odpsIns.ExecSQl("SELECT * FROM my_table LIMIT 10;")
    ins.WaitForSuccess()
}
```

</TabItem>
</Tabs>

## 运行前配置

设置环境变量：

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID="your_access_key_id"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="your_access_key_secret"
```

修改代码中的以下内容：
- Endpoint - 替换为你的项目所在地域的 Endpoint
- 项目名称 - 替换为你的项目名称
- 表名 - 替换为你要读取的表名

## 代码解析

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

### 第一步：初始化客户端

```python
from odps import ODPS

o = ODPS(
    access_id='your-access-id',
    secret_access_key='your-access-key',
    project='your-project',
    endpoint='http://service.odps.aliyun.com/api',
)
```

`ODPS` 是 PyODPS 的入口类，构造时传入认证信息、项目名称和 Endpoint。也可以通过环境变量自动检测凭据。

### 第二步：列出表

```python
for table in o.list_tables():
    print(table.name)
```

`list_tables()` 返回当前项目下所有表的迭代器。

### 第三步：执行 SQL 并读取结果

```python
with o.execute_sql('SELECT * FROM my_table LIMIT 10').open_reader() as reader:
    for record in reader:
        print(record)
```

`execute_sql()` 提交 SQL 任务并等待完成，`open_reader()` 打开结果读取器，以迭代器模式逐行返回数据。

</TabItem>
<TabItem value="go" label="Go">

### 第一步：初始化客户端

```go
aliAccount := account.NewAliyunAccount("your-access-id", "your-access-key")
odpsIns := odps.NewOdps(aliAccount, "http://service.odps.aliyun.com/api")
odpsIns.SetDefaultProjectName("your-project")
```

创建 Account 实例和 Odps 客户端，设置 Endpoint 和默认项目。

### 第二步：列出表

```go
tables := odpsIns.Tables()
tables.List(func(t *odps.Table, err error) {
    fmt.Println(t.Name())
})
```

通过 `Tables()` 获取表管理器，`List` 以回调方式遍历所有表。

### 第三步：执行 SQL

```go
ins, _ := odpsIns.ExecSQl("SELECT * FROM my_table LIMIT 10;")
ins.WaitForSuccess()
```

`ExecSQl` 提交 SQL 任务并返回实例，`WaitForSuccess()` 等待任务执行完成。

</TabItem>
</Tabs>

## 运行程序

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

使用 Maven 编译并运行：

```bash
mvn compile exec:java -Dexec.mainClass="FirstProgram"
```

或者先编译再运行：

```bash
mvn compile
java -cp target/classes:$(mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout) FirstProgram
```

</TabItem>
<TabItem value="python" label="Python">

直接运行 Python 脚本：

```bash
python first_program.py
```

</TabItem>
<TabItem value="go" label="Go">

使用 Go 运行：

```bash
go run main.go
```

</TabItem>
</Tabs>

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

确认表名拼写正确，且表属于指定的项目。如果表在其他项目中，需要指定项目名称。

## 下一步

- [构建 ODPS 客户端](./authentication.md) - 了解更多客户端配置选项
- [数据预览](../guides/read-data/preview.md) - 更多数据读取方式
- [SQL 执行](../guides/execute-sql/index.md) - 通过 SDK 执行 SQL 语句
