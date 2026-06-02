---
title: Core 核心模块
description: MaxCompute Java SDK 核心模块，提供服务入口和资源管理能力
sidebar_position: 1
module: odps-sdk-core
keywords: [odps, core, table, instance, project, schema, function, resource, sqltask]
---

# Core 核心模块

`odps-sdk-core` 是 MaxCompute Java SDK 的核心模块，提供与 MaxCompute 服务交互的基础能力。通过该模块，用户可以管理项目空间中的各类资源（表、实例、函数、资源文件等），提交和管理 SQL 任务，以及进行数据通道（Tunnel）操作。

该模块是所有其他 SDK 模块的基础依赖，提供了统一的认证、网络通信和资源抽象层。

## Maven 依赖

```xml
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-core</artifactId>
    <version>${odps.sdk.version}</version>
</dependency>
```

## 核心功能

| 功能 | 说明 |
|------|------|
| **认证管理** | 支持 AliyunAccount、StsAccount、BearerTokenAccount 等多种认证方式 |
| **项目管理** | 创建、查询、配置 MaxCompute 项目空间 |
| **表管理** | 表的创建、删除、Schema 查询、分区管理等 |
| **实例管理** | SQL/任务实例的提交、状态查询、结果获取 |
| **函数管理** | 自定义函数的注册和管理 |
| **资源管理** | Jar、File、Table 等资源的上传和管理 |
| **SQL 执行** | 通过 SQLTask 提交和执行 SQL 语句 |
| **Schema 管理** | MaxCompute Schema（命名空间）管理 |
| **Tunnel 集成** | 内置 TableTunnel 数据通道支持 |

## 核心类

| 类 | 说明 |
|------|------|
| `Odps` | SDK 入口类，持有认证信息和服务配置，提供访问各类资源集合的方法 |
| `Table` | 表对象，提供 Schema 查询、分区管理、生命周期管理等操作 |
| `Instance` | 实例对象，代表一个提交到 MaxCompute 的任务 |
| `Project` | 项目空间对象，管理项目级别的配置和属性 |
| `Schema` | Schema（命名空间）对象，用于组织表、函数等资源 |
| `Function` | 自定义函数对象 |
| `Resource` | 资源对象，包括 Jar、File、Table 等类型 |
| `SQLTask` | SQL 任务工具类，提供便捷的 SQL 提交和执行方法 |

## 配置

### 基础配置

```java
Account account = new AliyunAccount("accessId", "accessKey");
Odps odps = new Odps(account);
odps.setEndpoint("http://service.cn-hangzhou.maxcompute.aliyun.com/api");
odps.setDefaultProject("my_project");
odps.setCurrentSchema("my_schema");
```

### RestClient 配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `connectTimeout` | 连接超时时间 | 10 秒 |
| `readTimeout` | 读取超时时间 | 120 秒 |
| `retryTimes` | 重试次数 | 3 |

### OdpsOptions

通过 `Odps` 对象的 `getOptions()` 方法获取和设置 SDK 全局选项。

## 使用示例

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;

// 1. 初始化 Odps 对象
Account account = new AliyunAccount("accessId", "accessKey");
Odps odps = new Odps(account);
odps.setEndpoint("http://service.cn-hangzhou.maxcompute.aliyun.com/api");
odps.setDefaultProject("my_project");

// 2. 访问表
for (Table table : odps.tables()) {
    System.out.println(table.getName());
}

// 3. 获取表 Schema
Table table = odps.tables().get("my_table");
table.reload();
System.out.println(table.getSchema());

// 4. 执行 SQL
Instance instance = SQLTask.run(odps, "SELECT * FROM my_table LIMIT 10;");
instance.waitForSuccess();
```

## 相关文档

- [初始化 Odps 对象](../getting-started/authentication.md)
- [Tunnel 模块](./tunnel.md)
- [Storage API 模块](./storage-api.md)
