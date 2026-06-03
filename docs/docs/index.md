---
title: MaxCompute SDK
description: MaxCompute(ODPS) SDK for Java / Python / Go - 阿里云大数据计算服务的多语言客户端库
slug: /
sidebar_position: 1
keywords:
  - MaxCompute
  - ODPS
  - Java SDK
  - Python SDK
  - Go SDK
  - 大数据
---

# MaxCompute SDK

MaxCompute(ODPS) SDK 是阿里云 MaxCompute 大数据计算服务的官方客户端库，支持 **Java**、**Python** 和 **Go** 三种语言。它为开发者提供了完整的 API，用于管理 MaxCompute 项目中的表、资源、函数等对象，执行 SQL 查询，以及通过 Tunnel 或 Storage API 高效地读写数据。

## 安装

<Tabs groupId="sdk-language">
  <TabItem value="java" label="Java" default>

```xml
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-core</artifactId>
    <version>0.57.2-public</version>
</dependency>
```

  </TabItem>
  <TabItem value="python" label="Python">

```bash
pip install pyodps
```

  </TabItem>
  <TabItem value="go" label="Go">

```bash
go get github.com/aliyun/aliyun-odps-go-sdk
```

  </TabItem>
</Tabs>

## 模块概览（Java）

| 模块 | artifactId | 说明 |
|------|-----------|------|
| Core | `odps-sdk-core` | 核心模块，提供 MaxCompute 资源管理、SQL 执行、Tunnel 数据通道等功能 |
| Storage API | `odps-sdk-storage-api` | 基于 Arrow 格式的高性能数据读写接口 |
| Table API | `odps-sdk-table-api` | 表级别的数据读写高层封装 |
| UDF | `odps-sdk-udf` | 用户自定义函数(UDF/UDTF/UDAF)开发框架 |

## 文档导航

- [**快速开始**](getting-started/) - 环境配置与第一个程序
- [**使用指南**](guides/) - SQL 执行、数据读写、表管理等操作指南
- [**模块文档**](modules/) - 各模块的详细 API 说明
- [**API 参考**](reference/) - 类与方法的完整参考文档（Java）
- [**常见问题**](faq) - FAQ 与问题排查

## 相关链接

- [Java SDK GitHub](https://github.com/aliyun/aliyun-odps-java-sdk)
- [Python SDK GitHub](https://github.com/aliyun/aliyun-odps-python-sdk)
- [Go SDK GitHub](https://github.com/aliyun/aliyun-odps-go-sdk)
