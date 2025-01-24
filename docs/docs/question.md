---
title: 常见问题
sidebar_position: 6
---

## 问题排查指南
**首先判断是服务端问题还是客户端问题**

非 SQL 作业：
- 是否有 requestId：[如何获取 requestID](#如何获取-requestid)

SQL 作业：
- 查看 Logview：[如何获取 Logview](#如何获取-logview)

对于服务端问题，请咨询 MaxCompute 技术支持。
对于客户端问题，请提交 Github issue

:::note
如果未升级 SDK，客户端通常不会出现突发问题（原行为正常，突然行为异常）

如发现客户端问题，可以尝试升级 SDK 解决，你的问题可能在新版本中已修复，详情可以参考[更新日志](changelog.md)
:::


## 如何获取 Logview
对于 MaxCompute 中的每一个 Instance，都可以生成 一个 Logview，用于查看 Instance 的运行日志

### Logview V2（推荐）
```java
Instance i = SQLTask.run(odps, "select 1;");
Logview logview = new Logview(odps, 2);
// 生成一个过期时间为 3*24 小时的 Logview
String logview = odps.logview().generateLogView(i, 3 * 24);
// 打印 Logview
System.out.println(logview);
```

### Logview V1（默认行为）
```java
// 在执行 SQL 或其他作业时，通常会返回一个 Instance
Instance i = SQLTask.run(odps, "select 1;");
// 生成一个过期时间为 3*24 小时的 Logview
String logview = odps.logview().generateLogView(i, 3 * 24);
// 打印 Logview
System.out.println(logview);
```

## 如何获取 requestID
对于 MaxCompute 中的每个请求，如果发生问题，均会返回 OdpsException，你可以从 OdpsException 中获取 requestID
```java
try {
  // do something
} catch (OdpsException e) {
  System.out.println(e.getRequestId());
}
```

## 其他问题
其他常见问题可以参考：https://help.aliyun.com/zh/maxcompute/user-guide/faq-about-sdk-for-java