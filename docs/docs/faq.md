---
title: 常见问题
description: MaxCompute Java SDK 常见问题与解答
sidebar_position: 99
keywords:
  - FAQ
  - 常见问题
---

# 常见问题

## 问题排查

### 如何判断是服务端还是客户端问题?

**非 SQL 作业**：检查是否有 requestId，参考[如何获取 requestId](#如何获取-requestid)。

**SQL 作业**：查看 Logview 日志，参考[如何获取 Logview](#如何获取-logview)。

- 服务端问题请咨询 MaxCompute 技术支持
- 客户端问题请提交 [GitHub Issue](https://github.com/aliyun/aliyun-odps-java-sdk/issues)

:::tip
如果未升级 SDK，客户端通常不会出现突发问题。如遇异常，可尝试升级 SDK，问题可能在新版本中已修复，详见[更新日志](changelog.md)。
:::

### 如何获取 Logview?

每个 MaxCompute Instance 都可以生成 Logview，用于查看运行日志。

**Logview V2（推荐）**：

```java
Instance i = SQLTask.run(odps, "select 1;");
Logview logview = new Logview(odps, 2);
String logviewUrl = logview.generateLogView(i, 3 * 24);
System.out.println(logviewUrl);
```

**Logview V1**：

```java
Instance i = SQLTask.run(odps, "select 1;");
String logviewUrl = odps.logview().generateLogView(i, 3 * 24);
System.out.println(logviewUrl);
```

### 如何获取 requestId?

当请求发生异常时，可以从 `OdpsException` 中获取 requestId：

```java
try {
    // 业务逻辑
} catch (OdpsException e) {
    System.out.println(e.getRequestId());
}
```

---

## 版本与兼容性

### SDK 支持哪些 Java 版本?

| Java 版本 | 支持状态 |
|-----------|---------|
| Java 8    | 完全支持（编译目标版本） |
| Java 11   | 完全支持 |
| Java 17   | 支持，需要添加 JVM 参数开放模块访问 |
| Java 21   | 支持，需要添加 JVM 参数开放模块访问 |

Java 17+ 需要添加的 JVM 参数示例：

```
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

### Shaded JAR 和普通 JAR 有什么区别?

SDK 发布了两种 artifact：

- **`odps-sdk-core`**：普通 JAR，依赖的第三方库（如 Guava、Jackson、Protobuf 等）需要由用户项目管理，可能产生版本冲突。
- **`odps-sdk-core` (classifier: shaded)**：将主要第三方依赖重命名打包，避免与用户项目中的同名依赖冲突。推荐在依赖冲突场景下使用。

---

## 数据读写

### 应该使用 Tunnel 还是 Storage API?

| 对比维度 | Tunnel | Storage API |
|---------|--------|-------------|
| 数据格式 | SDK 自有格式 | Apache Arrow 列式格式 |
| 适用场景 | 通用数据上传/下载 | 高性能批量读写、与大数据生态集成 |
| 成熟度 | 成熟稳定 | 较新，性能更优 |
| 学习成本 | 较低 | 需了解 Arrow 格式 |

**建议**：新项目优先使用 Storage API 获得更好的性能；已有 Tunnel 代码且运行稳定的项目无需迁移。

---

## 连接与配置

### 如何配置连接超时时间?

通过 `RestClient` 设置连接和读取超时：

```java
Odps odps = new Odps(account);
odps.getRestClient().setConnectTimeout(60);  // 连接超时，单位秒
odps.getRestClient().setReadTimeout(300);    // 读取超时，单位秒
```

### 如何配置重试策略?

SDK 默认会对网络异常进行重试，可自定义重试次数：

```java
odps.getRestClient().setRetryTimes(5);  // 设置重试次数
```

---

## 常见错误

### ODPS-0410051: Invalid credentials

AccessKey ID 或 AccessKey Secret 不正确。请检查：
1. AccessKey 是否正确复制（注意首尾空格）
2. AccessKey 是否已被禁用
3. 是否使用了正确的 Endpoint

### ODPS-0422155: Query timed out

SQL 执行超时。可能原因：
1. 数据量过大，建议添加过滤条件或分区裁剪
2. 集群资源不足，请联系管理员

### NoSuchMethodError / ClassNotFoundException

通常是依赖冲突导致。解决方案：
1. 使用 `mvn dependency:tree` 排查冲突依赖
2. 使用 shaded 版本的 SDK
3. 在 pom.xml 中通过 `<exclusions>` 排除冲突的传递依赖

---

## 更多资源

- [阿里云官方 FAQ](https://help.aliyun.com/zh/maxcompute/user-guide/faq-about-sdk-for-java)
- [GitHub Issues](https://github.com/aliyun/aliyun-odps-java-sdk/issues)
- [更新日志](changelog.md)
