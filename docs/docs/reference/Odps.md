---
title: Odps
description: MaxCompute Java SDK 的入口类，提供访问各类资源集合的统一接口
sidebar_position: 1
module: odps-sdk-core
apis: [Odps]
keywords: [Odps, 入口, SDK, endpoint, project]
---

# Odps

`Odps` 是 MaxCompute Java SDK 的入口类，通过它可以访问 Tables、Instances、Projects 等所有资源集合。

## 获取实例

```java
Account account = new AliyunAccount("access_id", "access_key");
Odps odps = new Odps(account);
odps.setEndpoint("http://service.odps.aliyun.com/api");
odps.setDefaultProject("my_project");
```

通过已有 Odps 实例拷贝构造：

```java
Odps newOdps = new Odps(odps);
```

通过已有 Odps 实例克隆：

```java
Odps newOdps = odps.clone();
```

## 构造方法

### Odps(Account)

通过 Account 创建 Odps 实例。

```java
public Odps(Account account)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `account` | Account | 主账号实例 |

---

### Odps(Odps)

从已有 Odps 实例拷贝构造，复制所有配置。

```java
public Odps(Odps odps)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `odps` | Odps | 已有的 Odps 实例 |

---

## Account 相关

### setAccount

设置主账号。

```java
public void setAccount(Account account)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `account` | Account | 主账号实例 |

---

### getAccount

获取当前主账号。

```java
public Account getAccount()
```

**返回值**：当前 `Account` 实例

---

## 方法列表

### tables

获取 Tables 集合对象，用于操作项目中的表。

```java
public Tables tables()
```

**返回值**：`Tables` 集合对象

**示例**：

```java
Table table = odps.tables().get("table_name");
Table table = odps.tables().get("project_name", "table_name");
```

---

### instances

获取 Instances 集合对象，用于操作项目中的作业实例。

```java
public Instances instances()
```

**返回值**：`Instances` 集合对象

---

### projects

获取 Projects 集合对象，用于操作项目。

```java
public Projects projects()
```

**返回值**：`Projects` 集合对象

---

### schemas

获取 Schemas 集合对象，用于操作三层模型下的 Schema。

```java
public Schemas schemas()
```

**返回值**：`Schemas` 集合对象

---

### functions

获取 Functions 集合对象，用于操作用户自定义函数。

```java
public Functions functions()
```

**返回值**：`Functions` 集合对象

---

### resources

获取 Resources 集合对象，用于操作项目中的资源。

```java
public Resources resources()
```

**返回值**：`Resources` 集合对象

---

### volumes

获取 Volumes 集合对象，用于操作项目中的 Volume。

```java
public Volumes volumes()
```

**返回值**：`Volumes` 集合对象

---

### quotas

获取 Quotas 集合对象，用于操作 Quota 配额。

```java
public Quotas quotas()
```

**返回值**：`Quotas` 集合对象

---

### logview

获取 LogView 工具实例，用于生成作业日志链接。

```java
public LogView logview()
```

**返回值**：`LogView` 工具实例

---

### xFlows

获取 XFlows 集合对象，用于操作 XFlow 任务。

```java
public XFlows xFlows()
```

**返回值**：`XFlows` 集合对象

---

### tenant

获取 Tenant 实例，用于访问租户级信息。

```java
public Tenant tenant()
```

**返回值**：`Tenant` 实例

---

### setEndpoint

设置 MaxCompute 服务的 Endpoint 地址。

```java
public void setEndpoint(String endpoint)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `endpoint` | String | MaxCompute 服务地址，如 `http://service.odps.aliyun.com/api` |

---

### setDefaultProject

设置默认 Project 名称。SDK 中多数方法都有两个版本：一个需要传入 projectName，另一个使用默认 Project。

```java
public void setDefaultProject(String defaultProject)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `defaultProject` | String | 默认 Project 名称 |

---

### setCurrentSchema

设置当前 Schema（三层模型）。

```java
public void setCurrentSchema(String schema)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `schema` | String | Schema 名称，null 或空字符串表示使用默认 Schema |

---

### setTunnelEndpoint

设置 Tunnel 服务端点。未设置时将自动路由。

```java
public void setTunnelEndpoint(String tunnelEndpoint)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `tunnelEndpoint` | String | Tunnel 服务端点 URL |

---

### getEndpoint

获取 MaxCompute 服务端点地址。

```java
public String getEndpoint()
```

**返回值**：服务端点 URL 字符串

---

### getTunnelEndpoint

获取 Tunnel 服务端点地址。

```java
public String getTunnelEndpoint()
```

**返回值**：Tunnel 端点 URL 字符串

---

### getDefaultProject

获取默认项目名称。

```java
public String getDefaultProject()
```

**返回值**：默认 Project 名称

---

### getCurrentSchema

获取当前 Schema 名称。

```java
public String getCurrentSchema()
```

**返回值**：当前 Schema 名称，未设置时返回 null

---

### setUserAgent

设置 User-Agent 标识，用于追踪 SDK 调用来源。

```java
public void setUserAgent(String userAgent)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `userAgent` | String | 自定义 User-Agent 标识 |

---

### getUserAgent

获取当前 User-Agent 标识。

```java
public String getUserAgent()
```

**返回值**：User-Agent 标识字符串

---

### getLogViewHost

获取 LogView 服务地址。

```java
public String getLogViewHost()
```

**返回值**：LogView 服务 URL 字符串

---

### setLogViewHost

设置 LogView 服务地址。

```java
public void setLogViewHost(String host)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `host` | String | LogView 服务地址 |

---

### options

获取全局配置选项对象。

```java
public OdpsOptions options()
```

**返回值**：`OdpsOptions` 全局配置对象

---

### tableTunnel

创建 TableTunnel 实例。

```java
public TableTunnel tableTunnel()
public TableTunnel tableTunnel(Configuration configuration)
```

**返回值**：`TableTunnel` 实例

**示例**：

```java
TableTunnel tunnel = odps.tableTunnel();
```

---

### clone

克隆当前 Odps 实例，复制所有配置。

```java
public Odps clone()
```

**返回值**：新的 `Odps` 实例
