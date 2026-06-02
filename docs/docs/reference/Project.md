---
title: Project
description: Projects 集合与 Project 实体，用于 MaxCompute 项目的生命周期管理和属性访问。
sidebar_position: 15
module: odps-sdk-core
apis:
  - Projects.get
  - Projects.exists
  - Projects.create
  - Projects.delete
  - Projects.iterator
  - Project.getProperties
  - Project.getAllProperties
  - Project.getStatus
  - Project.getType
keywords:
  - Projects
  - Project
  - 项目管理
  - 项目属性
  - ProjectFilter
---

# Project

项目管理包含两个核心类：`Projects`（项目集合管理器）和 `Project`（项目实体）。`Projects` 提供项目级的增删改查操作，`Project` 封装单个项目的属性和行为。

## 获取实例

```java
// 获取 Projects 集合
Projects projects = odps.projects();

// 获取当前默认项目
Project project = projects.get();

// 获取指定项目
Project project = projects.get("project_name");
```

> `Project` 实现了 `LazyLoad` 接口，调用 `get()` 不会加载完整属性。首次访问属性时自动加载。

## Projects 方法

### get

```java
public Project get() throws OdpsException
public Project get(String projectName) throws OdpsException
```

**返回值**：`Project` 对象（延迟加载）

---

### exists

```java
public boolean exists(String projectName) throws OdpsException
```

**返回值**：项目存在返回 `true`

---

### create

```java
public void create(CreateProjectParam param) throws OdpsException
```

**CreateProjectParam Builder 方法**：

| 方法 | 说明 | 必填 |
|------|------|------|
| `name(String)` | 项目名称（2-64字符） | 是 |
| `owner(String)` | 项目所有者 | 是 |
| `defaultCluster(String)` | 默认计算集群 | 是 |
| `comment(String)` | 项目描述 | 否 |
| `superAdmin(String)` | 超级管理员 | 否 |
| `properties(Map)` | 项目属性（覆盖设置） | 否 |
| `appendProperty(String, String)` | 追加属性 | 否 |
| `groupName(String)` | 项目组名称 | 否 |
| `defaultQuotaId(String)` | 默认计算配额 ID | 否 |

---

### createExternalProject

创建外部项目（联邦查询）。

```java
public void createExternalProject(String projectName, String comment,
                                  String refProjectName,
                                  Project.ExternalProjectProperties extProperties)
    throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `projectName` | `String` | 外部项目名称 |
| `comment` | `String` | 项目描述 |
| `refProjectName` | `String` | 关联的内部项目名称 |
| `extProperties` | `ExternalProjectProperties` | 外部数据源配置 |

---

### delete

```java
public Instance delete(String projectName, boolean isImmediate) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `projectName` | `String` | 项目名称 |
| `isImmediate` | `boolean` | `true` 物理删除（不可逆）；`false` 逻辑删除（可恢复） |

**返回值**：物理删除时返回操作 Instance；逻辑删除返回 `null`

---

### iterator / iterable

```java
public Iterator<Project> iterator(String owner)
public Iterable<Project> iterable(String owner)
public Iterator<Project> iteratorByFilter(ProjectFilter filter)
```

**ProjectFilter 属性**：

| 属性 | 说明 |
|------|------|
| `owner` | 项目所有者（精确匹配） |
| `name` | 项目名称前缀匹配 |
| `user` | 项目使用者 |
| `groupName` | 项目组名称 |
| `tenantId` | 租户 ID |
| `regionId` | 地域 ID |
| `quotaNickname` | 配额别名 |

---

## Project 属性方法

### 基础属性

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `getName()` | `String` | 项目名称 |
| `getComment()` | `String` | 项目描述 |
| `getOwner()` | `String` | 项目所有者 |
| `getRegionId()` | `String` | 所属地域（如 `cn-shanghai`） |
| `getCreatedTime()` | `Date` | 创建时间 |
| `getLastModifiedTime()` | `Date` | 最后修改时间 |

---

### getType

```java
public ProjectType getType()
```

**返回值**：

| 枚举值 | 说明 |
|--------|------|
| `MANAGED` | 原生存储项目（默认） |
| `EXTERNAL` | 旧版外部存储（兼容） |
| `EXTERNAL_V2` | 新版外部存储（推荐） |

---

### getStatus

```java
public Status getStatus()
```

**返回值**：

| 枚举值 | 说明 |
|--------|------|
| `AVAILABLE` | 正常可用 |
| `READONLY` | 只读模式 |
| `DELETING` | 删除中 |
| `FROZEN` | 被冻结 |
| `UNKOWN` | 状态未知（需 `reload()`） |

---

### getProperties

获取项目显式配置的属性集合（不含继承配置）。

```java
public Map<String, String> getProperties()
```

---

### getAllProperties

获取全部配置属性（含从项目组继承的属性）。

```java
public Map<String, String> getAllProperties()
```

---

### getProperty

获取指定配置项。

```java
public String getProperty(String key)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `String` | 配置项名称，如 `"odps.sql.type.system.odps2"` |

---

### getExtendedProperties

获取项目扩展属性。

```java
public Map<String, String> getExtendedProperties()
```

---

### getStorageTierInfo

获取分层存储详情。需先调用 `reload()` 刷新数据。

```java
public StorageTierInfo getStorageTierInfo()
```

**StorageTierInfo 结构**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `storageTier` | `String` | 存储策略（STANDARD/LOWFREQUENCY/LONGTERM） |
| `storageLastModifiedTime` | `Date` | 最后统计时间 |
| `storageSize` | `Map<String, Long>` | 各层级存储量（字节） |

---

### getSecurityManager

获取项目安全管理器。

```java
public SecurityManager getSecurityManager()
```

---

### getTunnelEndpoint

获取 Tunnel 接入点。

```java
public String getTunnelEndpoint(String quotaName)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `quotaName` | `String` | 计算配额名称，`null` 使用默认配额 |
