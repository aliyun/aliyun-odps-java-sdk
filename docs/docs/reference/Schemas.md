---
title: Schemas
description: Schemas 类用于管理 MaxCompute 项目中的 Schema 命名空间，提供创建、获取、删除和遍历操作。
sidebar_position: 16
module: odps-sdk-core
apis:
  - Schemas.get
  - Schemas.create
  - Schemas.delete
  - Schemas.exists
  - Schemas.iterator
  - Schemas.iterable
keywords:
  - Schemas
  - Schema
  - 命名空间
  - SchemaFilter
---

# Schemas

`Schemas` 类用于管理 MaxCompute 项目中的 Schema。Schema 是命名空间概念，用于组织和管理表、函数等对象。

## 获取实例

```java
Schemas schemas = odps.schemas();
```

## 方法

### create

创建 Schema。

```java
public void create(String schemaName) throws OdpsException
public void create(String projectName, String schemaName) throws OdpsException
public void create(String projectName, String schemaName, String comment, boolean ifNotExists) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `schemaName` | `String` | Schema 名称 |
| `projectName` | `String` | 目标项目名称（可选） |
| `comment` | `String` | Schema 描述（可选） |
| `ifNotExists` | `boolean` | `true` 已存在时不报错；`false` 已存在时抛异常 |

---

### get

获取 Schema 对象。

```java
public Schema get()
public Schema get(String schemaName)
public Schema get(String projectName, String schemaName)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `schemaName` | `String` | Schema 名称；无参版本返回默认 Schema |
| `projectName` | `String` | 项目名称（可选） |

**返回值**：`Schema` 对象

---

### exists

检查 Schema 是否存在。

```java
public boolean exists(String schemaName) throws OdpsException
public boolean exists(String projectName, String schemaName) throws OdpsException
```

**返回值**：存在返回 `true`

---

### delete

删除 Schema。

```java
public void delete(String schemaName) throws OdpsException
public void delete(String projectName, String schemaName) throws OdpsException
```

---

### iterator

获取 Schema 迭代器。

```java
public Iterator<Schema> iterator()
public Iterator<Schema> iterator(String projectName)
public Iterator<Schema> iterator(String projectName, SchemaFilter filter)
```

---

### iterable

获取 Schema Iterable（支持 foreach 语法）。

```java
public Iterable<Schema> iterable()
public Iterable<Schema> iterable(String projectName)
```

---

## SchemaFilter

| 方法 | 说明 |
|------|------|
| `setOwner(String)` | 按所有者过滤 |
| `setName(String)` | 按名称前缀过滤 |

**示例**：

```java
SchemaFilter filter = new SchemaFilter();
filter.setOwner("user_id");
Iterator<Schema> iterator = schemas.iterator("project_name", filter);
```

---

## Schema 对象属性

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `getName()` | `String` | Schema 名称 |
| `getOwner()` | `String` | 所有者 |
| `getComment()` | `String` | 描述信息 |
| `getCreateTime()` | `Date` | 创建时间 |
| `getModifiedTime()` | `Date` | 最后修改时间 |
| `getProjectName()` | `String` | Schema 所属的项目名称 |
| `getType()` | `String` | Schema 类型 |

---

## 使用示例

```java
Schemas schemas = odps.schemas();

// 创建 Schema
schemas.create("my_project", "my_schema", "业务数据Schema", true);

// 检查是否存在
boolean exists = schemas.exists("my_schema");

// 遍历所有 Schema
for (Schema schema : schemas.iterable()) {
    System.out.println(schema.getName());
}

// 删除 Schema
schemas.delete("my_schema");
```
