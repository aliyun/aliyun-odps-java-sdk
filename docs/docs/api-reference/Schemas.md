---
title: Schemas
sidebar_position: 8
---

# Schemas

`Schemas` 类用于管理 MaxCompute 项目中的 Schema。Schema 是 MaxCompute 中的命名空间概念，用于组织和管理表、函数等对象。

## 概览

- [获取 Schemas 实例](#获取-schemas-实例)
- [创建 Schema](#创建-schema)
- [获取 Schema](#获取-schema)
- [检查 Schema 是否存在](#检查-schema-是否存在)
- [删除 Schema](#删除-schema)
- [遍历 Schema](#遍历-schema)

## 获取 Schemas 实例

要操作 Schema，首先需要获取 Schemas 实例：

```java
Schemas schemas = odps.schemas();
```

## 创建 Schema

```java
// 在默认项目中创建 Schema
schemas.create("schema_name");

// 在指定项目中创建 Schema
schemas.create("project_name", "schema_name");

// 在指定项目中创建带注释的 Schema
schemas.create("project_name", "schema_name", "Schema comment", false);

// 创建带 ifNotExists 参数的 Schema
schemas.create("project_name", "schema_name", "Schema comment", true); // 如果已存在则不报错
```

## 获取 Schema

```java
// 获取默认项目中的默认 Schema
Schema schema = schemas.get();

// 获取默认项目中的指定 Schema
Schema schema = schemas.get("schema_name");

// 获取指定项目中的指定 Schema
Schema schema = schemas.get("project_name", "schema_name");
```

## 检查 Schema 是否存在

```java
// 检查默认项目中 Schema 是否存在
boolean exists = schemas.exists("schema_name");

// 检查指定项目中 Schema 是否存在
boolean exists = schemas.exists("project_name", "schema_name");
```

## 删除 Schema

```java
// 删除默认项目中的 Schema
schemas.delete("schema_name");

// 删除指定项目中的 Schema
schemas.delete("project_name", "schema_name");
```

## 遍历 Schema

### 使用迭代器遍历

```java
// 遍历默认项目中的所有 Schema
Iterator<Schema> iterator = schemas.iterator();

// 遍历指定项目中的所有 Schema
Iterator<Schema> iterator = schemas.iterator("project_name");

// 使用过滤器遍历 Schema
SchemaFilter filter = new SchemaFilter();
filter.setOwner("user_id");
Iterator<Schema> iterator = schemas.iterator("project_name", filter);
```

### 使用 Iterable 遍历

```java
// 获取默认项目中 Schema 的 Iterable
Iterable<Schema> iterable = schemas.iterable();

// 获取指定项目中 Schema 的 Iterable
Iterable<Schema> iterable = schemas.iterable("project_name");

// 使用 foreach 语法遍历
for (Schema schema : schemas.iterable()) {
    System.out.println(schema.getName());
}
```

### SchemaFilter 使用

```java
// 创建过滤器
SchemaFilter filter = new SchemaFilter();
filter.setOwner("user_id"); // 按所有者过滤
filter.setName("prefix"); // 按名称前缀过滤

// 使用过滤器遍历
Iterator<Schema> iterator = schemas.iterator("project_name", filter);
```