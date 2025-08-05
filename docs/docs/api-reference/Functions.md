---
title: Functions
sidebar_position: 5
---

# Functions

`Functions` 类用于管理 MaxCompute 项目中的用户自定义函数 (UDF)。通过此类可以创建、更新、删除和查询函数。

## 概览

- [获取 Functions 实例](#获取-functions-实例)
- [创建函数](#创建函数)
- [获取函数](#获取函数)
- [检查函数是否存在](#检查函数是否存在)
- [更新函数](#更新函数)
- [删除函数](#删除函数)
- [遍历函数](#遍历函数)

## 获取 Functions 实例

要操作函数，首先需要获取 Functions 实例：

```java
Functions functions = odps.functions();
```

## 创建函数

### 基本创建方法

```java
// 在默认项目中创建函数
functions.create(function);

// 在指定项目中创建函数
functions.create("project_name", function);

// 在指定项目和 schema 中创建函数
functions.create("project_name", "schema_name", function);
```

**参数说明**：
- `function`: Function 对象，包含函数的定义信息

### Function 对象构建

```java
Function function = new Function();
function.setName("my_function");
function.setClassPath("com.example.MyFunction");
function.setResources(Arrays.asList("resource1.jar", "resource2.py"));
```

## 获取函数

### 基本获取方法

```java
// 获取默认项目中的函数
Function function = functions.get("function_name");

// 获取指定项目中的函数
Function function = functions.get("project_name", "function_name");

// 获取指定项目和 schema 中的函数
Function function = functions.get("project_name", "schema_name", "function_name");
```

## 检查函数是否存在

```java
// 检查默认项目中函数是否存在
boolean exists = functions.exists("function_name");

// 检查指定项目中函数是否存在
boolean exists = functions.exists("project_name", "function_name");

// 检查指定项目和 schema 中函数是否存在
boolean exists = functions.exists("project_name", "schema_name", "function_name");
```

## 更新函数

```java
// 更新默认项目中的函数
functions.update(function);

// 更新指定项目中的函数
functions.update("project_name", function);

// 更新指定项目和 schema 中的函数
functions.update("project_name", "schema_name", function);
```

## 删除函数

```java
// 删除默认项目中的函数
functions.delete("function_name");

// 删除指定项目中的函数
functions.delete("project_name", "function_name");

// 删除指定项目和 schema 中的函数
functions.delete("project_name", "schema_name", "function_name");
```

## 遍历函数

### 使用迭代器遍历

```java
// 遍历默认项目中的所有函数
Iterator<Function> iterator = functions.iterator();

// 遍历指定项目中的所有函数
Iterator<Function> iterator = functions.iterator("project_name");

// 遍历指定项目和 schema 中的所有函数
Iterator<Function> iterator = functions.iterator("project_name", "schema_name");
```

### 使用 Iterable 遍历

```java
// 获取默认项目中函数的 Iterable
Iterable<Function> iterable = functions.iterable();

// 获取指定项目中函数的 Iterable
Iterable<Function> iterable = functions.iterable("project_name");

// 获取指定项目和 schema 中函数的 Iterable
Iterable<Function> iterable = functions.iterable("project_name", "schema_name");

// 使用 foreach 语法遍历
for (Function function : functions.iterable()) {
    System.out.println(function.getName());
}
```