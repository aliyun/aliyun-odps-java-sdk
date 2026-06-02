---
title: Functions
description: Functions 类用于管理 MaxCompute 项目中的用户自定义函数 (UDF)，提供创建、获取、更新、删除和遍历操作。
sidebar_position: 13
module: odps-sdk-core
apis:
  - Functions.get
  - Functions.create
  - Functions.update
  - Functions.delete
  - Functions.exists
  - Functions.iterator
  - Functions.iterable
keywords:
  - Functions
  - UDF
  - 自定义函数
  - Function
---

# Functions

`Functions` 类用于管理 MaxCompute 项目中的用户自定义函数 (UDF)。通过此类可以创建、更新、删除和查询函数。

## 获取实例

```java
Functions functions = odps.functions();
```

## 方法

### create

创建函数。

```java
public void create(Function function) throws OdpsException
public void create(String projectName, Function function) throws OdpsException
public void create(String projectName, String schemaName, Function function) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `function` | `Function` | 函数定义对象 |
| `projectName` | `String` | 目标项目名称（可选） |
| `schemaName` | `String` | 目标 Schema 名称（可选） |

**Function 对象构建**：

```java
Function function = new Function();
function.setName("my_function");
function.setClassPath("com.example.MyFunction");
function.setResources(Arrays.asList("resource1.jar", "resource2.py"));
```

---

### get

获取函数对象。

```java
public Function get(String functionName)
public Function get(String projectName, String functionName)
public Function get(String projectName, String schemaName, String functionName)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `functionName` | `String` | 函数名称 |
| `projectName` | `String` | 项目名称（可选） |
| `schemaName` | `String` | Schema 名称（可选） |

**返回值**：`Function` 对象

---

### exists

检查函数是否存在。

```java
public boolean exists(String functionName) throws OdpsException
public boolean exists(String projectName, String functionName) throws OdpsException
public boolean exists(String projectName, String schemaName, String functionName) throws OdpsException
```

**返回值**：存在返回 `true`

---

### update

更新函数定义。

```java
public void update(Function function) throws OdpsException
public void update(String projectName, Function function) throws OdpsException
public void update(String projectName, String schemaName, Function function) throws OdpsException
```

---

### delete

删除函数。

```java
public void delete(String functionName) throws OdpsException
public void delete(String projectName, String functionName) throws OdpsException
public void delete(String projectName, String schemaName, String functionName) throws OdpsException
```

---

### iterator

获取函数迭代器。

```java
public Iterator<Function> iterator()
public Iterator<Function> iterator(String projectName)
public Iterator<Function> iterator(String projectName, String schemaName)
```

---

### iterable

获取函数 Iterable（支持 foreach 语法）。

```java
public Iterable<Function> iterable()
public Iterable<Function> iterable(String projectName)
public Iterable<Function> iterable(String projectName, String schemaName)
```

---

## Function 对象属性

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `getName()` | `String` | 函数名称 |
| `getClassPath()` | `String` | 函数实现类路径 |
| `getResources()` | `List<String>` | 依赖资源列表 |
| `getOwner()` | `String` | 函数所有者 |
| `getCreatedTime()` | `Date` | 创建时间 |

---

## 使用示例

```java
Functions functions = odps.functions();

// 创建函数
Function func = new Function();
func.setName("my_udf");
func.setClassPath("com.example.MyUDF");
func.setResources(Arrays.asList("my_udf.jar"));
functions.create(func);

// 遍历所有函数
for (Function f : functions.iterable()) {
    System.out.println(f.getName() + " -> " + f.getClassPath());
}

// 删除函数
functions.delete("my_udf");
```
