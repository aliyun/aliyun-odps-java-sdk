---
title: Resources
description: Resources 类用于管理 MaxCompute 项目中的资源文件，包括 JAR 包、Python 脚本、数据文件、表资源等。
sidebar_position: 14
module: odps-sdk-core
apis:
  - Resources.get
  - Resources.create
  - Resources.update
  - Resources.delete
  - Resources.exists
  - Resources.getResourceAsStream
  - Resources.iterator
  - Resources.iterable
keywords:
  - Resources
  - Resource
  - FileResource
  - TableResource
  - JAR
  - 资源管理
---

# Resources

`Resources` 类用于管理 MaxCompute 项目中的资源文件。资源文件是 UDF 函数执行时依赖的文件，包括 JAR 包、Python 脚本、数据文件等。

## 获取实例

```java
Resources resources = odps.resources();
```

## 资源类型

| 类 | 说明 |
|------|------|
| `FileResource` | 文件资源（JAR、PY、文本等） |
| `TableResource` | 表资源 |
| `VolumeResource` | 卷资源 |

## 方法

### create

创建资源。

```java
// 创建文件资源
public void create(Resource resource, InputStream inputStream) throws OdpsException
public void create(String projectName, Resource resource, InputStream inputStream) throws OdpsException
public void create(String projectName, String schemaName, Resource resource, InputStream inputStream) throws OdpsException

// 创建表资源/卷资源（无需 InputStream）
public void create(Resource resource) throws OdpsException
public void create(String projectName, Resource resource) throws OdpsException
public void create(String projectName, String schemaName, Resource resource) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `resource` | `Resource` | 资源对象（FileResource / TableResource / VolumeResource） |
| `inputStream` | `InputStream` | 文件资源的数据流 |
| `projectName` | `String` | 目标项目名称（可选） |
| `schemaName` | `String` | 目标 Schema 名称（可选） |

**示例**：

```java
FileResource resource = new FileResource();
resource.setName("my_udf.jar");
InputStream is = new FileInputStream("path/to/my_udf.jar");
resources.create(resource, is);
```

---

### get

获取资源对象。

```java
public Resource get(String resourceName)
public Resource get(String projectName, String resourceName)
public Resource get(String projectName, String schemaName, String resourceName)
```

**返回值**：`Resource` 对象

---

### getResourceAsStream

获取资源文件的输入流。

```java
public InputStream getResourceAsStream(String resourceName) throws OdpsException
public InputStream getResourceAsStream(String projectName, String resourceName) throws OdpsException
public InputStream getResourceAsStream(String projectName, String schemaName, String resourceName) throws OdpsException
```

**返回值**：`InputStream`，资源文件的数据流

---

### exists

检查资源是否存在。

```java
public boolean exists(String resourceName) throws OdpsException
public boolean exists(String projectName, String resourceName) throws OdpsException
public boolean exists(String projectName, String schemaName, String resourceName) throws OdpsException
```

**返回值**：存在返回 `true`

---

### update

更新资源。

```java
// 更新文件资源
public void update(Resource resource, InputStream inputStream) throws OdpsException
public void update(String projectName, Resource resource, InputStream inputStream) throws OdpsException
public void update(String projectName, String schemaName, Resource resource, InputStream inputStream) throws OdpsException

// 更新表资源/卷资源
public void update(Resource resource) throws OdpsException
public void update(String projectName, Resource resource) throws OdpsException
public void update(String projectName, String schemaName, Resource resource) throws OdpsException
```

---

### delete

删除资源。

```java
public void delete(String resourceName) throws OdpsException
public void delete(String projectName, String resourceName) throws OdpsException
public void delete(String projectName, String schemaName, String resourceName) throws OdpsException
```

---

### iterator

获取资源迭代器。

```java
public Iterator<Resource> iterator()
public Iterator<Resource> iterator(String projectName)
public Iterator<Resource> iterator(String projectName, String schemaName)
```

---

### iterable

获取资源 Iterable（支持 foreach 语法）。

```java
public Iterable<Resource> iterable()
public Iterable<Resource> iterable(String projectName)
public Iterable<Resource> iterable(String projectName, String schemaName)
```

---

### createTempResource

创建临时资源文件。

```java
public FileResource createTempResource(String fileName) throws OdpsException
public FileResource createTempResource(String projectName, String fileName) throws OdpsException
public FileResource createTempResource(String projectName, String filePath, Type type) throws OdpsException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `fileName` | `String` | 临时资源文件名 |
| `projectName` | `String` | 目标项目名称（可选） |
| `filePath` | `String` | 资源文件路径 |
| `type` | `Type` | 资源类型（FILE、JAR、PY、ARCHIVE 等） |

**返回值**：`FileResource`，创建的临时资源对象

---

## Resource 对象属性

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `getName()` | `String` | 资源名称 |
| `getType()` | `Resource.Type` | 资源类型（FILE、JAR、PY、ARCHIVE、TABLE、VOLUMEFILE） |
| `getOwner()` | `String` | 资源所有者 |
| `getCreatedTime()` | `Date` | 创建时间 |
| `getLastModifiedTime()` | `Date` | 最后修改时间 |
| `getSize()` | `long` | 资源大小（字节） |

---

## 使用示例

```java
Resources resources = odps.resources();

// 创建 JAR 资源
FileResource jar = new FileResource();
jar.setName("my_lib.jar");
resources.create(jar, new FileInputStream("my_lib.jar"));

// 创建表资源
TableResource tableRes = new TableResource();
tableRes.setName("my_table_res");
tableRes.setProject("my_project");
tableRes.setTable("source_table");
resources.create(tableRes);

// 遍历所有资源
for (Resource r : resources.iterable()) {
    System.out.println(r.getName() + " [" + r.getType() + "]");
}

// 删除资源
resources.delete("my_lib.jar");
```
