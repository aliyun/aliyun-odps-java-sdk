---
title: Resources
sidebar_position: 6
---

# Resources

`Resources` 类用于管理 MaxCompute 项目中的资源文件。资源文件是 UDF 函数执行时依赖的文件，包括 JAR 包、Python 脚本、数据文件等。

## 概览

- [获取 Resources 实例](#获取-resources-实例)
- [创建资源](#创建资源)
- [获取资源](#获取资源)
- [检查资源是否存在](#检查资源是否存在)
- [更新资源](#更新资源)
- [删除资源](#删除资源)
- [遍历资源](#遍历资源)

## 获取 Resources 实例

要操作资源，首先需要获取 Resources 实例：

```java
Resources resources = odps.resources();
```

## 创建资源

### 创建文件资源

```java
// 创建文件资源
FileResource resource = new FileResource();
resource.setName("my_resource.jar");

// 从 InputStream 创建资源
InputStream inputStream = new FileInputStream("path/to/my_resource.jar");
resources.create(resource, inputStream);

// 在指定项目中创建资源
resources.create("project_name", resource, inputStream);

// 在指定项目和 schema 中创建资源
resources.create("project_name", "schema_name", resource, inputStream);
```

### 创建表资源

```java
// 创建表资源
TableResource resource = new TableResource();
resource.setName("my_table_resource");
resource.setProject("source_project");
resource.setTable("source_table");

// 创建表资源
resources.create(resource);

// 在指定项目中创建表资源
resources.create("project_name", resource);

// 在指定项目和 schema 中创建表资源
resources.create("project_name", "schema_name", resource);
```

### 创建卷资源

```java
// 创建卷资源
VolumeResource resource = new VolumeResource();
resource.setName("my_volume_resource");
resource.setVolume("source_volume");

// 创建卷资源
resources.create(resource);

// 在指定项目中创建卷资源
resources.create("project_name", resource);
```

## 获取资源

### 获取资源输入流

```java
// 获取资源输入流
InputStream inputStream = resources.getResourceAsStream("resource_name");

// 获取指定项目中资源的输入流
InputStream inputStream = resources.getResourceAsStream("project_name", "resource_name");

// 获取指定项目和 schema 中资源的输入流
InputStream inputStream = resources.getResourceAsStream("project_name", "schema_name", "resource_name");
```

### 获取资源对象

```java
// 获取默认项目中的资源
Resource resource = resources.get("resource_name");

// 获取指定项目中的资源
Resource resource = resources.get("project_name", "resource_name");

// 获取指定项目和 schema 中的资源
Resource resource = resources.get("project_name", "schema_name", "resource_name");
```

## 检查资源是否存在

```java
// 检查默认项目中资源是否存在
boolean exists = resources.exists("resource_name");

// 检查指定项目中资源是否存在
boolean exists = resources.exists("project_name", "resource_name");

// 检查指定项目和 schema 中资源是否存在
boolean exists = resources.exists("project_name", "schema_name", "resource_name");
```

## 更新资源

### 更新文件资源

```java
// 更新文件资源
FileResource resource = new FileResource();
resource.setName("my_resource.jar");

InputStream inputStream = new FileInputStream("path/to/updated_resource.jar");
resources.update(resource, inputStream);

// 更新指定项目中的文件资源
resources.update("project_name", resource, inputStream);

// 更新指定项目和 schema 中的文件资源
resources.update("project_name", "schema_name", resource, inputStream);
```

### 更新表资源

```java
// 更新表资源
TableResource resource = new TableResource();
resource.setName("my_table_resource");
resource.setProject("updated_source_project");
resource.setTable("updated_source_table");

resources.update(resource);

// 更新指定项目中的表资源
resources.update("project_name", resource);

// 更新指定项目和 schema 中的表资源
resources.update("project_name", "schema_name", resource);
```

### 更新卷资源

```java
// 更新卷资源
VolumeResource resource = new VolumeResource();
resource.setName("my_volume_resource");
resource.setVolume("updated_source_volume");

resources.update(resource);

// 更新指定项目中的卷资源
resources.update("project_name", resource);
```

## 删除资源

```java
// 删除默认项目中的资源
resources.delete("resource_name");

// 删除指定项目中的资源
resources.delete("project_name", "resource_name");

// 删除指定项目和 schema 中的资源
resources.delete("project_name", "schema_name", "resource_name");
```

## 遍历资源

### 使用迭代器遍历

```java
// 遍历默认项目中的所有资源
Iterator<Resource> iterator = resources.iterator();

// 遍历指定项目中的所有资源
Iterator<Resource> iterator = resources.iterator("project_name");

// 遍历指定项目和 schema 中的所有资源
Iterator<Resource> iterator = resources.iterator("project_name", "schema_name");
```

### 使用 Iterable 遍历

```java
// 获取默认项目中资源的 Iterable
Iterable<Resource> iterable = resources.iterable();

// 获取指定项目中资源的 Iterable
Iterable<Resource> iterable = resources.iterable("project_name");

// 获取指定项目和 schema 中资源的 Iterable
Iterable<Resource> iterable = resources.iterable("project_name", "schema_name");

// 使用 foreach 语法遍历
for (Resource resource : resources.iterable()) {
    System.out.println(resource.getName());
}
```