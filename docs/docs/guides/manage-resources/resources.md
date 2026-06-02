---
title: 资源管理
description: 通过 Java SDK 上传、下载、更新和删除 MaxCompute 资源文件（JAR/FILE/TABLE/ARCHIVE/PY）
sidebar_position: 2
module: odps-sdk-core
task: manage-resources
apis:
  - Resources
  - Resource
  - FileResource
  - JarResource
  - TableResource
since: "0.27.0"
keywords:
  - 资源上传
  - JAR 资源
  - 文件资源
  - 表资源
  - ARCHIVE
---

# 资源管理

MaxCompute 中的资源是 UDF 函数执行时依赖的文件，包括 JAR 包、Python 脚本、数据文件等。通过 Java SDK 的 `Resources` 接口，可以对资源进行上传、下载、更新和删除操作。

## 前置条件

- 已完成 [认证配置](../../getting-started/authentication.md)
- 已添加 `odps-sdk-core` 依赖

## 完整示例

以下示例演示如何上传 JAR 文件作为资源，并对其进行查询和删除操作：

```java
import com.aliyun.odps.FileResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Resources;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.account.AliyunAccount;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ResourceManagementExample {

    public static void main(String[] args) throws OdpsException, IOException {
        // 初始化 Odps 客户端
        Odps odps = new Odps(new AliyunAccount("accessId", "accessKey"));
        odps.setDefaultProject("my_project");
        odps.setEndpoint("http://service.odps.aliyun.com/api");

        Resources resources = odps.resources();

        // 1. 上传 JAR 文件资源
        FileResource jarResource = new FileResource();
        jarResource.setName("my-udf-lib.jar");
        jarResource.setComment("UDF library for string operations");

        try (InputStream is = new FileInputStream("/path/to/my-udf-lib.jar")) {
            resources.create(jarResource, is);
            System.out.println("JAR 资源上传成功: my-udf-lib.jar");
        }

        // 2. 检查资源是否存在
        boolean exists = resources.exists("my-udf-lib.jar");
        System.out.println("资源是否存在: " + exists);

        // 3. 获取资源元信息
        Resource loaded = resources.get("my-udf-lib.jar");
        loaded.reload();
        System.out.println("资源类型: " + loaded.getType());
        System.out.println("资源大小: " + loaded.getSize());
        System.out.println("最后修改: " + loaded.getLastModifiedTime());

        // 4. 下载资源内容
        try (InputStream is = resources.getResourceAsStream("my-udf-lib.jar")) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            long totalBytes = 0;
            while ((bytesRead = is.read(buffer)) != -1) {
                totalBytes += bytesRead;
            }
            System.out.println("下载资源大小: " + totalBytes + " bytes");
        }

        // 5. 遍历所有资源
        System.out.println("项目中的所有资源:");
        for (Resource r : resources.iterable()) {
            System.out.println("  - " + r.getName() + " (" + r.getType() + ")");
        }

        // 6. 更新资源
        FileResource updatedResource = new FileResource();
        updatedResource.setName("my-udf-lib.jar");
        try (InputStream is = new FileInputStream("/path/to/my-udf-lib-v2.jar")) {
            resources.update(updatedResource, is);
            System.out.println("资源更新成功");
        }

        // 7. 删除资源
        resources.delete("my-udf-lib.jar");
        System.out.println("资源删除成功");
    }
}
```

## 代码说明

### 上传文件资源

文件类型资源（JAR/FILE/ARCHIVE/PY）通过 `InputStream` 上传：

```java
FileResource resource = new FileResource();
resource.setName("example.jar");

try (InputStream is = new FileInputStream("local/path/example.jar")) {
    resources.create(resource, is);
}
```

MaxCompute 会根据文件后缀名自动识别资源类型。也可以显式设置类型：

```java
resource.setType(Resource.Type.JAR);    // JAR 资源
resource.setType(Resource.Type.FILE);   // 普通文件资源
resource.setType(Resource.Type.ARCHIVE);// 压缩包资源
resource.setType(Resource.Type.PY);     // Python 脚本资源
```

支持在指定项目或 schema 中创建：

```java
// 在指定项目中创建
resources.create("target_project", resource, inputStream);

// 在指定项目和 schema 中创建
resources.create("target_project", "my_schema", resource, inputStream);
```

### 创建表资源

表资源引用项目中的已有表，不需要 `InputStream`：

```java
TableResource tableResource = new TableResource();
tableResource.setName("my_table_resource");
tableResource.setProject("source_project");
tableResource.setTable("source_table");

// 可选：指定分区
tableResource.setPartition("pt='20240101'");

resources.create(tableResource);
```

### 下载资源

通过 `getResourceAsStream` 方法获取资源的输入流：

```java
try (InputStream is = resources.getResourceAsStream("resource_name")) {
    // 处理输入流
}

// 指定项目和 schema
try (InputStream is = resources.getResourceAsStream("project", "schema", "resource_name")) {
    // 处理输入流
}
```

### 遍历资源

```java
// 迭代器方式
Iterator<Resource> it = resources.iterator();
while (it.hasNext()) {
    Resource r = it.next();
    System.out.println(r.getName() + " : " + r.getType());
}

// for-each 方式
for (Resource r : resources.iterable()) {
    System.out.println(r.getName());
}
```

### 删除资源

```java
// 删除默认项目中的资源
resources.delete("resource_name");

// 删除指定项目中的资源
resources.delete("project_name", "resource_name");

// 删除指定项目和 schema 中的资源
resources.delete("project_name", "schema_name", "resource_name");
```

## 资源类型

| 类型 | 后缀名 | 说明 | 用途 |
|------|---------|------|------|
| JAR | `.jar` | Java 归档文件 | UDF/UDTF/UDAF 的实现代码 |
| FILE | 任意 | 普通文件 | UDF 运行时读取的配置或数据文件 |
| PY | `.py` | Python 脚本 | Python UDF 的实现代码 |
| ARCHIVE | `.zip`/`.tar.gz`/`.tgz` | 压缩包 | 包含多个文件的归档，运行时自动解压 |
| TABLE | - | 表资源 | 引用项目中的表数据，供 UDF 运行时读取 |

## 配置选项

### FileResource 属性

| 属性 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name | String | Y | 资源名称（包含后缀） |
| type | Resource.Type | N | 资源类型，默认根据后缀名推断 |
| comment | String | N | 资源描述信息 |

### TableResource 属性

| 属性 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name | String | Y | 资源名称 |
| project | String | Y | 源表所在的项目名 |
| table | String | Y | 源表名称 |
| partition | String | N | 分区规格（如 `pt='20240101'`） |

## 注意事项

- 单个资源文件大小限制为 500 MB，超出需要使用 ARCHIVE 类型拆分上传
- 资源名称在同一项目同一 schema 下唯一，重复创建会抛出异常
- 删除已被函数引用的资源会导致对应函数不可用
- 上传资源时，`InputStream` 会被完整读取，确保网络连接稳定
- 更新资源时需要重新上传完整文件，不支持增量更新
- 表资源引用的表必须在同一项目或已通过 Package 授权的项目中

## 相关文档

- [函数管理](./functions.md) - 基于资源创建和管理自定义函数
- [UDF 函数开发与使用](./functions.md) - UDF 开发完整流程
- [Resources API 参考](../../reference/Resources.md) - Resources 类完整 API
