---
title: 函数管理
description: 通过 Java SDK 创建、查询、更新和删除 MaxCompute 用户自定义函数（UDF/UDTF/UDAF）
sidebar_position: 1
module: odps-sdk-core
task: manage-resources
apis:
  - Functions
  - Function
since: "0.27.0"
keywords:
  - UDF
  - UDTF
  - UDAF
  - 自定义函数
  - 函数注册
---

# 函数管理

MaxCompute 支持用户自定义函数（UDF），包括 UDF（标量函数）、UDTF（表值函数）和 UDAF（聚合函数）。通过 Java SDK 的 `Functions` 接口，可以对函数进行创建、查询、更新和删除等操作。

## 前置条件

- 已完成 [认证配置](../../getting-started/authentication.md)
- 已添加 `odps-sdk-core` 依赖
- 已将 UDF 对应的 JAR 包上传为资源（参见 [资源管理](./resources.md)）

## 完整示例

以下示例演示如何从已上传的 JAR 资源创建一个 UDF 函数，并对其进行查询和删除操作：

```java
import com.aliyun.odps.Function;
import com.aliyun.odps.Functions;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;

import java.util.Arrays;
import java.util.Iterator;

public class FunctionManagementExample {

    public static void main(String[] args) throws OdpsException {
        // 初始化 Odps 客户端
        Odps odps = new Odps(new AliyunAccount("accessId", "accessKey"));
        odps.setDefaultProject("my_project");
        odps.setEndpoint("http://service.odps.aliyun.com/api");

        Functions functions = odps.functions();

        // 1. 创建函数
        Function function = new Function();
        function.setName("my_upper_udf");
        function.setClassPath("com.example.udf.UpperCase");
        function.setResources(Arrays.asList("my-udf-lib.jar"));

        functions.create(function);
        System.out.println("函数创建成功: my_upper_udf");

        // 2. 检查函数是否存在
        boolean exists = functions.exists("my_upper_udf");
        System.out.println("函数是否存在: " + exists);

        // 3. 获取函数详情
        Function loaded = functions.get("my_upper_udf");
        loaded.reload();
        System.out.println("函数类路径: " + loaded.getClassPath());
        System.out.println("关联资源: " + loaded.getResourceNames());

        // 4. 遍历所有函数
        System.out.println("项目中的所有函数:");
        for (Function f : functions.iterable()) {
            System.out.println("  - " + f.getName());
        }

        // 5. 更新函数（例如更换 JAR 资源版本）
        Function updated = new Function();
        updated.setName("my_upper_udf");
        updated.setClassPath("com.example.udf.UpperCase");
        updated.setResources(Arrays.asList("my-udf-lib-v2.jar"));
        functions.update(updated);
        System.out.println("函数更新成功");

        // 6. 删除函数
        functions.delete("my_upper_udf");
        System.out.println("函数删除成功");
    }
}
```

## 代码说明

### 创建函数

创建函数时需要指定三个关键属性：
- `name`：函数名称，在项目中唯一
- `classPath`：UDF 实现类的全限定类名
- `resources`：函数依赖的资源列表（通常是 JAR 包资源名）

```java
Function function = new Function();
function.setName("my_upper_udf");
function.setClassPath("com.example.udf.UpperCase");
function.setResources(Arrays.asList("my-udf-lib.jar"));
functions.create(function);
```

支持在指定项目或 schema 中创建：

```java
// 在指定项目中创建
functions.create("target_project", function);

// 在指定项目和 schema 中创建
functions.create("target_project", "my_schema", function);
```

### 获取函数

```java
// 从默认项目获取
Function f = functions.get("function_name");

// 从指定项目获取
Function f = functions.get("project_name", "function_name");

// 从指定项目和 schema 获取
Function f = functions.get("project_name", "schema_name", "function_name");
```

获取到的 Function 对象是延迟加载的，访问详细属性前需要调用 `reload()`。

### 遍历函数

```java
// 迭代器方式
Iterator<Function> it = functions.iterator();
while (it.hasNext()) {
    Function f = it.next();
    System.out.println(f.getName());
}

// for-each 方式
for (Function f : functions.iterable()) {
    System.out.println(f.getName());
}
```

### 删除函数

```java
// 删除默认项目中的函数
functions.delete("function_name");

// 删除指定项目中的函数
functions.delete("project_name", "function_name");

// 删除指定项目和 schema 中的函数
functions.delete("project_name", "schema_name", "function_name");
```

## 函数类型

MaxCompute 支持三种自定义函数类型：

| 类型 | 基类 | 说明 | 输入/输出 |
|------|------|------|-----------|
| UDF | `com.aliyun.odps.udf.UDF` | 标量函数 | 一行输入，一个值输出 |
| UDTF | `com.aliyun.odps.udf.UDTF` | 表值函数 | 一行输入，多行输出 |
| UDAF | `com.aliyun.odps.udf.Aggregator` | 聚合函数 | 多行输入，一个值输出 |

三种类型的函数在注册时使用相同的 API，区别仅在于 `classPath` 指向的实现类不同。

## 配置选项

### Function 属性

| 属性 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name | String | Y | 函数名称 |
| classPath | String | Y | 实现类的全限定名 |
| resources | List\<String\> | Y | 依赖的资源名称列表 |
| comment | String | N | 函数描述信息 |

## 注意事项

- 创建函数前，必须先将对应的 JAR/PY 等资源上传到同一项目中（参见 [资源管理](./resources.md)）
- 函数名称在同一项目同一 schema 下唯一，重复创建会抛出 `OdpsException`
- 更新函数时需要重新指定所有属性（包括 classPath 和 resources），不支持部分更新
- 删除正在被 SQL 作业引用的函数可能导致作业失败
- 函数的 `resources` 列表中可包含多个资源，适用于 UDF 依赖多个 JAR 包的场景

## 相关文档

- [资源管理](./resources.md) - 上传和管理函数依赖的资源文件
- [UDF 函数开发与使用](../../core-concept/functions.md) - UDF 开发完整流程
- [Functions API 参考](../../api-reference/Functions.md) - Functions 类完整 API
