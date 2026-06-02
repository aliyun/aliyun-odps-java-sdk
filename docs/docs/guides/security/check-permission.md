---
title: 权限校验
description: 通过 Java SDK 检查用户是否拥有对 MaxCompute 对象的指定操作权限
sidebar_position: 1
module: odps-sdk-core
task: security
apis:
  - SecurityManager
since: "0.27.0"
keywords:
  - 权限检查
  - checkPermission
  - ACL
  - 访问控制
---

# 权限校验

在 MaxCompute 中，可以通过 `SecurityManager` 的 `checkPermission` 方法检查当前用户是否拥有对特定对象的操作权限。这在需要在业务逻辑中做权限预检时非常有用，可以避免在执行操作时才发现权限不足。

## 前置条件

- 已完成 [认证配置](../../getting-started/authentication.md)
- 已添加 `odps-sdk-core` 依赖
- 当前账号需要有目标项目的访问权限

## 完整示例

以下示例演示如何检查用户对表的 Select 权限：

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.security.CheckPermissionResult;
import com.aliyun.odps.security.CheckPermissionResultInfo;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.type.ActionType;
import com.aliyun.odps.type.ObjectType;
import com.aliyun.odps.security.PermissionDesc;

public class CheckPermissionExample {

    public static void main(String[] args) throws OdpsException {
        // 初始化 Odps 客户端
        Odps odps = new Odps(new AliyunAccount("accessId", "accessKey"));
        odps.setDefaultProject("my_project");
        odps.setEndpoint("http://service.odps.aliyun.com/api");

        // 获取 SecurityManager
        SecurityManager sm = odps.projects().get().getSecurityManager();

        // 1. 检查对表的 Select 权限
        PermissionDesc desc = new PermissionDesc(
            "my_project",
            ObjectType.Table,
            "sales_data",
            ActionType.Select
        );

        CheckPermissionResultInfo resultInfo = sm.checkPermission(desc);
        CheckPermissionResult result = resultInfo.getResult();

        if (result == CheckPermissionResult.Allow) {
            System.out.println("允许访问: 用户对 sales_data 表拥有 Select 权限");
        } else {
            System.out.println("拒绝访问: " + resultInfo.getMessage());
        }

        // 2. 检查对函数的 Execute 权限
        PermissionDesc funcDesc = new PermissionDesc(
            "my_project",
            ObjectType.Function,
            "my_udf",
            ActionType.Execute
        );

        CheckPermissionResultInfo funcResult = sm.checkPermission(funcDesc);
        System.out.println("函数执行权限: " + funcResult.getResult().name());

        // 3. 检查指定 schema 下对象的权限
        PermissionDesc schemaDesc = new PermissionDesc(
            "my_project",
            "my_schema",
            ObjectType.Table,
            "user_profile",
            ActionType.Describe
        );

        CheckPermissionResultInfo schemaResult = sm.checkPermission(schemaDesc);
        System.out.println("Schema 下表的 Describe 权限: " + schemaResult.getResult().name());
    }
}
```

## 代码说明

### 获取 SecurityManager

`SecurityManager` 通过项目对象获取：

```java
SecurityManager sm = odps.projects().get().getSecurityManager();
```

### 构建权限描述

`PermissionDesc` 封装了权限检查所需的全部参数：

```java
// 基本构造方法
PermissionDesc desc = new PermissionDesc(projectName, objectType, objectName, actionType);

// 指定 schema 的构造方法
PermissionDesc desc = new PermissionDesc(projectName, schemaName, objectType, objectName, actionType);
```

### 执行权限检查

调用 `checkPermission` 返回 `CheckPermissionResultInfo`，包含检查结果和附加信息：

```java
CheckPermissionResultInfo resultInfo = sm.checkPermission(desc);
CheckPermissionResult result = resultInfo.getResult(); // Allow 或 Deny
String message = resultInfo.getMessage();              // 附加说明信息
```

## 对象类型与操作类型

### ObjectType（对象类型）

| 枚举值 | 说明 |
|--------|------|
| `Project` | 项目 |
| `Table` | 表 |
| `Function` | 函数 |
| `Resource` | 资源 |
| `Instance` | 实例 |

### ActionType（操作类型）

| 枚举值 | 适用对象 | 说明 |
|--------|----------|------|
| `Read` | Project | 读取项目信息 |
| `Write` | Project | 写入项目信息 |
| `List` | Project | 列出项目中的对象 |
| `CreateTable` | Project | 在项目中创建表 |
| `CreateInstance` | Project | 在项目中创建实例 |
| `CreateFunction` | Project | 在项目中创建函数 |
| `CreateResource` | Project | 在项目中创建资源 |
| `Select` | Table | 查询表数据 |
| `Alter` | Table | 修改表结构 |
| `Update` | Table | 更新表数据 |
| `Drop` | Table | 删除表 |
| `Describe` | Table/Function/Resource | 查看对象元信息 |
| `Execute` | Function/Instance | 执行函数或实例 |
| `Delete` | Resource | 删除资源 |
| `Download` | Table | 下载表数据 |
| `All` | 所有 | 全部权限 |

:::info
ActionType 和 ObjectType 存在对应关系，例如不存在 "Read Table" 权限或 "Create Project" 权限。具体的权限列表请参考 [MaxCompute 权限文档](https://help.aliyun.com/zh/maxcompute/user-guide/maxcompute-permissions)。
:::

## 注意事项

- `checkPermission` 检查的是当前认证账号的权限
- 传入的参数（项目名、对象类型、对象名、操作类型）不能为 null，否则会抛出异常
- 权限检查结果可能受项目安全策略、角色绑定、Package 授权等多种因素影响
- 此方法仅做权限预检，不会实际执行操作
- 如果目标对象不存在，检查仍可能返回 Allow（权限模型不依赖对象是否存在）

## 相关文档

- [ACL 查询](./acl-query.md) - 执行 ACL 权限命令管理访问控制
- [权限检查参考](./check-permission.md) - checkPermission 详细类说明
