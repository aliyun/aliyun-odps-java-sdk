---
sidebar_position: 1
---
# 如何检查是否拥有某项权限

SDK中提供 SecurityManager 来提交权限相关的 SQL 和查询操作。

其中，可以使用`checkPermission`方法来检查权限。该方法主要用于验证用户是否具有特定操作的权限。

## 方法说明

### `checkPermission`

**描述**：检查权限。

**参数**：
- `desc`：权限描述对象，详见[PermissionDesc](#PermissionDesc)。

**返回**：
- `CheckPermissionResultInfo`：权限检查结果信息，详见[CheckPermissionResultInfo](#CheckPermissionResultInfo)。

**异常**：
- `OdpsException`：操作数据处理服务（ODPS）异常。

### 示例代码

```java
SecurityManager sm = odps.projects().get().getSecurityManager();
// 检查是否有对 "tableName" 的 Select Table 权限
PermissionDesc desc = new PermissionDesc("projectName", ObjectType.Table, "tableName", ActionType.Select);
try {
    CheckPermissionResultInfo resultInfo = sm.checkPermission(desc);
    CheckPermissionResult result = resultInfo.getResult();
    System.out.println("Permission check result: " + result.name()); // Deny or Allow
} catch (OdpsException e) {
    e.printStackTrace();
}
```

## 类说明

### `PermissionDesc`

**描述**：权限描述类，用于封装权限检查所需的参数。

**构造方法**：
- `PermissionDesc(String projectName, ObjectType objectType, String objectName, ActionType actionType)`
- `PermissionDesc(String projectName, String schemaName, ObjectType objectType, String objectName, ActionType actionType)`

**参数**：
- `projectName`：项目名称。
- `schemaName`：模式名称（可选）。
- `objectType`：对象类型，详见[ObjectType](#ObjectType)。
- `objectName`：对象名称。
- `actionType`：操作类型，详见[ActionType](#ActionType)。

### `CheckPermissionResultInfo`

**描述**：权限检查结果信息类。

**构造方法**：
- `CheckPermissionResultInfo(CheckPermissionResult result, String message)`

**参数**：
- `result`：权限检查结果，详见[CheckPermissionResult](#CheckPermissionResult)。
- `message`：结果信息。

## 枚举说明
:::info
实际上，ActionType 和 ObjectType 存在对应关系，比如不存在 Read Table 权限或 Create Project 权限。
具体的权限列表可以参考文档：[MaxCompute 权限](https://help.aliyun.com/zh/maxcompute/user-guide/maxcompute-permissions)
:::
### `ObjectType`

**描述**：对象类型枚举。

**枚举值**：
- `Project`
- `Table`
- `Function`
- `Resource`
- `Instance`

### `ActionType`

**描述**：操作类型枚举。

**枚举值**：
- `Read`
- `Write`
- `List`
- `CreateTable`
- `CreateInstance`
- `CreateFunction`
- `CreateResource`
- `All`
- `Describe`
- `Select`
- `Alter`
- `Update`
- `Drop`
- `Execute`
- `Delete`
- `Download`

### `CheckPermissionResult`

**描述**：权限检查结果枚举。

**枚举值**：
- `Allow`
- `Deny`

## 详细流程

1. **构建权限描述对象**：使用`PermissionDesc`类构建权限描述对象，包含项目名称、对象类型、对象名称和操作类型等信息。
2. **调用`checkPermission`方法**：将权限描述对象作为参数传递给`checkPermission`方法。
3. **处理返回结果**：根据返回的`CheckPermissionResultInfo`对象，获取权限检查结果和相关信息。

## 注意事项

- 确保传入的参数不为空，特别是项目名称、对象类型、对象名称和操作类型。
- 捕获并处理可能抛出的`OdpsException`异常。

通过以上步骤，您可以轻松实现权限检查功能，确保用户具有执行特定操作的权限。