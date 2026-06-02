---
title: ACL 查询
description: 通过 Java SDK 执行 ACL/Policy 权限命令，管理用户、角色和资源的访问控制
sidebar_position: 2
module: odps-sdk-core
task: security
apis:
  - SecurityManager
since: "0.27.0"
keywords:
  - ACL
  - GRANT
  - REVOKE
  - 授权
  - 角色管理
  - 访问控制
---

# ACL 查询

在 MaxCompute 中，可以通过 `SecurityManager` 执行 ACL（Access Control List）权限命令来管理访问控制。ACL 命令支持授权、撤销、查看权限、角色管理等操作，与在 MaxCompute Console 中执行权限 SQL 的效果一致。

## 前置条件

- 已完成 [认证配置](../../getting-started/authentication.md)
- 已添加 `odps-sdk-core` 依赖
- 当前账号需要有项目的 Admin 角色或相应的管理权限

## 完整示例

以下示例演示如何通过 SDK 执行常见的 ACL 权限命令：

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;

public class AclQueryExample {

    public static void main(String[] args) throws OdpsException {
        // 初始化 Odps 客户端
        Odps odps = new Odps(new AliyunAccount("accessId", "accessKey"));
        odps.setDefaultProject("my_project");
        odps.setEndpoint("http://service.odps.aliyun.com/api");

        // 获取 SecurityManager
        SecurityManager sm = odps.projects().get().getSecurityManager();

        // 1. 授予用户对表的 Select 权限
        executeCommand(sm, "GRANT SELECT ON TABLE sales_data TO USER alice;");

        // 2. 查看用户的权限
        executeCommand(sm, "SHOW GRANTS FOR USER alice;");

        // 3. 创建角色并绑定权限
        executeCommand(sm, "CREATE ROLE analyst;");
        executeCommand(sm, "GRANT SELECT ON TABLE sales_data TO ROLE analyst;");
        executeCommand(sm, "GRANT analyst TO alice;");

        // 4. 查看角色的权限
        executeCommand(sm, "DESCRIBE ROLE analyst;");

        // 5. 撤销权限
        executeCommand(sm, "REVOKE SELECT ON TABLE sales_data FROM USER alice;");

        // 6. 查看当前用户的权限
        executeCommand(sm, "SHOW GRANTS;");
    }

    /**
     * 执行 ACL 命令并输出结果
     */
    private static void executeCommand(SecurityManager sm, String command) throws OdpsException {
        System.out.println("执行: " + command);

        // 提交命令
        SecurityManager.AuthorizationQueryInstance instance = sm.run(command, false, null, null);

        // 等待执行完成
        while (!instance.isTerminated()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new OdpsException("命令执行被中断");
            }
        }

        // 检查执行状态
        if (instance.getStatus() == SecurityManager.AuthorizationQueryStatus.FAILED) {
            throw new OdpsException("命令执行失败: " + instance.getResult());
        }

        // 输出结果
        String result = instance.getResult();
        if (StringUtils.isNullOrEmpty(result)) {
            System.out.println("OK\n");
        } else {
            System.out.println(result + "\n");
        }
    }
}
```

## 代码说明

### 获取 SecurityManager

```java
SecurityManager sm = odps.projects().get().getSecurityManager();
```

### 执行 ACL 命令

通过 `run` 方法提交 ACL 命令，返回一个异步查询实例：

```java
SecurityManager.AuthorizationQueryInstance instance = sm.run(command, false, null, null);
```

参数说明：
- `command`：要执行的 ACL SQL 命令
- `useJson`：是否以 JSON 格式返回结果（`false` 表示普通文本格式）
- `projectName`：指定在哪个项目上下文中执行（`null` 使用默认项目）
- `supervisorToken`：supervisor token（通常为 `null`）

### 等待命令完成

ACL 命令是异步执行的，需要轮询等待完成：

```java
while (!instance.isTerminated()) {
    Thread.sleep(1000);
}
```

### 获取执行结果

```java
// 检查是否失败
if (instance.getStatus() == SecurityManager.AuthorizationQueryStatus.FAILED) {
    System.err.println("失败: " + instance.getResult());
}

// 获取结果文本
String result = instance.getResult();
```

## 常见 ACL 命令

### 授权（GRANT）

```sql
-- 授予用户对表的权限
GRANT SELECT ON TABLE table_name TO USER user_name;

-- 授予角色对项目的权限
GRANT CreateInstance ON PROJECT project_name TO ROLE role_name;

-- 授予用户对 Package 的权限
GRANT Read ON PACKAGE project_name.package_name TO USER user_name;
```

### 撤销（REVOKE）

```sql
-- 撤销用户对表的权限
REVOKE SELECT ON TABLE table_name FROM USER user_name;

-- 撤销角色对项目的权限
REVOKE CreateInstance ON PROJECT project_name FROM ROLE role_name;
```

### 查看权限（SHOW GRANTS）

```sql
-- 查看指定用户的权限
SHOW GRANTS FOR USER user_name;

-- 查看指定角色的权限
SHOW GRANTS FOR ROLE role_name;

-- 查看当前用户的权限
SHOW GRANTS;
```

### 角色管理

```sql
-- 创建角色
CREATE ROLE role_name;

-- 将角色授予用户
GRANT role_name TO user_name;

-- 查看角色详情
DESCRIBE ROLE role_name;

-- 删除角色
DROP ROLE role_name;

-- 列出所有角色
LIST ROLES;
```

### 用户管理

```sql
-- 添加用户
ADD USER user_name;

-- 移除用户
REMOVE USER user_name;

-- 列出所有用户
LIST USERS;
```

### 清除过期授权

```sql
CLEAR EXPIRED GRANTS;
```

## 配置选项

### run 方法参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| command | String | - | ACL SQL 命令（必填） |
| useJson | boolean | false | 是否以 JSON 格式返回结果 |
| projectName | String | null | 目标项目名（null 使用默认项目） |
| supervisorToken | String | null | Supervisor Token |

### AuthorizationQueryStatus 状态

| 状态 | 说明 |
|------|------|
| `SUCCESS` | 命令执行成功 |
| `FAILED` | 命令执行失败 |

## 注意事项

- ACL 命令是异步执行的，必须等待 `isTerminated()` 返回 `true` 后再获取结果
- 执行授权/撤销操作需要当前账号拥有 Admin 角色或 Super_Administrator 角色
- ACL 命令的语法与 MaxCompute Console 中的权限 SQL 完全一致
- 命令执行失败时，通过 `getResult()` 获取错误信息进行排查
- 建议设置合理的轮询间隔（如 1-3 秒），避免频繁请求
- 批量执行多条 ACL 命令时，建议逐条执行并检查结果，而非拼接为一条

## 相关文档

- [权限校验](./check-permission.md) - 检查用户是否拥有特定权限
- [ACL 权限命令参考](./acl-query.md) - 完整 ACL 命令列表
- [MaxCompute 权限管理](https://help.aliyun.com/zh/maxcompute/user-guide/maxcompute-permissions) - 官方权限文档
