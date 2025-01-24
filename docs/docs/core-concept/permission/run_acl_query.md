---
sidebar_position: 1
---
# 执行 ACL 权限命令

管理用户、角色和租户的访问控制
，在 MaxCompute 中，可以使用 SDK 执行 ACL（Access Control List）权限命令。 通过 ACL 命令，您可以精确控制谁可以访问哪些资源，以及他们可以执行哪些操作。

## 1. ACL 权限命令概述

ACL 权限命令主要包括以下操作：
- **授权（GRANT）**：将权限授予用户或角色。
- **撤销（REVOKE）**：从用户或角色中撤销权限。
- **查看权限（SHOW GRANTS）**：查看用户或角色的权限。
- **清除过期授权（CLEAR EXPIRED GRANTS）**：清理已过期的权限。
- **资源管理**：包括创建、删除、添加、移除、描述资源等操作。

## 2. 常见的 ACL 权限命令

以下是常见的 ACL 权限命令示例：

### 2.1 授权（GRANT）
- **授予用户对表的 Select 权限**：
  ```sql
  GRANT Select ON TABLE sales TO USERRAM$Bob@aliyun.com:Allen;
  ```
- **授予角色对项目的 CreateInstance 权限**：
  ```sql
  GRANT CreateInstance ON PROJECT my_project TO ROLE admin_role;
  ```
- **授予用户对包的 Read 权限**：
  ```sql
  GRANT Read ON PACKAGE my_project.my_package TO USER bob;
  ```
:::note
  全部支持的命令参考 [官方文档 Grant](https://help.aliyun.com/zh/maxcompute/user-guide/grant)
:::

### 2.2 撤销（REVOKE）
- **撤销用户对表的 Select 权限**：
  ```sql
  REVOKE Select ON TABLE sales FROM USER Allen;
  ```
- **撤销角色对项目的 CreateInstance 权限**：
  ```sql
  REVOKE CreateInstance ON PROJECT my_project FROM ROLE admin_role;
  ```
- **撤销用户对包的 Read 权限**：
  ```sql
  REVOKE Read ON PACKAGE my_project.my_package FROM USER bob;
  ```

:::note
全部支持的命令参考 [官方文档 Revoke](https://help.aliyun.com/zh/maxcompute/user-guide/revoke)
:::

### 2.3 查看权限（SHOW GRANTS）
- **查看用户的权限**：
  ```sql
  SHOW GRANTS FOR USER alice;
  ```
- **查看角色的权限**：
  ```sql
  SHOW GRANTS FOR ROLE analyst;
  ```
- **查看当前用户的权限**：
  ```sql
  SHOW GRANTS;
  ```

### 2.4 清除过期授权（CLEAR EXPIRED GRANTS）
- **清除所有过期的授权**：
  ```sql
  CLEAR EXPIRED GRANTS;
  ```

### 2.4 列出资源 (LIST)
- **列出所有用户**：
  ```sql
  LIST USERS;
  ```
- **列出所有角色**：
  ```sql
  LIST ROLES;
  ```
- **列出所有租户用户**：
  ```sql
  LIST TENANT USERS;
  ```
- **列出所有租户角色**：
  ```sql
  LIST TENANT ROLES;
  ```

### 2.5 创建资源（CREATE）
- **创建角色**：
  ```sql
  CREATE ROLE analyst;
  ```
- **创建包**：
  ```sql
  CREATE PACKAGE my_package;
  ```
- **创建租户角色**：
  ```sql
  CREATE TENANT ROLE tenant_admin;
  ```

### 2.6 删除资源（DROP)
- **删除角色**：
  ```sql
  DROP ROLE analyst;
  ```
- **删除包**：
  ```sql
  DELETE PACKAGE my_package;
  ```
- **删除租户角色**：
  ```sql
  DROP TENANT ROLE tenant_admin;
  ```

### 2.7 添加资源（ADD）
- **添加用户**：
  ```sql
  ADD USER alice;
  ```
- **添加租户用户**：
  ```sql
  ADD TENANT USER bob;
  ```

### 2.8 移除资源（REMOVE）
- **移除用户**：
  ```sql
  REMOVE USER alice;
  ```
- **移除租户用户**：
  ```sql
  REMOVE TENANT USER bob;
  ```

### 2.9 描述资源 (DESCRIBE)
- **描述角色**：
  ```sql
  DESCRIBE ROLE analyst;
  ```
- **描述包**：
  ```sql
  DESC PACKAGE my_package;
  ```

### 2.10 包管理
- **安装包**：
  ```sql
  INSTALL PACKAGE my_package;
  ```
- **卸载包**：
  ```sql
  UNINSTALL PACKAGE my_package;
  ```

### 2.11 标签管理
- **设置表标签**：
  ```sql
  SET LABEL 1 TO TABLE sales;
  ```
- **设置用户标签**：
  ```sql
  SET LABEL 1 TO USER alices;
  ```
- **取消标签**：
  ```sql
  REVOKE LABEL ON TABLE sales FROM USER alices;
  ```

### 2.12 项目权限
- **允许项目安装包**：
  ```sql
  ALLOW PROJECT my_project TO INSTALL PACKAGE my_package;
  ```
- **禁止项目访问**：
  ```sql
  DISALLOW PROJECT my_project TO INSTALL PACKAGE my_package;
  ```
---

## 3. 执行 ACL 权限命令的代码示例

以下是一个完整的 Java 示例代码，展示如何通过 MaxCompute SDK 执行 ACL 权限命令：

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;

public class AclCommandExample {

    public static void main(String[] args) {
        try {
            // 初始化 Odps 对象
            Odps odps = new Odps(account);
            odps.setDefaultProject("my_project");
            odps.setEndpoint("http://service.odps.aliyun.com/api");

            // 获取 SecurityManager
            SecurityManager sm = odps.projects().get().getSecurityManager();

            // 执行 GRANT 命令
            String grantCommand = "GRANT SELECT ON TABLE sales TO USER alice;";
            executeAclCommand(sm, grantCommand);

            // 执行 REVOKE 命令
            String revokeCommand = "REVOKE SELECT ON TABLE sales FROM USER alice;";
            executeAclCommand(sm, revokeCommand);

            // 执行 SHOW GRANTS 命令
            String showGrantsCommand = "SHOW GRANTS FOR USER alice;";
            executeAclCommand(sm, showGrantsCommand);

        } catch (OdpsException e) {
            e.printStackTrace();
        }
    }

    private static void executeAclCommand(SecurityManager sm, String command) throws OdpsException {
        // 执行 ACL 命令
        SecurityManager.AuthorizationQueryInstance instance = sm.run(command, false, null, null);

        // 等待命令执行完成
        while (!instance.isTerminated()) {
            System.out.println("Waiting for command to complete...");
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new OdpsException("Command execution interrupted.");
            }
        }

        // 获取命令结果
        String result = instance.getResult();
        if (instance.getStatus() == SecurityManager.AuthorizationQueryStatus.FAILED) {
            throw new OdpsException("Command failed: " + result);
        }

        // 输出结果
        if (StringUtils.isNullOrEmpty(result)) {
            System.out.println("OK");
        } else {
            System.out.println(result);
        }
    }
}
```
---

通过本文，您可以快速掌握如何在 MaxCompute 中执行 ACL 权限命令。如果有任何问题，请参考官方文档或联系技术支持。