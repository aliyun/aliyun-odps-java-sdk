---
title: 认证方式
description: MaxCompute Java SDK 支持的 5 种认证方式详解，包括 AccessKey、STS Token、CredentialProvider、双重签名和 Bearer Token。
sidebar_position: 2
module: odps-sdk-core
task: getting-started
apis: [AliyunAccount, StsAccount, AklessAccount, AppAccount, BearerTokenAccount]
keywords: [认证, AccessKey, STS, CredentialProvider, Bearer Token, 双重签名, Account]
---

# 认证方式

MaxCompute Java SDK 支持多种认证方式，适用于不同的使用场景。本文详细介绍每种认证方式的用法和适用场景。

## 如何选择

| 认证方式 | 适用场景 | 安全性 | 复杂度 |
|---------|---------|--------|--------|
| [AccessKey](#accesskey) | 开发测试、简单脚本 | 中 | 低 |
| [STS Token](#sts-token) | 临时授权、跨账号访问 | 高 | 中 |
| [CredentialProvider](#credentialprovider) | 生产环境、ECS/容器部署 | 高 | 中 |
| [双重签名](#双重签名) | 应用代理访问、多租户 | 高 | 高 |
| [Bearer Token](#bearer-token) | 短期访问、细粒度权限控制 | 高 | 中 |

**推荐**：生产环境建议使用 CredentialProvider，它支持凭据自动轮换和多种凭据来源（环境变量、配置文件、ECS RAM 角色等）。

## AccessKey

最常用的认证方式，使用阿里云 AccessKey ID 和 AccessKey Secret 进行身份验证。

### 示例代码

```java
import com.aliyun.odps.Account;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;

public class AccessKeyExample {

    public static void main(String[] args) {
        // 推荐从环境变量读取，避免硬编码
        String accessKeyId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
        String accessKeySecret = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");

        Account account = new AliyunAccount(accessKeyId, accessKeySecret);
        Odps odps = new Odps(account);
        odps.setEndpoint("http://service.odps.aliyun.com/api");
        odps.setDefaultProject("your_project");
    }
}
```

### 环境变量配置

强烈建议通过环境变量管理 AccessKey，避免将敏感信息写入代码或配置文件：

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID="your_access_key_id"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="your_access_key_secret"
```

### 获取 AccessKey

参考文档：[如何创建和获取 AccessKey](https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair)

:::warning
不要将 AccessKey 硬编码在代码中或提交到版本控制系统。生产环境请使用 CredentialProvider 方式。
:::

## STS Token

STS（Security Token Service）提供临时安全凭证，适用于临时授权场景，如移动端访问、跨账号操作等。STS Token 具有有效期，过期后自动失效。

### 示例代码

```java
import com.aliyun.odps.Account;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.StsAccount;

public class StsTokenExample {

    public static void main(String[] args) {
        String accessKeyId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
        String accessKeySecret = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
        String stsToken = System.getenv("ALIBABA_CLOUD_SECURITY_TOKEN");

        Account account = new StsAccount(accessKeyId, accessKeySecret, stsToken);
        Odps odps = new Odps(account);
        odps.setEndpoint("http://service.odps.aliyun.com/api");
        odps.setDefaultProject("your_project");
    }
}
```

### 适用场景

- 移动应用临时访问 MaxCompute
- 第三方应用临时授权
- 跨账号资源访问
- 需要限时访问的场景

## CredentialProvider

使用阿里云 Credential SDK 的 `ICredentialsProvider` 接口，支持多种凭据来源和自动轮换。这是生产环境的推荐方式。

### 添加依赖

除了 MaxCompute SDK 外，还需要引入阿里云 Credential SDK：

```xml
<dependency>
    <groupId>com.aliyun</groupId>
    <artifactId>credentials-java</artifactId>
    <version>0.3.12</version>
</dependency>
```

### 示例代码

```java
import com.aliyun.credentials.Client;
import com.aliyun.credentials.models.Config;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AklessAccount;

public class CredentialProviderExample {

    public static void main(String[] args) {
        // 方式一：使用默认凭据链（推荐）
        // 按顺序尝试：环境变量 > 配置文件 > ECS RAM 角色
        Client credentialClient = new Client();
        AklessAccount account = new AklessAccount(credentialClient);
        Odps odps = new Odps(account);
        odps.setEndpoint("http://service.odps.aliyun.com/api");
        odps.setDefaultProject("your_project");
    }

    /**
     * 方式二：显式指定 ECS RAM 角色
     */
    public static Odps buildWithEcsRamRole(String roleName) {
        Config config = new Config();
        config.setType("ecs_ram_role");
        config.setRoleName(roleName);

        Client credentialClient = new Client(config);
        AklessAccount account = new AklessAccount(credentialClient);
        Odps odps = new Odps(account);
        odps.setEndpoint("http://service.odps.aliyun.com/api");
        odps.setDefaultProject("your_project");
        return odps;
    }

    /**
     * 方式三：使用环境变量中的 AK（由 Credential SDK 管理）
     */
    public static Odps buildWithEnvCredential() {
        Config config = new Config();
        config.setType("access_key");
        config.setAccessKeyId(System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"));
        config.setAccessKeySecret(System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"));

        Client credentialClient = new Client(config);
        AklessAccount account = new AklessAccount(credentialClient);
        Odps odps = new Odps(account);
        odps.setEndpoint("http://service.odps.aliyun.com/api");
        odps.setDefaultProject("your_project");
        return odps;
    }
}
```

### 说明

`ICredentialsProvider` 是 [credentials-api](https://github.com/aliyun/alibabacloud-credentials-api/tree/master/java) 包定义的接口，[credentials-java](https://github.com/aliyun/credentials-java) 包提供了多种实现。

使用 CredentialProvider 的优势：
- 支持凭据自动刷新和轮换
- 支持多种凭据来源（环境变量、配置文件、ECS RAM 角色、OIDC 等）
- 统一的凭据管理方式，与其他阿里云 SDK 一致

:::note
`ICredentialsProvider` 实现需要自行管理凭据缓存和轮换，以确保性能不发生回退。
:::

### 参考链接

- [阿里云 CredentialProvider 使用文档](https://help.aliyun.com/zh/sdk/developer-reference/v2-manage-access-credentials)

## 双重签名

双重签名认证使用两组 AccessKey：一组标识应用身份，一组标识用户身份。适用于应用代理用户访问 MaxCompute 的场景。

### 示例代码

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.AppAccount;

public class DualSignatureExample {

    public static void main(String[] args) {
        // 用户身份凭证
        String userAccessKeyId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
        String userAccessKeySecret = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");

        // 应用身份凭证
        String appAccessKeyId = System.getenv("APP_ACCESS_KEY_ID");
        String appAccessKeySecret = System.getenv("APP_ACCESS_KEY_SECRET");

        // 创建应用账号（用于应用标识）
        AppAccount appAccount = new AppAccount(new AliyunAccount(appAccessKeyId, appAccessKeySecret));

        // 创建用户账号（用于用户标识）
        AliyunAccount userAccount = new AliyunAccount(userAccessKeyId, userAccessKeySecret);

        // 使用双重签名创建 Odps 实例
        Odps odps = new Odps(userAccount, appAccount);
        odps.setEndpoint("http://service.odps.aliyun.com/api");
        odps.setDefaultProject("your_project");
    }
}
```

### 适用场景

- 多租户 SaaS 应用代理用户访问
- 需要同时追踪应用和用户身份的场景
- 应用层面的访问控制和审计

## Bearer Token

Bearer Token 通常用于短期访问授权，配合 MaxCompute 的 Policy 权限控制使用。Token 具有时效性，过期后需要重新生成。

### 示例代码

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.BearerTokenAccount;

public class BearerTokenExample {

    public static void main(String[] args) {
        String bearerToken = "your_bearer_token";

        BearerTokenAccount account = new BearerTokenAccount(bearerToken);
        Odps odps = new Odps(account);
        odps.setEndpoint("http://service.odps.aliyun.com/api");
        odps.setDefaultProject("your_project");
    }
}
```

### 生成 Bearer Token

Bearer Token 通过 SecurityManager 生成，需要指定 Policy 规则：

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.security.SecurityManager;

public class GenerateBearerToken {

    public static String generateToken(Odps odps, String policy) throws Exception {
        SecurityManager sm = odps.projects().get().getSecurityManager();
        return sm.generateAuthorizationToken(policy, "BEARER");
    }
}
```

### 适用场景

- 需要细粒度权限控制的短期访问
- 为特定操作生成限时凭证
- 配合 Policy 实现最小权限原则

### 参考链接

- [Policy 权限控制文档](https://help.aliyun.com/zh/maxcompute/user-guide/policy-based-access-control-1)

## 通用配置

无论使用哪种认证方式，创建 `Odps` 实例后都需要配置 Endpoint 和默认项目：

```java
Odps odps = new Odps(account);

// 设置 Endpoint（必需）
odps.setEndpoint("http://service.odps.aliyun.com/api");

// 设置默认项目（必需）
odps.setDefaultProject("your_project");
```

### Endpoint 列表

不同地域使用不同的 Endpoint，常用地域如下：

| 地域 | Endpoint |
|------|----------|
| 华东2（上海） | `http://service.cn-shanghai.maxcompute.aliyun.com/api` |
| 华北2（北京） | `http://service.cn-beijing.maxcompute.aliyun.com/api` |
| 华南1（深圳） | `http://service.cn-shenzhen.maxcompute.aliyun.com/api` |
| 华东1（杭州） | `http://service.cn-hangzhou.maxcompute.aliyun.com/api` |

完整列表请参考：[MaxCompute 开服地域和 Endpoint](https://help.aliyun.com/zh/maxcompute/user-guide/endpoints)

## 参考链接

- [阿里云 RAM 用户指南](https://help.aliyun.com/zh/maxcompute/getting-started/prepare-a-ram-user)
- [阿里云 MaxCompute 官方文档](https://help.aliyun.com/zh/maxcompute)
