---
title: 构建 ODPS 客户端
sidebar_label: 构建 ODPS 客户端
sidebar_position: 1
---

# 构建 ODPS 客户端

本文档展示了如何使用多种认证方式构建阿里云MaxCompute（原ODPS）客户端实例。MaxCompute是一种大规模数据处理和分析服务，本SDK帮助用户快速集成和使用该服务。

## 目录
1. [使用AccessKey构建客户端](#使用AccessKey构建客户端)
2. [使用STS Token构建客户端](#使用STS-Token构建客户端)
3. [使用阿里云CredentialProvider构建客户端](#使用阿里云CredentialProvider构建客户端)
4. [使用双重签名认证构建客户端](#使用双重签名认证构建客户端)
5. [使用Bearer Token构建客户端](#使用Bearer-Token构建客户端)

## 使用AccessKey构建客户端

此方法需要用户提供AccessId和AccessKey。

### 示例代码

```java
public static Odps buildWithAccessKey(String accessId, String accessKey) {
  Account account = new AliyunAccount(accessId, accessKey);
  Odps odps = new Odps(account);
  odps.setEndpoint(SAMPLE_ENDPOINT);
  return odps;
}
```

### 参考链接

[如何创建和获取AccessKey](https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair)

## 使用STS Token构建客户端

此方法适用于临时授权场景。

### 示例代码

```java
public static Odps buildWithStsToken(String accessId, String accessKey, String stsToken) {
  Account account = new StsAccount(accessId, accessKey, stsToken);
  Odps odps = new Odps(account);
  odps.setEndpoint(SAMPLE_ENDPOINT);
  return odps;
}
```

## 使用阿里云CredentialProvider构建客户端

此方法适用于如ECS实例上的RAM角色授权等场景。

### 示例代码

```java
public static Odps buildWithCredentialProvider(ICredentialProvider credentialProvider) {
  Account account = new AklessAccount(credentialProvider);
  Odps odps = new Odps(account);
  odps.setEndpoint(SAMPLE_ENDPOINT);
  return odps;
}
```

### 说明

CredentialProvider 是阿里云提供的一种无AK认证方式，它实际上提供了一种基于STS Token的生成和自动轮换机制。阿里云的`aliyun-java-auth`包提供了多种`ICredentialProvider`的实现，例如`DefaultCredentialProvider`和`RamRoleArnCredentialProvider`，用户可以根据需要选择不同的实现。

## 使用双重签名认证构建客户端

某些应用需要双重签名认证（本质上是使用一组AK进行应用标识，另一组AK进行用户标识）。

### 示例代码

```java
public static Odps buildWithDualSignature(String accessId, String accessKey, String appAccessId, String appAccessKey) {
  AppAccount appAccount = new AppAccount(new AliyunAccount(appAccessId, appAccessKey));
  Account account = new AliyunAccount(accessId, accessKey);
  Odps odps = new Odps(account, appAccount);
  odps.setEndpoint(SAMPLE_ENDPOINT);
  return odps;
}
```

## 使用Bearer Token构建客户端

Bearer Tokens通常用于短期访问授权。

### 示例代码

```java
public static Odps buildWithBearerToken(String bearerToken) {
  Account account = new BearerTokenAccount(bearerToken);
  Odps odps = new Odps(account);
  odps.setEndpoint(SAMPLE_ENDPOINT);
  return odps;
}
```

### 生成Bearer Token

Bearer Token的生成方法如下：

```java
// 示例代码
SecurityManager sm = new SecurityManager();
String policy = "<your policy>";
String bearerToken = sm.generateAuthorizationToken(policy, "BEARER");
```

### 参考链接

- [阿里云MaxCompute官方文档](https://help.aliyun.com/zh/maxcompute)
- [阿里云RAM用户指南](https://help.aliyun.com/zh/maxcompute/getting-started/prepare-a-ram-user)
- [Policy 权限控制文档](https://help.aliyun.com/zh/maxcompute/user-guide/policy-based-access-control-1)
- [阿里云 CredentialProvider 使用文档](https://help.aliyun.com/zh/sdk/developer-reference/v2-java-integrated-sdk)

