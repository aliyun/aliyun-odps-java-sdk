---
title: Configuration 配置类
sidebar_position: 1
---

# Configuration 配置类

## 概述

`Configuration` 类用于保存与 ODPS Tunnel
服务通讯过程中需要的配置信息。该类继承自 `GeneralConfiguration`，并扩展了一些特定于 ODPS Tunnel 的配置项。

## 构造方法

### Configuration(Odps odps)

```java
public Configuration(Odps odps)
```

通过 `Odps` 对象构建 `Configuration` 实例。

### Configuration(Builder builder)

```java
public Configuration(Builder builder)
```

通过 [`Builder`](#builder-类) 对象构建 `Configuration` 实例，`Builder`
类可以通过静态方法 [`Configuration#builder(odps)`](#创建-configuration-builder) 方法构建。

#### 示例代码

```java
import com.aliyun.odps.tunnel.Configuration;

Configuration configuration=Configuration.builder(odps)
    .withRetryLogger(new RetryLogger())
    .withRetryPolicy(new FlinkDefaultRetryPolicy())
    // default compress option is zlib
    .withCompressOptions(new CompressOption())
    .build();
```

## 方法

### 创建 Configuration Builder

```java
public static Builder builder(Odps odps)
```

创建并返回一个新的 `Builder` 对象。

### 获取压缩选项

```java
public CompressOption getCompressOption()
```

返回当前的压缩选项。所有支持的压缩选项参考 [`CompressOption` 类文档](../CompressOption.md)。

### 设置压缩选项

```java
public void setCompressOption(CompressOption option)
```

设置压缩选项。所有支持的压缩选项参考 [`CompressOption` 类文档](../CompressOption.md)。

### 获取指定Project的Tunnel服务入口地址

```java
@Override
public URI getEndpoint(String projectName)throws TunnelException
```

返回指定Project的Tunnel服务入口地址。

与 MaxCompute Endpoint 不同，Tunnel Endpoint 由 SDK
自动获取，通常无须用户手动指定。这个方法实现了这个路由逻辑，通常无须用户调用。

### 获取Odps对象

```java
public Odps getOdps()
```

返回当前的 `Odps` 对象。

### 获取 Quota 名称

```java
public String getQuotaName()
```
返回当前使用的 Quota 名称。

### 获取标签列表

```java
public List<String> getTags()
```

返回当前的标签列表。

### 获取重试策略

```java
public TunnelRetryHandler.RetryPolicy getRetryPolicy()
```

返回当前的重试策略。重试策略介绍参考[`RetryPolicy`](RetryLogic.md#retrypolicy-重试策略)。

### 获取重试日志记录器

```java
public RestClient.RetryLogger getRetryLogger()
```

返回当前的重试日志记录器。

### 设置 Quota 名称

```java
public void setQuotaName(String quotaName)
```

设置希望使用的 Quota 名称。

### 检查 Quota 名称是否可用

```java
public boolean availableQuotaName()
```

检查当前设置的 Quota 名称是否可用，实际上这个接口不会真正检查 Quota 是否存在，只会检查用户是否设置了 Quota 名称。

### 创建新的RestClient

```java
public RestClient newRestClient(String projectName)throws TunnelException
```

创建并返回一个新的 `RestClient` 对象。

### 转换为Builder

```java
public Builder toBuilder()
```

将当前 `Configuration` 对象转换为 `Configuration.Builder` 对象。

## Builder 类

`Configuration.Builder` 类用于构建 `Configuration` 对象。

### 构造方法

```java
private Builder(Odps odps)
```

通过 `Odps` 对象构建 `Builder` 实例。

### 方法

#### 设置 Quota 名称

```java
public Builder withQuotaName(String quotaName)
```

设置配额名称。

#### 设置标签列表

```java
public Builder withTags(List<String> tags)
```

设置标签列表。

#### 设置压缩选项

```java
public Builder withCompressOptions(CompressOption compressOption)
```

设置压缩选项。所有支持的压缩选项参考 [`CompressOption` 类文档](../CompressOption.md)。

#### 设置重试策略

```java
public Builder withRetryPolicy(TunnelRetryHandler.RetryPolicy retryPolicy)
```

设置重试策略。默认重试策略和配置方式，参考[`RetryPolicy`](RetryLogic.md#retrypolicy-重试策略)。

#### 设置重试日志记录器

```java
public Builder withRetryLogger(RestClient.RetryLogger retryLogger)
```

设置重试日志记录器。参考[`RetryLogger`](RetryLogic.md#retrylogger-接口)。

#### 构建Configuration对象

```java
public Configuration build()
```

构建并返回一个 `Configuration` 对象。