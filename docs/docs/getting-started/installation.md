---
title: 安装与配置
description: 在项目中引入 MaxCompute SDK 依赖，支持 Java（Maven/Gradle）、Python（pip）和 Go（go get）。
sidebar_position: 1
module: odps-sdk-core
task: getting-started
apis: []
keywords: [maven, gradle, pip, go get, 安装, 依赖, repository, java版本]
---

# 安装与配置

本文介绍如何在项目中引入 MaxCompute SDK 依赖。

## 安装 SDK

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

### SDK 模块说明

MaxCompute Java SDK 由多个模块组成，可按需引入：

| 模块 | artifactId | 说明 |
|------|-----------|------|
| Core | `odps-sdk-core` | 核心模块，包含客户端、表操作、SQL 执行、Tunnel 数据通道等功能 |
| Table API | `odps-sdk-table-api` | 表数据读写高级 API，提供流式读写能力 |
| Storage API | `odps-sdk-storage-api` | 存储层访问接口，用于直接读写底层存储数据 |
| UDF | `odps-sdk-udf` | 用户自定义函数开发框架 |

大多数场景只需引入 `odps-sdk-core` 即可。

### Maven

#### 基本依赖

在项目的 `pom.xml` 中添加依赖：

```xml
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-core</artifactId>
    <version>0.57.2-public</version>
</dependency>
```

#### 引入其他模块

根据需要添加其他模块：

```xml
<!-- 表数据读写高级 API -->
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-table-api</artifactId>
    <version>0.57.2-public</version>
</dependency>

<!-- 存储层访问接口 -->
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-storage-api</artifactId>
    <version>0.57.2-public</version>
</dependency>

<!-- UDF 开发框架 -->
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-udf</artifactId>
    <version>0.57.2-public</version>
</dependency>
```

#### 仓库配置

SDK 发布在 Maven Central，可直接使用。如需加速下载，可配置阿里云 Maven 镜像仓库：

```xml
<repositories>
    <repository>
        <id>aliyun-public</id>
        <url>https://maven.aliyun.com/repository/public</url>
    </repository>
</repositories>
```

也可以在 Maven 的全局配置文件 `~/.m2/settings.xml` 中配置镜像：

```xml
<mirrors>
    <mirror>
        <id>aliyun-mirror</id>
        <mirrorOf>central</mirrorOf>
        <url>https://maven.aliyun.com/repository/public</url>
    </mirror>
</mirrors>
```

#### 关于 BOM

当前 MaxCompute Java SDK 未提供 BOM（Bill of Materials）。如果项目中同时使用多个模块，请确保所有模块版本保持一致。

### Gradle

#### 基本依赖

在 `build.gradle` 中添加：

```groovy
dependencies {
    implementation 'com.aliyun.odps:odps-sdk-core:0.57.2-public'
}
```

#### 引入其他模块

```groovy
dependencies {
    implementation 'com.aliyun.odps:odps-sdk-core:0.57.2-public'
    implementation 'com.aliyun.odps:odps-sdk-table-api:0.57.2-public'
    implementation 'com.aliyun.odps:odps-sdk-storage-api:0.57.2-public'
    implementation 'com.aliyun.odps:odps-sdk-udf:0.57.2-public'
}
```

#### 仓库配置

```groovy
repositories {
    mavenCentral()
    // 阿里云镜像（可选，用于加速下载）
    maven { url 'https://maven.aliyun.com/repository/public' }
}
```

如果使用 Kotlin DSL（`build.gradle.kts`）：

```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://maven.aliyun.com/repository/public") }
}

dependencies {
    implementation("com.aliyun.odps:odps-sdk-core:0.57.2-public")
}
```

### Java 版本要求

| SDK 版本 | 最低 Java 版本 | 说明 |
|---------|--------------|------|
| 0.54.0 及以上 | Java 8 | 同时支持 Java 21 |
| 0.54.0 以下 | Java 8 | 仅支持 Java 8 |

- **最低要求**：Java 8（JDK 1.8）
- **推荐版本**：Java 8 或 Java 21
- 自 0.54.0 版本起，SDK 完整支持 Java 21 运行时

</TabItem>
<TabItem value="python" label="Python">

### 安装

使用 pip 安装 PyODPS：

```bash
pip install pyodps
```

### 可选依赖

如需使用 DataFrame 功能，建议同时安装 pandas：

```bash
pip install pyodps[pandas]
```

### Python 版本要求

- **最低要求**：Python 3.6
- **推荐版本**：Python 3.8+

</TabItem>
<TabItem value="go" label="Go">

### 安装

使用 `go get` 安装 Go SDK：

```bash
go get github.com/aliyun/aliyun-odps-go-sdk
```

### Go 版本要求

- **最低要求**：Go 1.18
- **推荐版本**：Go 1.20+

</TabItem>
</Tabs>

## 验证安装

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

创建一个简单的测试类来验证 SDK 是否正确引入：

```java
import com.aliyun.odps.Odps;

public class VerifyInstallation {
    public static void main(String[] args) {
        System.out.println("MaxCompute Java SDK 引入成功");
        System.out.println("Odps class: " + Odps.class.getName());
    }
}
```

如果编译运行成功，说明 SDK 已正确安装。

</TabItem>
<TabItem value="python" label="Python">

在 Python 中验证安装：

```python
import odps
print("PyODPS 安装成功")
print(f"版本: {odps.__version__}")
```

</TabItem>
<TabItem value="go" label="Go">

创建一个简单的程序验证安装：

```go
package main

import (
    "fmt"
    "github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func main() {
    fmt.Println("MaxCompute Go SDK 引入成功")
    _ = odps.NewOdps
}
```

运行 `go run main.go`，如果没有报错则安装成功。

</TabItem>
</Tabs>

## 下一步

- [认证方式](./authentication.md) - 了解如何配置身份认证
- [第一个程序](./first-program.md) - 编写第一个 MaxCompute 程序
