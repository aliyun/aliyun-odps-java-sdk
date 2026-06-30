# ODPS SDK SimpleXML

## 概述

`odps-sdk-simplexml` 是一个基于 Simple XML 框架的 XML 序列化框架，包含了安全修复，用于替代原版 Simple XML 框架中存在漏洞的版本。

## 背景

原版 Simple XML 框架存在 XML 外部实体（XXE）等安全漏洞。本模块通过对 Simple XML 源码进行修改，修复了这些安全问题，提供了一个安全的 XML 序列化解决方案。

## 特性

- **安全修复**: 修复了原版 Simple XML 框架中的 XXE 等安全漏洞
- **独立模块**: 从 `odps-sdk-core` 中提取出来，形成独立的 Maven 模块
- **向后兼容**: 保持与原版 API 的兼容性，现有代码无需修改即可使用
- **轻量级**: 仅包含 XML 序列化所需的核心功能

## 依赖

- `stax-api`: StAX API
- `stax`: StAX 实现
- `xpp3`: XML Pull Parser

## 使用方式

### Maven 依赖

```xml
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-simplexml</artifactId>
    <version>${project.version}</version>
</dependency>
```

### 基本用法

```java
import com.aliyun.odps.simpleframework.xml.Serializer;
import com.aliyun.odps.simpleframework.xml.core.Persister;

// 创建序列化器
Serializer serializer = new Persister();

// 序列化对象到 XML
serializer.write(object, outputStream);

// 从 XML 反序列化对象
MyObject obj = serializer.read(MyObject.class, inputStream);
```

## 模块结构

```
odps-sdk-simplexml/
├── src/main/java/com/aliyun/odps/simpleframework/
│   ├── xml/              # XML 注解和核心接口
│   │   ├── convert/      # 类型转换器
│   │   ├── core/         # 核心实现
│   │   ├── filter/       # 过滤器
│   │   ├── strategy/     # 序列化策略
│   │   ├── stream/       # XML 流处理
│   │   ├── transform/    # 类型转换
│   │   └── util/         # 工具类
└── pom.xml
```

## 安全注意事项

本模块已修复以下安全问题：

1. **XXE 漏洞**: 禁用了外部实体解析，防止 XML 外部实体攻击
2. **XML 注入**: 加强了 XML 解析的安全性
3. **DoS 防护**: 添加了资源限制，防止拒绝服务攻击

## 迁移说明

如果您之前使用的是 `odps-sdk-core` 内置的 simpleframework，迁移到独立模块非常简单：

1. 添加 `odps-sdk-simplexml` 依赖
2. 无需修改任何 import 语句，包路径保持不变
3. 重新编译项目

## 许可证

本模块遵循 ODPS SDK 的许可证。

## 贡献

如有问题或建议，请通过 ODPS SDK 的 issue 跟踪器反馈。