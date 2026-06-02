---
title: 标签操作
description: 使用 MaxCompute Java SDK 管理表和列的标签（Tag），包括标准标签和简单标签的增删查操作
sidebar_position: 5
module: odps-sdk-core
task: manage-tables
apis:
  - Table
  - Tag
since: "0.47.0"
keywords:
  - 标签
  - Tag
  - SimpleTag
  - 数据分类
  - 列标签
  - 数据治理
---

# 标签操作

MaxCompute 支持对表和列打标操作。通过标签，可以基于不同的业务场景对数据做标识（Tagging），使业务能够基于标签合理使用数据，例如数据访问控制和数据血缘追踪。

## Tag 与 SimpleTag

MaxCompute 提供两种标签机制：

| 类型 | 说明 | 特点 |
|------|------|------|
| **Tag** | 标准标签，由独立的 Tag 对象表示 | 需要预先创建 Tag 资源，支持复杂的权限控制和策略关联 |
| **SimpleTag** | 简单标签，以 category/key/value 三元组表示 | 无需预先创建，直接以键值对形式附加到表或列上，适合轻量级分类 |

## 前置条件

所有标签操作基于 `Table` 实例：

```java
Odps odps = new Odps(...);
Table table = odps.tables().get("project_name", "table_name");
```

## 标准标签 (Tag)

### 获取表级别标签

```java
List<Tag> tags = table.getTags();

for (Tag tag : tags) {
    System.out.println("Tag: " + tag.getName());
}
```

### 获取列级别标签

```java
// 获取指定列的标签
List<Tag> columnTags = table.getTags("column_name");

for (Tag tag : columnTags) {
    System.out.println("Column tag: " + tag.getName());
}
```

### 添加表级别标签

表和标签必须属于同一个 project：

```java
// 获取 Tag 对象
Tag tag = ...; // 通过 Tags API 获取

// 为表添加标签
table.addTag(tag);
```

### 添加列级别标签

为指定列添加标签：

```java
Tag tag = ...; // 通过 Tags API 获取

// 为多个列添加同一个标签
table.addTag(tag, List.of("column1", "column2"));
```

### 删除标签

```java
Tag tag = ...; // 要删除的标签对象
table.removeTag(tag);
```

## 简单标签 (SimpleTag)

SimpleTag 通过 category（类别）、key（键）、value（值）三元组来标识，无需预先创建 Tag 资源。

### 获取表级别简单标签

返回值为嵌套 Map 结构：`Map<category, Map<key, value>>`

```java
Map<String, Map<String, String>> simpleTags = table.getSimpleTags();

for (Map.Entry<String, Map<String, String>> categoryEntry : simpleTags.entrySet()) {
    String category = categoryEntry.getKey();
    for (Map.Entry<String, String> kvEntry : categoryEntry.getValue().entrySet()) {
        System.out.println(category + "/" + kvEntry.getKey() + " = " + kvEntry.getValue());
    }
}
```

### 获取列级别简单标签

```java
Map<String, Map<String, String>> columnSimpleTags = table.getSimpleTags("column_name");
```

### 添加表级别简单标签

```java
// 参数：category, key, value
table.addSimpleTag("data_classification", "sensitivity", "high");
table.addSimpleTag("business", "domain", "finance");
```

### 添加列级别简单标签

```java
// 为指定列添加简单标签
table.addSimpleTag("pii", "type", "phone_number", List.of("phone_col"));
table.addSimpleTag("pii", "type", "email", List.of("email_col", "backup_email_col"));
```

### 删除表级别简单标签

```java
// 参数：category, key, value
table.removeSimpleTag("data_classification", "sensitivity", "high");
```

### 删除列级别简单标签

```java
table.removeSimpleTag("pii", "type", "phone_number", List.of("phone_col"));
```

## 完整示例

### 为表添加数据分类标签

```java
public void classifyTable(Odps odps) throws OdpsException {
    Table table = odps.tables().get("my_project", "user_profile");

    // 添加业务域标签
    table.addSimpleTag("business", "domain", "user_center");
    table.addSimpleTag("business", "priority", "P0");

    // 添加数据安全等级标签
    table.addSimpleTag("security", "level", "confidential");

    // 查看已添加的标签
    Map<String, Map<String, String>> tags = table.getSimpleTags();
    System.out.println("Table simple tags: " + tags);
}
```

### 为敏感列打标

```java
public void tagSensitiveColumns(Odps odps) throws OdpsException {
    Table table = odps.tables().get("my_project", "customer_info");

    // 标记 PII（个人身份信息）列
    table.addSimpleTag("pii", "type", "id_card", List.of("id_number"));
    table.addSimpleTag("pii", "type", "phone", List.of("mobile", "home_phone"));
    table.addSimpleTag("pii", "type", "address", List.of("home_address"));

    // 查看特定列的标签
    Map<String, Map<String, String>> phoneTags = table.getSimpleTags("mobile");
    System.out.println("mobile column tags: " + phoneTags);
}
```

### 使用标准 Tag 进行访问控制

```java
public void applyAccessControlTag(Odps odps, Tag sensitiveTag) throws OdpsException {
    Table table = odps.tables().get("my_project", "financial_data");

    // 为表添加标准标签（用于策略关联）
    table.addTag(sensitiveTag);

    // 为特定敏感列添加标签
    table.addTag(sensitiveTag, List.of("account_balance", "transaction_amount"));

    // 验证标签已添加
    List<Tag> tableTags = table.getTags();
    System.out.println("Table tags count: " + tableTags.size());

    List<Tag> colTags = table.getTags("account_balance");
    System.out.println("Column tags count: " + colTags.size());
}
```
