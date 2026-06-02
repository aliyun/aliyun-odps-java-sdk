# MaxCompute Java SDK 文档现代化设计

## 概述

对 MaxCompute Java SDK 文档进行全面重构，实现：
1. **任务驱动结构** — 按"我要做什么"组织文档，取代当前混乱的 core-concept/api-reference 二分法
2. **AI Native (llms.txt)** — 遵循 llms.txt 标准，让 LLM 工具可以直接发现和消费文档
3. **多模块聚合** — 将 core、tunnel、storage-api、udf 四个模块统一为一个文档入口
4. **同时服务人类和 AI** — 保留 Docusaurus 站点，叠加 llms.txt 生成层

## 目录结构

```
docs/docs/
├── index.md                          # SDK 总览入口
├── getting-started/                  # 快速上手（黄金路径）
│   ├── _category_.json
│   ├── installation.md               # Maven/Gradle 安装
│   ├── authentication.md             # 认证方式大全
│   └── first-program.md              # 第一个程序
│
├── guides/                           # 任务驱动指南（核心）
│   ├── _category_.json
│   ├── read-data/                    # "我要读数据"
│   │   ├── _category_.json
│   │   ├── index.md                  # 三种方式对比 + 选型指南
│   │   ├── preview.md                # Table.read() 预览（少量数据）
│   │   ├── tunnel-download.md        # Tunnel 批量下载
│   │   └── storage-api-read.md       # Storage API 高性能读取
│   │
│   ├── write-data/                   # "我要写数据"
│   │   ├── _category_.json
│   │   ├── index.md                  # 写入方式选型指南
│   │   ├── tunnel-upload.md          # Tunnel 批量上传
│   │   ├── tunnel-stream.md          # Tunnel 流式写入
│   │   ├── tunnel-upsert.md          # Tunnel Upsert（Delta Table）
│   │   └── storage-api-write.md      # Storage API 写入
│   │
│   ├── execute-sql/                  # "我要执行 SQL"
│   │   ├── _category_.json
│   │   ├── index.md                  # SQL 执行方式概览
│   │   ├── offline.md                # 离线作业
│   │   ├── mcqa.md                   # MCQA 交互式查询
│   │   └── sql-executor.md           # SQLExecutor 高级用法
│   │
│   ├── manage-tables/                # "我要管理表"
│   │   ├── _category_.json
│   │   ├── index.md
│   │   ├── create-table.md           # 建表
│   │   ├── alter-table.md            # 改表（重命名、改列、改生命周期等）
│   │   ├── partitions.md             # 分区管理
│   │   └── tags.md                   # 标签管理
│   │
│   ├── manage-resources/             # "我要管理资源/函数"
│   │   ├── _category_.json
│   │   ├── functions.md
│   │   └── resources.md
│   │
│   └── security/                     # "我要管理权限"
│       ├── _category_.json
│       ├── check-permission.md
│       └── acl-query.md
│
├── modules/                          # 模块级概览
│   ├── _category_.json
│   ├── core.md                       # odps-sdk-core
│   ├── tunnel.md                     # odps-sdk-tunnel（含配置、重试策略）
│   ├── storage-api.md                # odps-sdk-storage-api
│   └── udf.md                        # odps-sdk-udf
│
├── reference/                        # API 参考
│   ├── _category_.json
│   ├── Odps.md
│   ├── Table.md
│   ├── Instance.md
│   ├── SQLExecutor.md
│   ├── TableTunnel.md
│   ├── DownloadSession.md
│   ├── UploadSession.md
│   ├── StreamUploadSession.md
│   ├── UpsertSession.md
│   ├── MaxStorageClient.md
│   ├── TableReadSession.md
│   ├── TableWriteSession.md
│   ├── Functions.md
│   ├── Resources.md
│   ├── Project.md
│   ├── Schemas.md
│   └── Partition.md
│
├── operations/                       # 运维相关
│   ├── _category_.json
│   ├── tunnel-tags.md
│   └── types.md
│
├── changelog.md
└── faq.md
```

## llms.txt 设计

### 规范

遵循 [llms.txt 提案](https://llmstxt.org/)，生成两个文件：

### `/llms.txt` — 索引文件

作用：让 LLM 快速了解 SDK 的能力范围和文档结构，按需加载具体内容。

格式：
```markdown
# MaxCompute Java SDK

> MaxCompute(ODPS) SDK for Java - 阿里云大数据计算服务的 Java 客户端库。
> 提供表管理、SQL 执行、数据批量传输（Tunnel）、高性能读写（Storage API）等能力。

## Getting Started

- [安装](getting-started/installation.md): Maven/Gradle 依赖配置，最新版本号
- [认证](getting-started/authentication.md): AccessKey、STS Token、CredentialProvider、Bearer Token、双重签名
- [第一个程序](getting-started/first-program.md): 初始化客户端，读取表数据

## Guides

- [读数据](guides/read-data/index.md): Table.read() 预览、Tunnel 下载、Storage API 读取
- [写数据](guides/write-data/index.md): Tunnel 上传/流式/Upsert、Storage API 批量/流式写入
- [执行 SQL](guides/execute-sql/index.md): 离线作业、MCQA 交互式查询、SQLExecutor
- [管理表](guides/manage-tables/index.md): 建表、改表、分区、标签
- [管理资源](guides/manage-resources/functions.md): 函数和资源管理
- [权限管理](guides/security/check-permission.md): 权限校验和 ACL 查询

## Modules

- [odps-sdk-core](modules/core.md): 核心模块 - Odps/Table/Instance/Project/Schema 等对象管理
- [odps-sdk-tunnel](modules/tunnel.md): 数据传输通道 - 批量上传下载/流式写入/Upsert
- [odps-sdk-storage-api](modules/storage-api.md): 高性能读写 - 基于 Apache Arrow 的列式数据传输
- [odps-sdk-udf](modules/udf.md): UDF 开发 - 自定义函数开发框架

## API Reference

- [Odps](reference/Odps.md): SDK 入口类，管理所有资源
- [Table](reference/Table.md): 表对象，元数据查询和 DDL 操作
- [TableTunnel](reference/TableTunnel.md): Tunnel 入口，创建上传/下载 Session
- [SQLExecutor](reference/SQLExecutor.md): SQL 执行器，支持离线和 MCQA
- [MaxStorageClient](reference/MaxStorageClient.md): Storage API 客户端

## Optional

- [更新日志](changelog.md): 版本发布历史
- [常见问题](faq.md): FAQ
- [类型映射](operations/types.md): MaxCompute 类型与 Java 类型映射表
```

### `/llms-full.txt` — 全量聚合文件

作用：提供完整文档内容，适合一次性塞入 LLM 上下文窗口。

格式：所有 `.md` 文件按结构顺序拼接，每个文件前加分隔标记：
```markdown
# MaxCompute Java SDK - Complete Documentation

---
# getting-started/installation.md

[文件完整内容]

---
# getting-started/authentication.md

[文件完整内容]

---
...
```

### 生成方式

新建 `scripts/generate-llms-txt.js`：
- 读取所有 `docs/docs/**/*.md` 文件
- 解析 frontmatter 获取 title 和 description
- 按预定义结构生成 `llms.txt` 索引
- 将所有文件内容按顺序拼接生成 `llms-full.txt`
- 输出到 `docs/static/` 目录

在 `package.json` 中增加 script：
```json
{
  "scripts": {
    "generate-llms": "node scripts/generate-llms-txt.js",
    "build": "npm run generate-llms && docusaurus build"
  }
}
```

## Frontmatter 元数据规范

每篇文档使用统一的 YAML frontmatter：

```yaml
---
title: 文档标题（必填）
description: 一句话描述文档内容，用于 llms.txt 索引（必填）
sidebar_position: 1

# AI-friendly metadata
module: odps-sdk-tunnel               # 所属 Maven artifact
task: read-data                       # 所属任务类别（对应 guides/ 分类）
apis:                                 # 关联的核心 Java 类
  - TableTunnel
  - DownloadSession
prerequisites:                        # 前置知识
  - getting-started/authentication
since: "0.47.0"                       # 最低 SDK 版本要求
keywords:                             # 额外检索关键词
  - download
  - batch read
---
```

字段说明：

| 字段 | 必填 | 说明 |
|------|------|------|
| title | Y | 文档标题 |
| description | Y | 一行摘要，用于 llms.txt 和 SEO |
| sidebar_position | N | Docusaurus sidebar 排序 |
| module | N | 所属 Maven 模块 |
| task | N | 关联的 guides 任务分类 |
| apis | N | 涉及的 Java 类列表 |
| prerequisites | N | 建议先阅读的文档 |
| since | N | 最低版本要求 |
| keywords | N | 检索关键词 |

## 内容补全优先级

### P0 — 核心路径（首批实现）

| 文档 | 来源 | 说明 |
|------|------|------|
| getting-started/installation.md | 新写 | 从 intro.md 和 quick-start.md 提取安装部分 |
| getting-started/authentication.md | 重构 core-concept/init-odps-client.md | 补充实际最佳实践 |
| getting-started/first-program.md | 重构 quick-start.md | 精简为最小可运行示例 |
| guides/read-data/index.md | 新写 | 三种方式对比表 + 选型决策树 |
| guides/read-data/preview.md | 重构 core-concept/table-read.md | |
| guides/read-data/tunnel-download.md | 重构 api-reference/tunnel/DownloadSession.md | 从 API 文档转为任务指南 |
| guides/read-data/storage-api-read.md | 迁移 api-reference/storage-api/read.md | |
| guides/write-data/index.md | 新写 | 写入方式选型 |
| guides/write-data/tunnel-upload.md | 重构 api-reference/tunnel/UploadSession.md | |
| guides/write-data/tunnel-stream.md | 重构 api-reference/tunnel/StreamUploadSession.md | |
| guides/write-data/storage-api-write.md | 迁移 api-reference/storage-api/write.md | |
| guides/execute-sql/index.md | 重构 core-concept/execute-sql/index.mdx | |
| guides/execute-sql/offline.md | 迁移 core-concept/execute-sql/offline.md | |
| guides/execute-sql/mcqa.md | 合并 mcqav1.md + mcqav2.md | |

### P1 — 常用操作

| 文档 | 来源 |
|------|------|
| guides/manage-tables/* | 重构 core-concept/create-table.md + api-reference/Table.md |
| modules/core.md | 新写 |
| modules/tunnel.md | 从 api-reference/tunnel/Configuration.md 等提取 |
| modules/storage-api.md | 迁移 api-reference/storage-api/overview.md |
| reference/Table.md | 精简现有 api-reference/Table.md |
| reference/SQLExecutor.md | 精简现有 |

### P2 — 进阶内容

| 文档 | 来源 |
|------|------|
| guides/security/* | 迁移 core-concept/permission/* |
| guides/manage-resources/* | 迁移 api-reference/Functions.md, Resources.md |
| guides/write-data/tunnel-upsert.md | 从 api-reference/tunnel/UpsertSession.md 重构 |
| modules/udf.md | 新写 |
| 迁移指南 | 新写（从旧版本升级的注意事项） |

## 内容迁移映射

旧文件 → 新文件的映射关系：

| 旧路径 | 新路径 | 处理方式 |
|--------|--------|----------|
| intro.md | index.md | 重写 |
| quick-start.md | getting-started/first-program.md | 重构 |
| core-concept/init-odps-client.md | getting-started/authentication.md | 重构 |
| core-concept/table-read.md | guides/read-data/preview.md | 迁移 |
| core-concept/create-table.md | guides/manage-tables/create-table.md | 迁移 |
| core-concept/tabletunnel.md | modules/tunnel.md + guides | 拆分 |
| core-concept/functions.md | guides/manage-resources/functions.md | 迁移 |
| core-concept/execute-sql/* | guides/execute-sql/* | 重构 |
| core-concept/permission/* | guides/security/* | 迁移 |
| api-reference/Table.md | reference/Table.md | 精简 |
| api-reference/tunnel/* | reference/ + guides/write-data/ | 拆分 |
| api-reference/storage-api/* | modules/storage-api.md + guides/ | 拆分 |
| api-reference/Functions.md | reference/Functions.md | 迁移 |
| api-reference/SQLExecutor.md | reference/SQLExecutor.md | 迁移 |
| question.md | faq.md | 重命名 + 补充 |
| changelog.md | changelog.md | 保持 |
| operations/* | operations/* | 保持 |

## 构建工具链

### 现有工具保留
- Docusaurus（站点生成）
- Mermaid 主题（图表支持）
- 本地搜索插件

### 新增工具
1. `scripts/generate-llms-txt.js` — llms.txt 生成脚本
2. 更新 `package.json` scripts 集成 llms.txt 生成
3. `sidebars.js` 改为 autogenerated 模式（减少手动维护）

### 文档质量保证
- frontmatter 校验：CI 中检查必填字段（title, description）
- 死链检测：Docusaurus 内置 `onBrokenMarkdownLinks: 'throw'`（已启用）
- llms.txt 校验：确保索引中的文件路径都存在

## 设计原则

1. **任务导向** — 文档按"用户想做什么"组织，而非按"SDK 有什么类"组织
2. **渐进展开** — 每个 guide 的 index 提供选型指南，子页面展开具体操作
3. **跨模块关联** — 一个 guide 可以引用多个模块的内容（如"读数据"同时涉及 core/tunnel/storage-api）
4. **AI 友好** — 结构化元数据 + llms.txt 标准 + 全量聚合文件
5. **可维护** — 减少重复内容，reference 是权威来源，guide 引用 reference

## 非目标

- 不改变 Maven 模块结构
- 不引入 API 自动生成工具（当前阶段手写即可）
- 不做多语言（保持中文为主）
- 不做版本化文档（单一最新版）
