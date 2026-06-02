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
│   │   ├── storage-api-read.md       # Storage API 高性能读取
│   │   └── blob.md                   # Blob 数据读写
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

格式：所有 `.md` 和 `.mdx` 文件按结构顺序拼接，每个文件前加分隔标记：
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

### 链接格式说明

`llms.txt` 中的链接使用**相对于 `docs/docs/` 目录的文件系统路径**，如 `getting-started/installation.md`。

这些路径同时可以通过 Docusaurus 站点访问（去掉 `.md` 后缀，加上 baseUrl 前缀）：
- 文件路径：`getting-started/installation.md`
- 站点 URL：`https://aliyun.github.io/aliyun-odps-java-sdk/getting-started/installation/`

LLM 工具使用文件路径从仓库直接读取内容；人类用户通过站点 URL 浏览。

### 生成方式

新建 `scripts/generate-llms-txt.js`：
- 读取所有 `docs/docs/**/*.{md,mdx}` 文件
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

### P1 — reference/ 页面优先级

| 优先级 | 文档 | 说明 |
|--------|------|------|
| P1 | reference/Odps.md | SDK 入口类 |
| P1 | reference/Table.md | 最常用对象 |
| P1 | reference/TableTunnel.md | Tunnel 入口 |
| P1 | reference/SQLExecutor.md | SQL 执行 |
| P1 | reference/MaxStorageClient.md | Storage API 入口 |
| P1 | reference/DownloadSession.md | 配合 guides/read-data |
| P1 | reference/UploadSession.md | 配合 guides/write-data |
| P1 | reference/StreamUploadSession.md | 流式写入 |
| P2 | reference/Instance.md | 进阶 |
| P2 | reference/UpsertSession.md | Delta Table 写入 |
| P2 | reference/TableReadSession.md | Storage API 读取 |
| P2 | reference/TableWriteSession.md | Storage API 写入 |
| P2 | reference/Functions.md | 函数管理 |
| P2 | reference/Resources.md | 资源管理 |
| P2 | reference/Project.md | 项目管理 |
| P2 | reference/Schemas.md | Schema 管理 |
| P2 | reference/Partition.md | 分区对象 |

### P2 — 进阶内容

| 文档 | 来源 |
|------|------|
| guides/security/* | 迁移 core-concept/permission/* |
| guides/manage-resources/* | 迁移 api-reference/Functions.md, Resources.md |
| guides/write-data/tunnel-upsert.md | 从 api-reference/tunnel/UpsertSession.md 重构 |
| guides/read-data/blob.md | 迁移 api-reference/storage-api/blob.md |
| modules/udf.md | 新写 |
| 迁移指南 | 新写（从旧版本升级的注意事项） |

## 内容迁移映射

旧文件 → 新文件的完整映射关系（所有现有文件均有明确去向）：

| 旧路径 | 新路径 | 处理方式 |
|--------|--------|----------|
| intro.md | index.md | 重写 |
| quick-start.md | getting-started/first-program.md | 重构 |
| core-concept/init-odps-client.md | getting-started/authentication.md | 重构 |
| core-concept/table-read.md | guides/read-data/preview.md | 迁移 |
| core-concept/create-table.md | guides/manage-tables/create-table.md | 迁移 |
| core-concept/tabletunnel.md | modules/tunnel.md + guides/ | 拆分 |
| core-concept/functions.md | guides/manage-resources/functions.md | 迁移 |
| core-concept/execute-sql/index.mdx | guides/execute-sql/index.md | 重构（转为 .md） |
| core-concept/execute-sql/offline.md | guides/execute-sql/offline.md | 迁移 |
| core-concept/execute-sql/mcqav1.md | guides/execute-sql/mcqa.md | 合并 |
| core-concept/execute-sql/mcqav2.md | guides/execute-sql/mcqa.md | 合并 |
| core-concept/permission/index.mdx | guides/security/check-permission.md | 重构（转为 .md） |
| core-concept/permission/check_permission.md | guides/security/check-permission.md | 合并 |
| core-concept/permission/run_acl_query.md | guides/security/acl-query.md | 迁移 |
| api-reference/Table.md | reference/Table.md | 精简 |
| api-reference/Instances.md | reference/Instance.md | 迁移（重命名为单数） |
| api-reference/Functions.md | reference/Functions.md | 迁移 |
| api-reference/Resources.md | reference/Resources.md | 迁移 |
| api-reference/Project.md | reference/Project.md | 迁移 |
| api-reference/Schemas.md | reference/Schemas.md | 迁移 |
| api-reference/Partition.md | reference/Partition.md | 迁移 |
| api-reference/SQLExecutor.md | reference/SQLExecutor.md | 迁移 |
| api-reference/tunnel/TableTunnel.md | reference/TableTunnel.md | 迁移 |
| api-reference/tunnel/Configuration.md | modules/tunnel.md | 吸收 |
| api-reference/tunnel/RetryLogic.md | modules/tunnel.md | 吸收 |
| api-reference/tunnel/DownloadSession.md | reference/DownloadSession.md + guides/read-data/tunnel-download.md | 拆分 |
| api-reference/tunnel/UploadSession.md | reference/UploadSession.md + guides/write-data/tunnel-upload.md | 拆分 |
| api-reference/tunnel/StreamUploadSession.md | reference/StreamUploadSession.md + guides/write-data/tunnel-stream.md | 拆分 |
| api-reference/tunnel/UpsertSession.md | reference/UpsertSession.md + guides/write-data/tunnel-upsert.md | 拆分 |
| api-reference/storage-api/overview.md | modules/storage-api.md | 迁移 |
| api-reference/storage-api/client.md | reference/MaxStorageClient.md | 迁移 |
| api-reference/storage-api/read.md | guides/read-data/storage-api-read.md | 迁移 |
| api-reference/storage-api/write.md | guides/write-data/storage-api-write.md | 迁移 |
| api-reference/storage-api/blob.md | guides/read-data/blob.md | 迁移（Blob 数据读写指南） |
| question.md | faq.md | 重命名 + 补充 |
| changelog.md | changelog.md | 保持 |
| operations/* | operations/* | 保持 |

注：所有 `.mdx` 文件在新结构中统一转为 `.md`，Docusaurus 同时支持两种格式。

## Guide 页面内容模板

所有 `guides/` 下的具体操作页面（非 index）统一使用以下结构：

```markdown
---
title: [操作名称]
description: [一句话描述]
module: [模块名]
task: [任务分类]
apis: [关联 API 类]
since: "[最低版本]"
---

# [操作名称]

[1-2 句简介：这是什么、什么时候用]

## 前置条件

- 已完成 [认证配置](../getting-started/authentication.md)
- 已添加 `[artifact-id]` 依赖

## 完整示例

[可直接复制运行的完整代码示例，包含 import]

## 代码说明

[拆解关键步骤，每步 2-3 句]

## 配置选项

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| ... | ... | ... | ... |

## 注意事项

- [限制、边界条件、常见错误]

## 相关文档

- [关联 guide 或 reference 页面]
```

对于 `guides/*/index.md` 选型概览页面，使用：

```markdown
---
title: [任务名称]
description: [简介]
---

# [任务名称]

[1-2 句描述这个任务类别]

## 方式对比

| 方式 | 适用场景 | 性能 | 复杂度 |
|------|----------|------|--------|
| ... | ... | ... | ... |

## 如何选择

[决策树或建议，帮助用户选择合适的方式]

## 详细指南

- [方式1](./xxx.md) — 一句话说明
- [方式2](./yyy.md) — 一句话说明
```

## 构建工具链

### 现有工具保留
- Docusaurus（站点生成）
- Mermaid 主题（图表支持）
- 本地搜索插件

### 新增工具
1. `scripts/generate-llms-txt.js` — llms.txt 生成脚本
2. 更新 `package.json` scripts 集成 llms.txt 生成
3. `sidebars.js` — 确认保持现有 autogenerated 模式，利用 `_category_.json` 控制排序

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
