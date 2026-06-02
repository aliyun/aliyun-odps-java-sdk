# Documentation Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure MaxCompute Java SDK docs into a task-driven architecture with llms.txt support, replacing the current confusing core-concept/api-reference split.

**Architecture:** Three-phase approach: (1) scaffold directory structure, (2) massive parallel content creation via subagents, (3) build tooling and cleanup. Phase 2 dispatches 10+ independent subagents simultaneously since doc files have no interdependencies.

**Tech Stack:** Docusaurus, Markdown, Node.js (llms.txt generator script)

**Spec:** `docs/docs/superpowers/specs/2026-06-02-docs-modernization-design.md`

---

## Phase 1: Scaffold (Sequential)

### Task 1: Create directory structure and category files

**Files:**
- Create: All `_category_.json` files in new directories
- Modify: `docs/docusaurus.config.js` (remove broken link ignore)

- [ ] **Step 1: Create new directory structure**

```bash
cd docs/docs
mkdir -p getting-started guides/read-data guides/write-data guides/execute-sql guides/manage-tables guides/manage-resources guides/security modules reference
```

- [ ] **Step 2: Create _category_.json files**

Create the following files with appropriate sidebar labels and positions:

`getting-started/_category_.json`:
```json
{"label": "快速上手", "position": 2}
```

`guides/_category_.json`:
```json
{"label": "使用指南", "position": 3}
```

`guides/read-data/_category_.json`:
```json
{"label": "读数据", "position": 1}
```

`guides/write-data/_category_.json`:
```json
{"label": "写数据", "position": 2}
```

`guides/execute-sql/_category_.json`:
```json
{"label": "执行 SQL", "position": 3}
```

`guides/manage-tables/_category_.json`:
```json
{"label": "管理表", "position": 4}
```

`guides/manage-resources/_category_.json`:
```json
{"label": "管理资源", "position": 5}
```

`guides/security/_category_.json`:
```json
{"label": "权限管理", "position": 6}
```

`modules/_category_.json`:
```json
{"label": "模块概览", "position": 4}
```

`reference/_category_.json`:
```json
{"label": "API 参考", "position": 5}
```

- [ ] **Step 3: Update root _category_.json**

Update `docs/docs/_category_.json` to set position 1 for the index page.

- [ ] **Step 4: Commit scaffold**

```bash
git add docs/docs/getting-started docs/docs/guides docs/docs/modules docs/docs/reference
git commit -m "docs: scaffold new documentation directory structure"
```

---

## Phase 2: Content Creation (Massively Parallel)

All tasks in Phase 2 are **independent** and should be dispatched as parallel subagents. Each subagent receives:
1. The source file(s) to migrate from (current docs)
2. The target file(s) to create
3. The content template from the spec
4. Context about the SDK's APIs (from existing docs + source code)

### Task 2: Write `index.md` + `faq.md` (Agent: index)

**Files:**
- Create: `docs/docs/index.md`
- Create: `docs/docs/faq.md`
- Source: `docs/docs/intro.md`, `docs/docs/question.md`

- [ ] **Step 1: Write index.md**

Rewrite `intro.md` as the new landing page. Must include:
- One-paragraph SDK introduction
- Latest version number (check Maven Central or pom.xml)
- Quick install snippet (Maven + Gradle)
- Navigation to getting-started, guides, modules
- Use frontmatter: `title`, `description`, `sidebar_position: 1`

- [ ] **Step 2: Write faq.md**

Migrate `question.md` → `faq.md`, restructure as proper FAQ with clear Q&A format.

- [ ] **Step 3: Commit**

---

### Task 3: Write `getting-started/` (Agent: getting-started)

**Files:**
- Create: `docs/docs/getting-started/installation.md`
- Create: `docs/docs/getting-started/authentication.md`
- Create: `docs/docs/getting-started/first-program.md`
- Source: `docs/docs/intro.md`, `docs/docs/quick-start.md`, `docs/docs/core-concept/init-odps-client.md`

- [ ] **Step 1: Write installation.md**

Extract and enhance installation info. Cover:
- Maven dependency (with actual latest version from pom.xml)
- Gradle dependency
- BOM usage
- Repository configuration (Maven Central + Aliyun mirror)
- Module artifacts table (core, tunnel, storage-api, udf)

- [ ] **Step 2: Write authentication.md**

Restructure `init-odps-client.md`. Must cover all 5 auth methods:
- AccessKey (most common)
- STS Token (temporary)
- CredentialProvider (recommended for production)
- Dual Signature (app + user)
- Bearer Token (short-lived)

Add: best practice recommendations, when to use which method.

- [ ] **Step 3: Write first-program.md**

Rewrite `quick-start.md` as a minimal working example:
- Complete runnable code (with imports)
- Step-by-step explanation
- Expected output
- "What's next" links to guides

- [ ] **Step 4: Commit**

---

### Task 4: Write `guides/read-data/` (Agent: read-data)

**Files:**
- Create: `docs/docs/guides/read-data/index.md`
- Create: `docs/docs/guides/read-data/preview.md`
- Create: `docs/docs/guides/read-data/tunnel-download.md`
- Create: `docs/docs/guides/read-data/storage-api-read.md`
- Create: `docs/docs/guides/read-data/blob.md`
- Source: `docs/docs/core-concept/table-read.md`, `docs/docs/api-reference/tunnel/DownloadSession.md`, `docs/docs/api-reference/storage-api/read.md`, `docs/docs/api-reference/storage-api/blob.md`

- [ ] **Step 1: Write index.md (选型指南)**

Comparison table:

| 方式 | 适用场景 | 数据量 | 性能 | 复杂度 |
|------|----------|--------|------|--------|
| Table.read() | 预览少量数据 | <1万行 | 低 | 最简单 |
| Tunnel Download | 批量导出 | 不限 | 中 | 中等 |
| Storage API | 高性能并行读取 | 不限 | 最高 | 较复杂 |

Include decision tree helping users choose.

- [ ] **Step 2: Write preview.md**

Migrate from `table-read.md`. Follow guide template:
- Prerequisites, full example, config options (limit/partition/columns/timezone), type mapping table, caveats

- [ ] **Step 3: Write tunnel-download.md**

Restructure from `DownloadSession.md`. Task-oriented:
- Create session → open reader → iterate records → close
- Full working example
- Configuration (compression, concurrency, retry)
- Parallel download with multiple threads

- [ ] **Step 4: Write storage-api-read.md**

Migrate from `api-reference/storage-api/read.md`:
- Builder pattern session creation
- Split-based parallel reading
- Arrow format handling
- Column pruning, partition filtering, predicate pushdown

- [ ] **Step 5: Write blob.md**

Migrate from `api-reference/storage-api/blob.md`:
- Blob data concept
- Single and batch download
- Working examples

- [ ] **Step 6: Commit**

---

### Task 5: Write `guides/write-data/` (Agent: write-data)

**Files:**
- Create: `docs/docs/guides/write-data/index.md`
- Create: `docs/docs/guides/write-data/tunnel-upload.md`
- Create: `docs/docs/guides/write-data/tunnel-stream.md`
- Create: `docs/docs/guides/write-data/tunnel-upsert.md`
- Create: `docs/docs/guides/write-data/storage-api-write.md`
- Source: `docs/docs/api-reference/tunnel/UploadSession.md`, `docs/docs/api-reference/tunnel/StreamUploadSession.md`, `docs/docs/api-reference/tunnel/UpsertSession.md`, `docs/docs/api-reference/storage-api/write.md`

- [ ] **Step 1: Write index.md (选型指南)**

Comparison table:

| 方式 | 适用场景 | 可见性 | 事务性 | 性能 |
|------|----------|--------|--------|------|
| Tunnel Upload | 批量导入 | 提交后可见 | 原子提交 | 中 |
| Tunnel Stream | 实时写入 | flush 后可见 | 非事务 | 高 |
| Tunnel Upsert | 更新/插入 Delta Table | flush 后可见 | 行级 | 高 |
| Storage API Batch | 批量写入(Arrow) | 提交后可见 | 原子提交 | 最高 |
| Storage API Stream | 流式写入(Arrow) | flush 后可见 | 非事务 | 最高 |

- [ ] **Step 2: Write tunnel-upload.md**

Task-oriented guide: create session → create writer → write records → commit
- Full example with imports
- Block management
- Error handling and retry

- [ ] **Step 3: Write tunnel-stream.md**

StreamUploadSession guide:
- Difference from batch upload
- flush semantics, slot management
- Schema mismatch handling (since 0.50.0)
- Dynamic partition writing (since 0.55.0)

- [ ] **Step 4: Write tunnel-upsert.md**

UpsertSession guide:
- Delta Table prerequisite
- Upsert stream lifecycle
- Flush listener pattern
- Primary key bucketing

- [ ] **Step 5: Write storage-api-write.md**

From `api-reference/storage-api/write.md`:
- Batch mode (with commit)
- Streaming mode (flush visible)
- Arrow VectorSchemaRoot population

- [ ] **Step 6: Commit**

---

### Task 6: Write `guides/execute-sql/` (Agent: execute-sql)

**Files:**
- Create: `docs/docs/guides/execute-sql/index.md`
- Create: `docs/docs/guides/execute-sql/offline.md`
- Create: `docs/docs/guides/execute-sql/mcqa.md`
- Create: `docs/docs/guides/execute-sql/sql-executor.md`
- Source: `docs/docs/core-concept/execute-sql/index.mdx`, `docs/docs/core-concept/execute-sql/offline.md`, `docs/docs/core-concept/execute-sql/mcqav1.md`, `docs/docs/core-concept/execute-sql/mcqav2.md`, `docs/docs/api-reference/SQLExecutor.md`

- [ ] **Step 1: Write index.md**

Overview of SQL execution modes:
- Offline (SQLTask): large jobs, batch processing
- MCQA v1 (Interactive): low-latency queries with session
- MCQA v2 (Interactive V2): improved interactive mode
- Decision guidance on when to use which

- [ ] **Step 2: Write offline.md**

Migrate from `core-concept/execute-sql/offline.md`:
- SQLTask.run() basic usage
- Getting results (getTaskResult vs InstanceTunnel)
- Logview/JobInsight URL
- Priority and hints configuration

- [ ] **Step 3: Write mcqa.md**

Merge `mcqav1.md` + `mcqav2.md`:
- MCQA concept (MaxCompute Query Acceleration)
- v1 mode (session-based)
- v2 mode (per-query, simpler)
- Fallback policy configuration
- Getting results

- [ ] **Step 4: Write sql-executor.md**

Advanced SQLExecutor guide from `api-reference/SQLExecutor.md`:
- Builder configuration
- Run with hints
- Result iteration (RecordSet)
- Execution log, query ID
- Cancel and lifecycle

- [ ] **Step 5: Commit**

---

### Task 7: Write `guides/manage-tables/` (Agent: manage-tables)

**Files:**
- Create: `docs/docs/guides/manage-tables/index.md`
- Create: `docs/docs/guides/manage-tables/create-table.md`
- Create: `docs/docs/guides/manage-tables/alter-table.md`
- Create: `docs/docs/guides/manage-tables/partitions.md`
- Create: `docs/docs/guides/manage-tables/tags.md`
- Source: `docs/docs/core-concept/create-table.md`, `docs/docs/api-reference/Table.md`

- [ ] **Step 1: Write index.md**

Table management overview:
- Table lifecycle (create → alter → drop)
- Table types (managed, external, view, materialized view)
- Getting a Table instance (lazy load concept)

- [ ] **Step 2: Write create-table.md**

From `core-concept/create-table.md`:
- TableCreator builder pattern
- Column definition
- Partition columns
- Primary keys, clustering
- Table formats (APPEND, TRANSACTION, DELTA)
- Auto-partition (GenerateExpression)

- [ ] **Step 3: Write alter-table.md**

Extract from `api-reference/Table.md` §表更新操作:
- rename, setLifeCycle, changeOwner, changeComment
- addColumns, dropColumns, alterColumnType, changeColumnName
- changeClusterInfo, touch

- [ ] **Step 4: Write partitions.md**

Extract from `api-reference/Table.md` §分区操作:
- getPartition, getPartitions, getPartitionSpecs
- createPartition, deletePartition
- hasPartition
- PartitionSpec construction

- [ ] **Step 5: Write tags.md**

Extract from `api-reference/Table.md` §标签操作:
- Tag vs SimpleTag concept
- Table-level and column-level tagging
- CRUD operations for both types

- [ ] **Step 6: Commit**

---

### Task 8: Write `guides/manage-resources/` + `guides/security/` (Agent: resources-security)

**Files:**
- Create: `docs/docs/guides/manage-resources/functions.md`
- Create: `docs/docs/guides/manage-resources/resources.md`
- Create: `docs/docs/guides/security/check-permission.md`
- Create: `docs/docs/guides/security/acl-query.md`
- Source: `docs/docs/core-concept/functions.md`, `docs/docs/api-reference/Functions.md`, `docs/docs/api-reference/Resources.md`, `docs/docs/core-concept/permission/check_permission.md`, `docs/docs/core-concept/permission/run_acl_query.md`

- [ ] **Step 1: Write functions.md**

Merge from `core-concept/functions.md` + `api-reference/Functions.md`:
- List/get/create/delete functions
- Function types (UDF, UDTF, UDAF)
- Resource dependencies

- [ ] **Step 2: Write resources.md**

From `api-reference/Resources.md`:
- Resource types (JAR, FILE, TABLE, ARCHIVE)
- Upload/download/list/delete

- [ ] **Step 3: Write check-permission.md**

From `core-concept/permission/check_permission.md`:
- Permission check API usage
- Action types
- Response interpretation

- [ ] **Step 4: Write acl-query.md**

From `core-concept/permission/run_acl_query.md`:
- ACL query execution
- Policy-based access control

- [ ] **Step 5: Commit**

---

### Task 9: Write `modules/` (Agent: modules)

**Files:**
- Create: `docs/docs/modules/core.md`
- Create: `docs/docs/modules/tunnel.md`
- Create: `docs/docs/modules/storage-api.md`
- Create: `docs/docs/modules/udf.md`
- Source: `docs/docs/core-concept/tabletunnel.md`, `docs/docs/api-reference/tunnel/Configuration.md`, `docs/docs/api-reference/tunnel/RetryLogic.md`, `docs/docs/api-reference/storage-api/overview.md`

- [ ] **Step 1: Write core.md**

`odps-sdk-core` module overview:
- Maven coordinates
- Key classes: Odps, Table, Instance, Project, Schema, Function, Resource
- Entry point pattern (Account → Odps → resources)
- Configuration: endpoint, default project, RestClient settings

- [ ] **Step 2: Write tunnel.md**

`odps-sdk-tunnel` module overview:
- Maven coordinates
- Absorb content from `Configuration.md` and `RetryLogic.md`
- Configuration builder pattern (endpoint, compression, tags, quotaName)
- Retry strategy (TunnelRetryHandler)
- Compression options (LZ4, ZSTD, ZLIB)
- Relationship to core module

- [ ] **Step 3: Write storage-api.md**

Migrate `api-reference/storage-api/overview.md`:
- Maven coordinates
- Architecture diagram (mermaid)
- Core features table
- Thread safety notes
- Resource management (AutoCloseable)
- Quick start examples (read + write)

- [ ] **Step 4: Write udf.md**

`odps-sdk-udf` module overview:
- Maven coordinates
- UDF development workflow
- VectorizedExtractor/Outputer (batch processing)
- InputSplitter
- Relationship to other modules

- [ ] **Step 5: Commit**

---

### Task 10: Write P1 `reference/` pages (Agent: reference-core)

**Files:**
- Create: `docs/docs/reference/Odps.md`
- Create: `docs/docs/reference/Table.md`
- Create: `docs/docs/reference/TableTunnel.md`
- Create: `docs/docs/reference/SQLExecutor.md`
- Create: `docs/docs/reference/MaxStorageClient.md`
- Create: `docs/docs/reference/DownloadSession.md`
- Create: `docs/docs/reference/UploadSession.md`
- Create: `docs/docs/reference/StreamUploadSession.md`
- Source: existing `api-reference/` files + Java source code

- [ ] **Step 1: Write Odps.md**

Pure API reference for `com.aliyun.odps.Odps`:
- Constructor
- Key methods: tables(), instances(), projects(), schemas(), functions(), resources()
- Configuration: setEndpoint, setDefaultProject, setTunnelEndpoint
- options() configuration

- [ ] **Step 2: Write Table.md**

Streamline existing `api-reference/Table.md`:
- Keep all API signatures and parameter descriptions
- Remove tutorial-style explanations (those live in guides now)
- Organize: metadata getters → data operations → DDL → partitions → tags → extended info

- [ ] **Step 3: Write TableTunnel.md**

From `api-reference/tunnel/TableTunnel.md`:
- Constructor and configuration
- createUploadSession / createDownloadSession / createStreamUploadSession / createUpsertSession
- preview method
- Method signatures with parameters

- [ ] **Step 4: Write SQLExecutor.md**

Migrate existing, keep as pure reference:
- SQLExecutorBuilder methods
- run(), getResult(), cancel()
- ExecuteMode enum
- FallbackPolicy

- [ ] **Step 5: Write MaxStorageClient.md**

From `api-reference/storage-api/client.md`:
- Builder pattern
- createTableReadSessionBuilder / createTableWriteSessionBuilder
- Thread safety notes

- [ ] **Step 6: Write DownloadSession.md**

From existing tunnel doc:
- Session creation parameters
- openRecordReader variants
- openBufferedRecordReader, openBufferedArrowRecordReader
- getSchema, getRecordCount

- [ ] **Step 7: Write UploadSession.md**

From existing:
- Session creation
- openRecordWriter
- getBlockList, commit
- Block management

- [ ] **Step 8: Write StreamUploadSession.md**

From existing:
- Builder pattern
- openRecordWriter, flush
- Schema version, allowSchemaMismatch
- Dynamic partition support

- [ ] **Step 9: Commit**

---

### Task 11: Write P2 `reference/` pages (Agent: reference-secondary)

**Files:**
- Create: `docs/docs/reference/Instance.md`
- Create: `docs/docs/reference/UpsertSession.md`
- Create: `docs/docs/reference/TableReadSession.md`
- Create: `docs/docs/reference/TableWriteSession.md`
- Create: `docs/docs/reference/Functions.md`
- Create: `docs/docs/reference/Resources.md`
- Create: `docs/docs/reference/Project.md`
- Create: `docs/docs/reference/Schemas.md`
- Create: `docs/docs/reference/Partition.md`
- Source: existing `api-reference/` files

- [ ] **Step 1: Write Instance.md**

From `api-reference/Instances.md`:
- Instance lifecycle (create → running → terminated)
- waitForSuccess, getTaskResult, getTaskSummary
- stop, cancel
- ResultDescriptor

- [ ] **Step 2: Write UpsertSession.md, TableReadSession.md, TableWriteSession.md**

From existing tunnel/storage-api docs — pure API reference format.

- [ ] **Step 3: Write Functions.md, Resources.md**

Migrate from existing api-reference (minimal restructure needed).

- [ ] **Step 4: Write Project.md, Schemas.md, Partition.md**

Migrate from existing api-reference (minimal restructure needed).

- [ ] **Step 5: Commit**

---

## Phase 3: Tooling and Cleanup (Sequential)

### Task 12: Create llms.txt generation script

**Files:**
- Create: `docs/scripts/generate-llms-txt.js`
- Modify: `docs/package.json`

- [ ] **Step 1: Write generate-llms-txt.js**

```javascript
#!/usr/bin/env node
/**
 * Generates llms.txt and llms-full.txt from docs/docs/ markdown files.
 * Run: node scripts/generate-llms-txt.js
 * Output: docs/static/llms.txt, docs/static/llms-full.txt
 */

const fs = require('fs');
const path = require('path');
const matter = require('gray-matter');

const DOCS_DIR = path.join(__dirname, '..', 'docs');
const STATIC_DIR = path.join(__dirname, '..', 'static');

// Define the llms.txt structure (sections → files)
const STRUCTURE = {
  'Getting Started': [
    'getting-started/installation.md',
    'getting-started/authentication.md',
    'getting-started/first-program.md',
  ],
  'Guides': [
    'guides/read-data/index.md',
    'guides/write-data/index.md',
    'guides/execute-sql/index.md',
    'guides/manage-tables/index.md',
    'guides/manage-resources/functions.md',
    'guides/security/check-permission.md',
  ],
  'Modules': [
    'modules/core.md',
    'modules/tunnel.md',
    'modules/storage-api.md',
    'modules/udf.md',
  ],
  'API Reference': [
    'reference/Odps.md',
    'reference/Table.md',
    'reference/TableTunnel.md',
    'reference/SQLExecutor.md',
    'reference/MaxStorageClient.md',
  ],
  'Optional': [
    'changelog.md',
    'faq.md',
    'operations/types.md',
  ],
};

// Generate llms.txt (index)
function generateIndex() {
  let output = `# MaxCompute Java SDK\n\n`;
  output += `> MaxCompute(ODPS) SDK for Java - 阿里云大数据计算服务的 Java 客户端库。\n`;
  output += `> 提供表管理、SQL 执行、数据批量传输（Tunnel）、高性能读写（Storage API）等能力。\n\n`;

  for (const [section, files] of Object.entries(STRUCTURE)) {
    output += `## ${section}\n\n`;
    for (const file of files) {
      const filePath = path.join(DOCS_DIR, file);
      if (fs.existsSync(filePath)) {
        const content = fs.readFileSync(filePath, 'utf-8');
        const { data } = matter(content);
        const title = data.title || path.basename(file, '.md');
        const desc = data.description || '';
        output += `- [${title}](${file})${desc ? ': ' + desc : ''}\n`;
      }
    }
    output += '\n';
  }
  return output;
}

// Generate llms-full.txt (all content concatenated)
function generateFull() {
  let output = `# MaxCompute Java SDK - Complete Documentation\n\n`;
  const allFiles = getAllMarkdownFiles(DOCS_DIR);

  for (const file of allFiles) {
    const relativePath = path.relative(DOCS_DIR, file);
    // Skip superpowers/ internal docs
    if (relativePath.startsWith('superpowers')) continue;
    const content = fs.readFileSync(file, 'utf-8');
    const { content: body } = matter(content);
    output += `---\n# ${relativePath}\n\n${body.trim()}\n\n`;
  }
  return output;
}

function getAllMarkdownFiles(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...getAllMarkdownFiles(fullPath));
    } else if (entry.name.match(/\.(md|mdx)$/)) {
      results.push(fullPath);
    }
  }
  return results.sort();
}

// Main
fs.mkdirSync(STATIC_DIR, { recursive: true });
fs.writeFileSync(path.join(STATIC_DIR, 'llms.txt'), generateIndex());
fs.writeFileSync(path.join(STATIC_DIR, 'llms-full.txt'), generateFull());
console.log('Generated llms.txt and llms-full.txt');
```

- [ ] **Step 2: Update package.json**

Add to scripts:
```json
"generate-llms": "node scripts/generate-llms-txt.js",
"build": "npm run generate-llms && docusaurus build"
```

Add dev dependency: `gray-matter` (for frontmatter parsing)

- [ ] **Step 3: Test script**

```bash
cd docs && npm install gray-matter && node scripts/generate-llms-txt.js
cat static/llms.txt | head -30
wc -l static/llms-full.txt
```

- [ ] **Step 4: Commit**

```bash
git add docs/scripts docs/static/llms.txt docs/static/llms-full.txt docs/package.json
git commit -m "feat(docs): add llms.txt generation script"
```

---

### Task 13: Remove old documentation files

**Files:**
- Delete: All files under `docs/docs/core-concept/`
- Delete: All files under `docs/docs/api-reference/`
- Delete: `docs/docs/intro.md`
- Delete: `docs/docs/quick-start.md`
- Delete: `docs/docs/question.md`

- [ ] **Step 1: Verify new docs are complete**

Check that all source content has been migrated by verifying every file in the migration mapping table has its target created.

- [ ] **Step 2: Remove old directories**

```bash
rm -rf docs/docs/core-concept docs/docs/api-reference
rm docs/docs/intro.md docs/docs/quick-start.md docs/docs/question.md
```

- [ ] **Step 3: Test build**

```bash
cd docs && npx docusaurus build
```

Fix any broken links if build fails.

- [ ] **Step 4: Commit**

```bash
git add -A docs/docs
git commit -m "docs: remove old documentation structure (migrated to new layout)"
```

---

### Task 14: Final validation

- [ ] **Step 1: Run Docusaurus build**

```bash
cd docs && npx docusaurus build
```

Ensure zero errors.

- [ ] **Step 2: Regenerate llms.txt**

```bash
node docs/scripts/generate-llms-txt.js
```

Verify output covers all new pages.

- [ ] **Step 3: Spot-check content**

Verify key pages render correctly:
- `getting-started/authentication.md` has all 5 auth methods
- `guides/read-data/index.md` has comparison table
- `reference/Table.md` has complete API listing
- `llms.txt` links are all valid

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "docs: complete documentation modernization"
```

---

## Parallel Execution Map

```
Phase 1 (sequential):
  Task 1: Scaffold directories
     │
     ▼
Phase 2 (all parallel):
  ┌──────────────────────────────────────────────────────────────┐
  │  Task 2:  index + faq                                        │
  │  Task 3:  getting-started/ (3 pages)                         │
  │  Task 4:  guides/read-data/ (5 pages)                        │
  │  Task 5:  guides/write-data/ (5 pages)                       │
  │  Task 6:  guides/execute-sql/ (4 pages)                      │
  │  Task 7:  guides/manage-tables/ (5 pages)                    │
  │  Task 8:  guides/manage-resources/ + security/ (4 pages)     │
  │  Task 9:  modules/ (4 pages)                                 │
  │  Task 10: reference/ P1 (8 pages)                            │
  │  Task 11: reference/ P2 (9 pages)                            │
  └──────────────────────────────────────────────────────────────┘
     │
     ▼
Phase 3 (sequential):
  Task 12: llms.txt script
  Task 13: Remove old docs
  Task 14: Final validation
```

**Total: ~47 new documentation pages created across 10 parallel subagents.**

---

## Subagent Dispatch Instructions

When executing Phase 2, dispatch all 10 subagents in a single message. Each subagent should receive:

1. **The spec** (for templates and metadata requirements): `docs/docs/superpowers/specs/2026-06-02-docs-modernization-design.md`
2. **Source files** specific to their task (listed in each task's "Source" field)
3. **Target files** to create (listed in each task's "Files" field)
4. **Instruction**: Follow the guide template from the spec, use proper frontmatter with all fields (title, description, module, task, apis, since, keywords), write in Chinese (matching existing docs), include complete runnable code examples.

Each subagent works in the same repo (not isolated worktrees) since they create non-overlapping files. No merge conflicts possible.
