#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const DOCS_DIR = path.join(__dirname, '..', 'docs');
const STATIC_DIR = path.join(__dirname, '..', 'static');

const STRUCTURE = {
  'Getting Started': [
    'getting-started/installation.md',
    'getting-started/authentication.md',
    'getting-started/first-program.md',
  ],
  'Guides': [
    'guides/read-data/index.md',
    'guides/read-data/preview.md',
    'guides/read-data/tunnel-download.md',
    'guides/read-data/storage-api-read.md',
    'guides/read-data/blob.md',
    'guides/write-data/index.md',
    'guides/write-data/tunnel-upload.md',
    'guides/write-data/tunnel-stream.md',
    'guides/write-data/tunnel-upsert.md',
    'guides/write-data/storage-api-write.md',
    'guides/execute-sql/index.md',
    'guides/execute-sql/offline.md',
    'guides/execute-sql/mcqa.md',
    'guides/execute-sql/sql-executor.md',
    'guides/manage-tables/index.md',
    'guides/manage-tables/create-table.md',
    'guides/manage-tables/alter-table.md',
    'guides/manage-tables/partitions.md',
    'guides/manage-tables/tags.md',
    'guides/manage-resources/functions.md',
    'guides/manage-resources/resources.md',
    'guides/security/check-permission.md',
    'guides/security/acl-query.md',
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
    'reference/DownloadSession.md',
    'reference/UploadSession.md',
    'reference/StreamUploadSession.md',
    'reference/Instance.md',
    'reference/UpsertSession.md',
    'reference/TableReadSession.md',
    'reference/TableWriteSession.md',
    'reference/Functions.md',
    'reference/Resources.md',
    'reference/Project.md',
    'reference/Schemas.md',
    'reference/Partition.md',
  ],
  'Optional': [
    'changelog.md',
    'faq.md',
    'operations/types.md',
  ],
};

function parseFrontmatter(content) {
  const match = content.match(/^---\n([\s\S]*?)\n---/);
  if (!match) return {};
  const data = {};
  for (const line of match[1].split('\n')) {
    const kv = line.match(/^(\w+):\s*(.+)/);
    if (kv) data[kv[1]] = kv[2].replace(/^["']|["']$/g, '');
  }
  return data;
}

function stripFrontmatter(content) {
  return content.replace(/^---\n[\s\S]*?\n---\n*/, '');
}

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
        const data = parseFrontmatter(content);
        const title = data.title || path.basename(file, '.md');
        const desc = data.description || '';
        output += `- [${title}](${file})${desc ? ': ' + desc : ''}\n`;
      } else {
        console.warn(`WARN: ${file} not found, skipping`);
      }
    }
    output += '\n';
  }
  return output;
}

function getAllMarkdownFiles(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === 'superpowers') continue;
      results.push(...getAllMarkdownFiles(fullPath));
    } else if (entry.name.match(/\.(md|mdx)$/)) {
      results.push(fullPath);
    }
  }
  return results.sort();
}

function generateFull() {
  let output = `# MaxCompute Java SDK - Complete Documentation\n\n`;
  const allFiles = getAllMarkdownFiles(DOCS_DIR);

  for (const file of allFiles) {
    const relativePath = path.relative(DOCS_DIR, file);
    const content = fs.readFileSync(file, 'utf-8');
    const body = stripFrontmatter(content).trim();
    output += `---\n# ${relativePath}\n\n${body}\n\n`;
  }
  return output;
}

fs.mkdirSync(STATIC_DIR, { recursive: true });

const indexContent = generateIndex();
fs.writeFileSync(path.join(STATIC_DIR, 'llms.txt'), indexContent);

const fullContent = generateFull();
fs.writeFileSync(path.join(STATIC_DIR, 'llms-full.txt'), fullContent);

console.log(`Generated llms.txt (${(indexContent.length / 1024).toFixed(1)} KB)`);
console.log(`Generated llms-full.txt (${(fullContent.length / 1024).toFixed(1)} KB)`);
console.log(`Total docs: ${getAllMarkdownFiles(DOCS_DIR).length} files`);
