---
title: Blob 数据读写
sidebar_position: 5
toc_max_heading_level: 4
---

## 概述

MaxCompute 的 `BLOB` 类型用于存储非结构化的二进制大对象数据（如图片、音频、文档等）。Blob 在表中以**引用（Reference）**的形式存储于 `BLOB` 类型列，实际的二进制数据存储在独立的对象存储后端。

读写 Blob 分两个层面：

- **读取**：先从表行数据中取出 Blob 引用，再通过 [`BlobManager`](#blobmanager-接口) 根据引用下载实际内容
- **写入**：支持两种方式——[Arrow 接口](./write.md#tablearrowwriter)（`TableArrowWriter`）和行接口（`RecordWriter`），见下文

客户端通过 [`MaxStorageClient.openBlobManager()`](./client.md#openblobmanager) 获取 `BlobManager`，通过 [`MaxStorageClient.createTableReadSessionBuilder()`](./client.md#createtablereadsessionbuilder) 和 [`createTableWriteSessionBuilder()`](./client.md#createtablewritesessionbuilder) 进入读写流程。

:::caution Blob 引用在 Arrow 列接口中的格式差异

使用 Arrow 列接口（`VarBinaryVector`）操作 Blob 时，**读取和写入的字节格式不同**（服务端会自动转换）：

| 方向 | VarBinaryVector 中的字节格式 | 如何构造 Blob |
|---|---|---|
| **读取** | 引用字符串的 UTF-8 字节 | `Blob.fromReference(new String(bytes, StandardCharsets.UTF_8))` |
| **写入** | `Base64.decode(reference)` 的原始字节 | `imageVector.setSafe(i, blob.getReferenceBytes())` |

若使用**行接口**（`RecordWriter` / `RecordReader`），则无需关心这一差异，SDK 内部会自动处理。
:::

---

## Blob 数据读取

### 获取 BlobManager

通过 `MaxStorageClient.openBlobManager()` 获取，`BlobManager` 提供单条和批量两种下载方式：

```java
BlobManager blobManager = client.openBlobManager();
```

### BlobManager 接口

#### `download` — 下载单个 Blob

```java
InputStream download(Blob blobRef)
```

**参数**：
- `blobRef`：Blob 引用对象，通过 `Blob.fromReference(String)` 创建

**返回值**：包含 Blob 原始数据的 `InputStream`，调用方**必须关闭**

**异常**：`BlobDownloadException` / `MaxStorageException` / `ClientException`

---

#### `batchDownload` — 批量下载 Blob

```java
BlobDataIterator batchDownload(List<Blob> blobs)
```

**参数**：
- `blobs`：Blob 引用列表，迭代器返回顺序与输入顺序一致

**返回值**：`BlobDataIterator`，调用方**必须关闭**

**异常**：`BlobDownloadException`（通过 `getFailedBlobRef()` 可获取失败的 Blob 引用）/ `MaxStorageException` / `ClientException`

---

### BlobDataIterator 使用规范

`BlobDataIterator` 继承自 `Iterator<InputStream>` 和 `AutoCloseable`。

:::caution
1. **必须**在 `try-with-resources` 中使用，确保底层网络连接正确关闭
2. 每个 `next()` 返回的 `InputStream` 在调用下一次 `hasNext()` 前**必须关闭**；若未关闭，迭代器会自动跳过并打印 warn 日志
3. 中途不再需要时，直接关闭迭代器即可
:::

---

### 读取示例

#### 行接口读取（RecordReader，推荐）

行接口直接返回 `Blob` 对象，无需手动处理字节转换：

```java
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withColumns(Arrays.asList("id", "image_data"))
    .build();

BlobManager blobManager = client.openBlobManager();

for (InputSplit split : session.getSplits()) {
    ArrowReader arrowReader = session.createReaderBuilder(split).build();
    RecordReader recordReader = arrowReader.getAsRecordReader();

    List<Blob> blobs = new ArrayList<>();
    recordReader.forEach(record -> {
        Blob blob = (Blob) record.get("image_data");
        if (blob != null) {
            blobs.add(blob);
        }
    });

    // 批量下载
    try (BlobDataIterator iterator = blobManager.batchDownload(blobs)) {
        while (iterator.hasNext()) {
            try (InputStream data = iterator.next()) {
                byte[] content = data.readAllBytes();
                // 处理 content...
            }
        }
    }

    arrowReader.close();
    recordReader.close();
}
```

#### Arrow 列接口读取（单条下载）

读取时，`VarBinaryVector` 中存储的是 Blob 引用字符串的 UTF-8 字节，需要先解码为字符串再构造 `Blob` 对象：

```java
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withColumns(Arrays.asList("id", "image_data"))  // image_data 为 BLOB 类型列
    .build();

BlobManager blobManager = client.openBlobManager();

for (InputSplit split : session.getSplits()) {
    try (ArrowReader reader = session.createReaderBuilder(split).build()) {
        while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            VarBinaryVector blobVector = (VarBinaryVector) root.getVector("image_data");

            for (int row = 0; row < root.getRowCount(); row++) {
                if (blobVector.isNull(row)) {
                    continue;
                }
                // VarBinaryVector 中存储的是引用字符串的 UTF-8 字节
                String reference = new String(blobVector.get(row), StandardCharsets.UTF_8);
                Blob blobRef = Blob.fromReference(reference);

                try (InputStream data = blobManager.download(blobRef)) {
                    byte[] content = data.readAllBytes();
                    System.out.println("Blob 大小: " + content.length + " bytes");
                }
            }
        }
    }
}
```

#### Arrow 列接口读取（批量下载，更高效）

```java
for (InputSplit split : session.getSplits()) {
    try (ArrowReader reader = session.createReaderBuilder(split).build()) {
        while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            VarBinaryVector blobVector = (VarBinaryVector) root.getVector("image_data");

            List<Blob> blobRefs = new ArrayList<>();
            for (int row = 0; row < root.getRowCount(); row++) {
                if (!blobVector.isNull(row)) {
                    String reference = new String(blobVector.get(row), StandardCharsets.UTF_8);
                    blobRefs.add(Blob.fromReference(reference));
                }
            }
            if (blobRefs.isEmpty()) continue;

            try (BlobDataIterator iterator = blobManager.batchDownload(blobRefs)) {
                while (iterator.hasNext()) {
                    try (InputStream data = iterator.next()) {
                        byte[] content = data.readAllBytes();
                        // 处理 content...
                    }
                }
            } catch (BlobDownloadException e) {
                System.err.println("下载失败的 Blob: " + e.getFailedBlobRef());
                throw e;
            }
        }
    }
}
```

---

## Blob 数据写入

Storage API 提供两套写入接口，均支持 Blob 写入：

| 接口 | 适用场景 |
|------|----------|
| **行接口**（`RecordWriter`） | 逐行写入，使用熟悉的 `Record` 对象，简单易用 |
| **Arrow 接口**（`TableArrowWriter`） | 批量列式写入，性能更高，需要操作 `VectorSchemaRoot` |

---

### 写入方式一：行接口（RecordWriter）

行接口通过 `TableArrowWriter.getAsRecordWriter(rowCountPerBatch)` 获取，内部自动完成 Blob 上传，用户只需将原始 Blob 数据（`Blob.fromInputStream(...)`）设置到 `Record` 中即可。

**写入流程：**

```
tableArrowWriter.getAsRecordWriter(batchSize)
    └─ RecordWriter
           ├─ newRecord()         → 创建支持 Blob 自动上传的 Record
           ├─ record.set(col, Blob.fromInputStream(stream))  → 设置原始 Blob
           ├─ write(record)       → SDK 内部自动上传 Blob，回填引用
           └─ flush()             → 发送数据
```

#### 示例

```java
try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId).build()) {
    try (ArrowWriter arrowWriter = session.createWriterBuilder("stream-1", 1).build()) {
        // 获取行接口，每累积 1000 行自动打一个 Arrow Batch
        RecordWriter recordWriter = ((TableArrowWriter) arrowWriter).getAsRecordWriter(1000);

        // 使用 newRecord() 创建的 Record 才支持 Blob 自动上传
        Record record = recordWriter.newRecord();
        for (int i = 0; i < 10000; i++) {
            record.set("id", (long) i);
            // 将原始 InputStream 包装成 Blob，SDK 会在 write() 时自动上传
            InputStream imageStream = loadImageStream(i);
            record.set("image_data", Blob.fromInputStream(imageStream));
            recordWriter.write(record);
        }
        recordWriter.flush();
    }
    session.commit();
}
```

:::info
- `newRecord()` 返回的是特殊的 `BlobUploadableRecord`，`set()` 时若检测到 `Blob.fromInputStream()`，会绑定上传器，在 `write()` 时自动触发上传并回填引用
- 对于已有引用的 Blob（`Blob.fromReference(...)`），可直接设置，无需上传
- `DeltaTableRecordWriter`（主键表）用法相同，额外支持 `delete(record)` 方法
:::

---

### 写入方式二：Arrow 接口（TableArrowWriter）

Arrow 接口直接操作 `VectorSchemaRoot`。写入时，BLOB 列（`VarBinaryVector`）中需存放 `blob.getReferenceBytes()`（即 `Base64.decode(reference)`）。有两种模式：

#### 模式 A：自动批量上传（`withBatchBlobUploadEnabled(true)`）

开启后，用户在 `VarBinaryVector` 中填入**原始二进制数据**，SDK 在 `writeBatch()` 时自动完成上传并将引用字节回填：

```
VarBinaryVector.setSafe(row, 原始图片字节)
    └─ writeBatch(root)
           ├─ 检测 BLOB 列，批量上传原始数据
           ├─ 服务端返回引用 → 回填 VarBinaryVector
           └─ 正常写入 Arrow 数据（BLOB 列已替换为引用字节）
```

```java
try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1)
        .withBatchBlobUploadEnabled(true)   // 开启自动上传模式
        .build()) {

    TableArrowWriter arrowWriter = (TableArrowWriter) writer;
    try (VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot()) {
        root.allocateNew();
        BigIntVector idVector = (BigIntVector) root.getVector("id");
        VarBinaryVector imageVector = (VarBinaryVector) root.getVector("image_data");

        for (int i = 0; i < 100; i++) {
            idVector.setSafe(i, i);
            imageVector.setSafe(i, loadImageBytes(i));  // 填入原始图片字节
        }
        root.setRowCount(100);

        writer.writeBatch(root);  // SDK 自动上传 Blob 并回填引用
    }
    writer.flush();
}
```

#### 模式 B：手动管理引用（默认模式）

不开启 `withBatchBlobUploadEnabled` 时，用户需自行管理 Blob 引用。写入时 `VarBinaryVector` 中存放的是 `blob.getReferenceBytes()`（即 `Base64.decode(reference)`），而非原始数据。

这种模式特别适合 **Blob 上传与表数据写入解耦**的架构，例如：
```
上游服务 → batchUploadBlob() 批量上传 → 拿到 Blob Ref → 发 Kafka
下游服务 → 收到 Blob Ref → 直接填引用写 MC（无需再上传）
```

##### 上游：上传 Blob 获取引用

:::caution 主键表（Delta Table）限制
`uploadBlob()` 和 `batchUploadBlob()` **不支持主键表**。原因是主键表要求每个 Blob 携带 `distributionKey`（从该行的主键列值序列化而来），而这两个方法只接收裸的 Blob 数据，无法计算分布键。

主键表写入 Blob 请使用**模式 A**（`withBatchBlobUploadEnabled(true)`），它在 `writeBatch()` 时拥有完整的 Arrow Batch 上下文，可以从 PK 列提取分布键。
:::

**方式一：批量上传（推荐，单次 HTTP 请求）**

使用 `batchUploadBlob(columnId, List<byte[]>)` 在单次请求中批量上传多个 Blob，返回与输入顺序一致的引用列表：

```java
try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
    TableArrowWriter arrowWriter = (TableArrowWriter) writer;

    // 获取目标 BLOB 列的 columnId
    long blobColumnId = arrowWriter.getWriteSchema().getColumns().stream()
        .filter(c -> c.getName().equals("image_data"))
        .findFirst().get().getColumnId();

    // 准备 Blob 数据
    List<byte[]> blobDataList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
        blobDataList.add(loadImageBytes(i));
    }

    // 单次请求批量上传，返回与输入顺序一致的引用列表
    List<Blob> blobRefs = arrowWriter.batchUploadBlob(blobColumnId, blobDataList);

    // 将引用传递给下游（如写入 Kafka），或直接写入 Arrow Batch
    for (Blob ref : blobRefs) {
        sendToKafka(ref.getReference());  // 引用是一个 Base64 字符串
    }
}
```

**方式二：逐条上传（每条一次 HTTP 请求）**

使用 `uploadBlob(columnId, InputStream)` 逐条上传单个 Blob，适合 Blob 数量较少或需要流式处理的场景：

```java
try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
    TableArrowWriter arrowWriter = (TableArrowWriter) writer;

    long blobColumnId = arrowWriter.getWriteSchema().getColumns().stream()
        .filter(c -> c.getName().equals("image_data"))
        .findFirst().get().getColumnId();

    for (int i = 0; i < 10; i++) {
        // 每次调用发起一次独立的 HTTP 请求
        Blob blobRef = arrowWriter.uploadBlob(blobColumnId, loadImageStream(i));
        sendToKafka(blobRef.getReference());
    }
}
```

##### 下游：直接填入引用写表

下游收到 Blob 引用后，无需再上传，直接将引用的原始字节写入 `VarBinaryVector`：

```java
try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
    TableArrowWriter arrowWriter = (TableArrowWriter) writer;
    try (VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot()) {
        root.allocateNew();
        BigIntVector idVector = (BigIntVector) root.getVector("id");
        VarBinaryVector imageVector = (VarBinaryVector) root.getVector("image_data");

        // references 来自上游（如从 Kafka 消费），每个元素是一个 Base64 格式的引用字符串
        List<String> references = consumeFromKafka();
        for (int i = 0; i < references.size(); i++) {
            idVector.setSafe(i, i);
            // 写入时需要 Base64 解码：服务端期望的是引用的原始字节
            Blob blob = Blob.fromReference(references.get(i));
            imageVector.setSafe(i, blob.getReferenceBytes());
        }
        root.setRowCount(references.size());
        writer.writeBatch(root);
    }
    writer.flush();
}
```

:::tip
也可以使用 `uploadBlob(columnId, inputStream)` 逐条上传单个 Blob，但每次调用会发起独立 HTTP 请求。对于批量场景，推荐使用 `batchUploadBlob()`。
:::

:::info 两种模式对比
| | 模式 A（自动上传） | 模式 B（手动管理引用） |
|---|---|---|
| 开启方式 | `withBatchBlobUploadEnabled(true)` | 默认，无需额外配置 |
| VarBinaryVector 中填写 | 原始二进制数据 | Blob 引用字节（`blob.getReferenceBytes()`） |
| 上传时机 | `writeBatch()` 时自动批量上传 | 通过 `batchUploadBlob()` 或 `uploadBlob()` 手动上传 |
| 上传请求数 | **1 次** / Batch | `batchUploadBlob()`: **1 次** / 批；`uploadBlob()`: **N 次** |
| 适用场景 | 客户端直写 MC，数据和上传一步到位 | Blob 上传与表写入解耦（如上下游分离架构） |
| 主键表（Delta Table） | **支持**（`writeBatch()` 时可从 PK 列计算分布键） | **不支持**（缺少 PK 上下文，无法计算分布键） |
:::

---

## 异常处理

| 异常类 | 说明 |
|--------|------|
| `BlobDownloadException` | Blob 下载失败，通过 `getFailedBlobRef()` 可获取失败的 Blob 引用 |
| `MaxStorageException` | 服务端错误，如 Blob 不存在、无权限等 |
| `ClientException` | 客户端错误，如参数非法、连接超时等 |

```java
try (BlobDataIterator iterator = blobManager.batchDownload(blobs)) {
    while (iterator.hasNext()) {
        try (InputStream stream = iterator.next()) {
            // 处理数据...
        }
    }
} catch (BlobDownloadException e) {
    Blob failedBlob = e.getFailedBlobRef();
    System.err.println("下载失败: " + (failedBlob != null ? failedBlob.getReference() : "未知"));
} catch (MaxStorageException e) {
    System.err.println("服务端错误: " + e.getMessage());
}
```

---

## 最佳实践

1. **优先使用批量下载**：`batchDownload()` 单次请求传输多个 Blob，效率远高于循环调用 `download()`
2. **及时关闭 InputStream**：每个 `InputStream` 用完后立即关闭，否则会阻塞迭代器读取下一个 Blob
3. **写入选型建议**：
   - 普通表优先选**行接口**（`RecordWriter`），代码简洁，Blob 上传完全透明
   - 客户端直写场景选 **Arrow 接口 + 模式 A**（`withBatchBlobUploadEnabled(true)`），数据和上传一步到位
   - 上下游分离架构选 **Arrow 接口 + 模式 B**：上游用 `batchUploadBlob()` 批量上传，下游直接填引用写表
   - **主键表（Delta Table）** 的 Blob 写入只能通过 **Arrow 接口 + 模式 A** 完成（行接口和模式 B 均不支持，因缺少 PK 上下文无法计算分布键）
4. **`newRecord()` 的重要性**：行接口必须使用 `recordWriter.newRecord()` 创建的 `Record`，才能支持 Blob 的自动上传；用其他方式创建的 `Record` 不具备此能力
