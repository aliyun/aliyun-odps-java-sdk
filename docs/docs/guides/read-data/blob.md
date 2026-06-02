---
title: Blob 数据下载
description: 使用 Storage API 的 BlobManager 下载非结构化二进制大对象数据（图片、音频、文档等）。
sidebar_position: 5
module: odps-sdk-storage-api
task: read-data
apis: [BlobManager]
since: "0.54.0"
keywords: [Blob, 二进制, 非结构化, BlobManager, 下载, 批量下载]
---

# Blob 数据下载

MaxCompute 的 `BLOB` 类型用于存储非结构化的二进制大对象数据（如图片、音频、文档等）。Blob 在表中以引用（Reference）的形式存储，实际的二进制数据需要通过 `BlobManager` 单独下载。

## 前置条件

- 添加 `odps-sdk-storage-api` 模块依赖
- 已初始化 `MaxStorageClient`
- 目标表包含 `BLOB` 类型列
- SDK 版本 >= 0.54.0

## 完整示例

```java
import com.aliyun.odps.storage.api.MaxStorageClient;
import com.aliyun.odps.storage.api.TableIdentifier;
import com.aliyun.odps.storage.api.blob.Blob;
import com.aliyun.odps.storage.api.blob.BlobManager;
import com.aliyun.odps.storage.api.blob.BlobDataIterator;
import com.aliyun.odps.storage.api.read.TableReadSession;
import com.aliyun.odps.storage.api.read.InputSplit;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BlobDownloadExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建客户端
        MaxStorageClient client = MaxStorageClient.builder()
            .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
            .credentialsProvider(credentialsProvider)
            .build();

        // 2. 读取包含 Blob 列的表
        TableIdentifier tableId = TableIdentifier.of("my_project", "image_table");
        TableReadSession session = client.createTableReadSessionBuilder(tableId)
            .withColumns(Arrays.asList("id", "image_data"))
            .build();

        // 3. 获取 BlobManager
        BlobManager blobManager = client.openBlobManager();

        // 4. 读取表数据并收集 Blob 引用
        for (InputSplit split : session.getSplits()) {
            ArrowReader arrowReader = session.createReaderBuilder(split).build();
            RecordReader recordReader = arrowReader.getAsRecordReader();

            List<Blob> blobs = new ArrayList<>();
            List<Long> ids = new ArrayList<>();

            recordReader.forEach(record -> {
                Blob blob = (Blob) record.get("image_data");
                if (blob != null) {
                    blobs.add(blob);
                    ids.add((Long) record.get("id"));
                }
            });

            // 5. 批量下载 Blob 数据
            try (BlobDataIterator iterator = blobManager.batchDownload(blobs)) {
                int index = 0;
                while (iterator.hasNext()) {
                    try (InputStream data = iterator.next()) {
                        byte[] content = data.readAllBytes();
                        // 保存到本地文件
                        Path outputPath = Paths.get("/tmp/images/" + ids.get(index) + ".jpg");
                        Files.write(outputPath, content);
                        System.out.println("已下载: " + outputPath + " (" + content.length + " bytes)");
                    }
                    index++;
                }
            }

            arrowReader.close();
            recordReader.close();
        }
    }
}
```

## 代码说明

1. **创建客户端**：与 Storage API 读取相同，通过 `MaxStorageClient.builder()` 构建。
2. **读取表数据**：使用 `TableReadSession` 读取包含 Blob 列的表，获取每行的 Blob 引用。
3. **获取 BlobManager**：通过 `client.openBlobManager()` 获取 Blob 管理器实例。
4. **收集 Blob 引用**：通过行接口（`RecordReader`）读取表数据，从 Record 中获取 `Blob` 对象。
5. **批量下载**：调用 `blobManager.batchDownload(blobs)` 批量下载，返回 `BlobDataIterator` 迭代器。
6. **处理数据**：从迭代器中逐个获取 `InputStream`，读取实际的二进制内容。

## 配置选项

### BlobManager 方法

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `download(Blob)` | Blob 引用对象 | InputStream | 下载单个 Blob，调用方必须关闭流 |
| `batchDownload(List<Blob>)` | Blob 引用列表 | BlobDataIterator | 批量下载，返回顺序与输入一致 |

### Blob 构造方式

| 方法 | 说明 |
|------|------|
| `Blob.fromReference(String)` | 从引用字符串构造 Blob 对象 |
| Record 中直接获取 | 行接口自动返回 `Blob` 对象，无需手动构造 |

## 单条下载

适用于 Blob 数量较少或需要按需下载的场景：

```java
BlobManager blobManager = client.openBlobManager();

// 从 Record 中获取 Blob 引用
Blob blobRef = (Blob) record.get("image_data");

// 单条下载
try (InputStream data = blobManager.download(blobRef)) {
    byte[] content = data.readAllBytes();
    System.out.println("Blob 大小: " + content.length + " bytes");
}
```

## 批量下载

适用于大量 Blob 的高效下载，单次请求传输多个 Blob：

```java
BlobManager blobManager = client.openBlobManager();

// 收集多个 Blob 引用
List<Blob> blobs = new ArrayList<>();
// ... 从表数据中收集 ...

// 批量下载
try (BlobDataIterator iterator = blobManager.batchDownload(blobs)) {
    while (iterator.hasNext()) {
        try (InputStream data = iterator.next()) {
            byte[] content = data.readAllBytes();
            // 处理 content...
        }
    }
} catch (BlobDownloadException e) {
    // 获取失败的 Blob 引用
    Blob failedBlob = e.getFailedBlobRef();
    System.err.println("下载失败: " + failedBlob.getReference());
}
```

## Arrow 列接口读取 Blob

如果使用 Arrow 列接口而非行接口，需要手动从 `VarBinaryVector` 中解析 Blob 引用：

```java
import org.apache.arrow.vector.VarBinaryVector;
import java.nio.charset.StandardCharsets;

for (InputSplit split : session.getSplits()) {
    try (ArrowReader reader = session.createReaderBuilder(split).build()) {
        while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            VarBinaryVector blobVector = (VarBinaryVector) root.getVector("image_data");

            List<Blob> blobRefs = new ArrayList<>();
            for (int row = 0; row < root.getRowCount(); row++) {
                if (!blobVector.isNull(row)) {
                    // 读取时 VarBinaryVector 中是引用字符串的 UTF-8 字节
                    String reference = new String(blobVector.get(row), StandardCharsets.UTF_8);
                    blobRefs.add(Blob.fromReference(reference));
                }
            }

            if (!blobRefs.isEmpty()) {
                try (BlobDataIterator iterator = blobManager.batchDownload(blobRefs)) {
                    while (iterator.hasNext()) {
                        try (InputStream data = iterator.next()) {
                            byte[] content = data.readAllBytes();
                            // 处理 content...
                        }
                    }
                }
            }
        }
    }
}
```

## 注意事项

- **优先使用批量下载**：`batchDownload()` 单次请求传输多个 Blob，效率远高于循环调用 `download()`。
- **必须关闭 InputStream**：每个 `InputStream` 用完后必须立即关闭，否则会阻塞迭代器读取下一个 Blob。
- **BlobDataIterator 必须关闭**：务必使用 try-with-resources 确保底层网络连接正确释放。
- **迭代顺序**：`batchDownload()` 返回的数据顺序与输入的 Blob 引用列表顺序一致。
- **行接口 vs Arrow 接口**：推荐使用行接口（`RecordReader`）读取 Blob 列，SDK 自动处理引用格式转换；使用 Arrow 列接口需要手动从字节解码引用字符串。
- **异常处理**：`BlobDownloadException` 提供 `getFailedBlobRef()` 方法，可获取下载失败的具体 Blob 引用，便于重试。

## 相关文档

- [Storage API 读取](./storage-api-read.md) - Storage API 读取表数据
- [读取数据概览](./index.md) - 三种方式对比与选型
