---
title: UploadSession 上传会话
sidebar_position: 4
---

UploadSession 作为 TableTunnel 的核心组件，专为用户提供了一种高效、灵活的手段来应对数据上传挑战，贯穿从初始化、数据写入到完成上传的整个生命周期管理。
本文档详尽解析了UploadSession的使用方法，包括如何初始化新会话或获取现有会话、运用RecordWriter与TunnelBufferedWriter实现数据写入操作、完成数据上传的提交流程，
以及探索RecordPack和Apache Arrow格式的进阶应用，旨在帮助开发者充分利用MaxCompute的高性能数据上传能力。
通过深入理解并实践这些指南，用户能够针对不同场景定制最合适的上传策略，确保数据传输既高效又可靠。

## 目录

- [初始化](#初始化)
- [使用 RecordWriter 写入数据](#使用-recordwriter-写入数据)
- [完成写入](#完成写入)
- [其他实用功能](#其他实用功能)
- [进阶操作：利用 RecordPack 实现高级写入特性](#进阶操作利用-recordpack-实现高级写入特性)
- [进阶操作：利用 Apache Arrow 格式高效写入](#进阶操作利用-apache-arrow-格式高效写入)

## 初始化

### 前提条件

确保你已经有一个有效的 `TableTunnel` 实例。如果尚未创建，请参考 [TableTunnel 文档](TableTunnel.md)
获取实例化方法。

### 创建新的上传会话

#### 对于非分区表

- **基本创建**

  ```java
  TableTunnel.UploadSession uploadSession = tableTunnel.createUploadSession(projectName, tableName);
  ```

- **带覆盖选项创建**

  如果希望新上传的数据覆盖表中已存在的数据，则需设置 `overwrite` 参数为 `true`。

  ```java
  TableTunnel.UploadSession uploadSession = tableTunnel.createUploadSession(projectName, tableName, overwrite);
  ```
- **指定Namespace Schema**

  若表属于特定Namespace Schema下，还需提供 `schemaName`。

  ```java
  TableTunnel.UploadSession uploadSession = tableTunnel.createUploadSession(projectName, schemaName, tableName, overwrite);
  ```

#### 对于分区表

- **基础用法**

  需要提供完整的分区规格 (`PartitionSpec`)，且分区必须是最末级的。

  ```java
  TableTunnel.UploadSession uploadSession = tableTunnel.createUploadSession(projectName, tableName, partitionSpec);
  ```

- **带覆盖与分区**

  同样可以指定是否覆盖已有数据。

  ```java
  TableTunnel.UploadSession uploadSession = tableTunnel.createUploadSession(projectName, tableName, partitionSpec, overwrite);
  ```

- **指定Namespace Schema**

  若表属于特定Namespace Schema下，还需提供 `schemaName`。

  ```java
  TableTunnel.UploadSession uploadSession = tableTunnel.createUploadSession(projectName, schemaName, tableName, partitionSpec, overwrite);
  ```

### 获取现有上传会话

已创建的上传会话可以通过其唯一的 `sessionId` 获取：

- **非分区表**

  ```java
  TableTunnel.UploadSession existingSession = tableTunnel.getUploadSession(projectName, tableName, sessionId);
  ```

- **分区表**

  ```java
  TableTunnel.UploadSession existingSession = tableTunnel.getUploadSession(projectName, tableName, partitionSpec, sessionId);
  ```

#### 多线程/进程共享会话

为了提高并发效率，可以在多线程或多进程中共享一个上传会话。这需要指定会话的总共享数 (`shares`)
和当前实例的唯一标识 (`shareId`)：

```java
TableTunnel.UploadSession sharedSession=tableTunnel.getUploadSession(projectName,tableName,sessionId,shares,shareId);
```

对于分区表，同样适用：

```java
TableTunnel.UploadSession sharedSession=tableTunnel.getUploadSession(projectName,tableName,partitionSpec,sessionId,shares,shareId);
```

### 注意事项

- 在调用上述方法前，务必确保 `TableTunnel` 实例是可用的，并且已正确配置了项目名等信息。
- 分区表的上传会话创建要求提供完整的分区路径。
- 多线程/进程共享同一会话时，合理分配 `shares` 和 `shareId` 是必要的，以避免资源冲突和数据混乱。

以上步骤指导你完成了一个 `UploadSession` 的初始化及复用，无论是创建新的上传任务还是继续之前中断的任务，这些方法都将是基础且重要的操作。

## 使用 RecordWriter 写入数据

`RecordWriter` 是用于向 MaxCompute 表中写入数据的核心组件。
通过 `UploadSession` 提供的一系列方法，你可以根据需求选择不同的压缩策略、超时设定以及自定义版本控制来高效地上传数据。
每个Session在服务端的生命周期为24小时。

### 核心概念

blockId：会话级别，用于标识数据块ID，使用者应当保证同一个blockId只有一个写入者，否则会导致数据混乱。通过指定不同的blockId，可以实现多线程写入操作，提升写入效率。

### 使用 TunnelRecordWriter

TunnelRecordWriter 与服务端维护一个长链接，每次调用`write(record)`方法，都会像服务端发送新的数据。
当写入数据结束时，一定需要调用`close` 方法，关闭 `RecordWriter` 断开链接。

**不压缩数据**

如果你不需要对上传数据进行压缩，可以使用以下方法打开 `RecordWriter`：

```java
RecordWriter writer=uploadSession.openRecordWriter(blockId);
```

其中，`blockId` 是用户自定义的一个0到19999之间的数字，用于标识本次上传的数据块。

**启用压缩**

若希望在数据传输过程中进行压缩，可以指定压缩参数：

```java
RecordWriter writer=uploadSession.openRecordWriter(blockId,true);
```

或者更具体地指定压缩算法：

```java
CompressOption compressOption=new CompressOption(CompressOption.CompressAlgorithm.GZIP,0,0);
    RecordWriter writer=uploadSession.openRecordWriter(blockId,compressOption);
```

### 使用 TunnelBufferedWriter

`TunnelBufferedWriter` 维护了一个缓冲区，只有当调用`TunnelBufferedWriter#flush()`方法时，才与服务端建立链接。

`TunnelBufferedWriter` 向用户屏蔽了 blockId 的概念，使写入操作更像传统文件写入。它通过内部维护一个缓冲区，当数据量超过缓冲区，或用户手动调用
`TunnelBufferedWriter#flush()`
操作时，将缓存数据作为一个新的数据块（blockId）发送到服务端。获取blockId的原理为根据shareId的递增值，因此当同一个Session开启了多个TunnelBufferedWriter时，需要参考
[多线程/进程共享会话](#多线程进程共享会话) 正确的维护 `shares` 和 `shareId`

**基础使用**

```java
RecordWriter bufferedWriter=uploadSession.openBufferedWriter();
```

**启用压缩**

```java
RecordWriter bufferedWriter=uploadSession.openBufferedWriter(true);
```

或指定压缩选项：

```java
CompressOption compressOption=new CompressOption(CompressOption.CompressAlgorithm.SNAPPY,0,0);
    RecordWriter bufferedWriter=uploadSession.openBufferedWriter(compressOption);
```

**设置超时**

`timeout` 表示超时时间 单位 `ms`, `<=0` 代表无超时.
推荐值: `(BufferSizeInMB / UploadBandwidthInMB) * 1000 * 120%`

```java
long timeout=(1024/100)*1000*1.2; // 示例超时计算，假设缓冲区大小1MB，上传带宽100MB/s
public RecordWriter openBufferedWriter(CompressOption compressOption,long timeout)
```

**设置超时与BlockVersionProvider**

为了更好地控制上传过程，可以设置超时时间和自定义Block版本控制逻辑：

```java
long timeout=(1024/100)*1000*1.2; // 示例超时计算，假设缓冲区大小1MB，上传带宽100MB/s
    BlockVersionProvider versionProvider=new CustomBlockVersionProvider(); // 自定义版本提供逻辑
    RecordWriter bufferedWriter=uploadSession.openBufferedWriter(compressOption,timeout,versionProvider);
```

#### 注意事项

- 在使用 `openRecordWriter` 方法时，确保 `blockId` 的唯一性和合理性，以免造成数据混乱。
- 一个Block大小上限100GB，建议大于64M的数据。
- 上传数据时，`RecordWriter`如果120秒内没有网络动作，服务端将主动关闭连接，此时Writer将不可用，请重新打开一个新的Writer写入。
- 如果不像手动维护 `blockId`和缓冲操作，可以使用 `TunnelBufferedWriter` 。
- 自定义 `BlockVersionProvider` 可以为每个数据块提供特定的版本控制逻辑，但需谨慎使用，确保不会引发版本冲突。

通过上述方法，你可以灵活地根据数据特点和上传需求选择合适的 `RecordWriter`
初始化方式，进而高效地将数据上传至MaxCompute表中。

## 完成写入

完成数据上传后，必须通过调用 `commit` 方法来确认，以确保数据最终被提交到 MaxCompute
表中。此步骤是确保数据持久化和可见性的关键操作。

### 基础提交

对于无需块级验证的情况，可使用简化版的提交方法：

```java
/**
 * 简化提交流程，不执行数据块校验。
 *
 * @throws TunnelException 上传过程中出现的隧道异常。
 * @throws IOException IO相关异常。
 */
public void commit()throws TunnelException,IOException;
```

### 带数据块校验的提交

若需要确保上传数据的完整性，可以提供已成功上传的数据块列表进行校验：

```java
/**
 * 提交上传任务，并进行数据块完整性校验。
 *
 * @param blocks 用户记录的已成功上传的数据块ID列表，用于与服务端核对。
 * @throws TunnelException 如果提交的块列表与服务端记录不匹配，则抛出异常。
 * @throws IOException IO相关异常。
 */
public void commit(Long[]blocks)throws TunnelException,IOException;
```

在实际操作中，根据是否需要进行块级完整性验证，选择合适的 `commit` 方法来安全且高效地完成上传任务。

## 其他实用功能

### 会话信息与管理

以下方法帮助您获取上传会话的详细信息及管理上传过程：

- **获取会话ID**:

  ```java
  /**
   * 返回当前上传会话的唯一标识ID。
   *
   * @return 上传会话ID。
   */
  public String getId();
  ```

- **获取表结构**:

  ```java
  /**
   * 获取与上传会话关联的表结构信息。
   *
   * @return 表的结构定义。
   */
  public TableSchema getSchema();
  ```

- **查询配额名称**:

  ```java
  /**
   * 获取用于本次上传的 quota 名称，注意并非是 quotaNickName。
   */
  public String getQuotaName();
  ```

- **获取会话状态**:

  ```java
  /**
   * 刷新并获取当前上传会话的状态。
   */
  public UploadStatus getStatus() throws TunnelException, IOException;
  ```

当前返回值包括以下状态码：

- UNKNOWN：Server端刚创建一个Session时设置的初始值。
- NORMAL：创建Upload对象成功。
- CLOSING：当调用complete方法（结束上传）时，服务端会先把状态置为CLOSING。
- CLOSED：完成结束上传（即把数据移动到结果表所在目录）后。
- EXPIRED：上传超时。
- CRITICAL：服务出错。

### 配置与记录创建

- **访问配置信息**:

  ```java
  /**
   * 提供上传会话使用的配置对象，以便于获取或修改配置项。
   */
  public Configuration getConfig();
  ```

- **创建记录对象**:

  ```java
  /**
   * 创建一个新的Record对象，准备用于数据上传。
   *
   * @return 一个新的Record实例，根据表结构初始化。
   */
  public Record newRecord();
  ```

这些辅助方法使得在上传数据前后或过程中，能够便捷地管理会话信息、检查状态、以及准备数据记录，从而提高开发效率和灵活性。

## 进阶操作：利用 RecordPack 实现高级写入特性

`RecordPack` 类的设计旨在内存中批量存储记录，为数据上传提供了更多的灵活性与定制化能力。通过它，您可以更高效地组织和处理数据，特别是在处理大量记录或需要特定序列化逻辑的场景下。

### RecordPack 基础

`RecordPack` 是一个抽象类，提供了两个核心方法来构建和读取记录集合：

```java
/**
 * RecordPack 基类，用于内存中批量管理 Record 对象。
 */
public abstract class RecordPack {

  /**
   * 向 RecordPack 中添加一条记录。
   *
   * @param record 待插入的记录对象。
   * @throws IOException 操作失败时抛出。
   */
  abstract public void append(Record record) throws IOException;

  /**
   * 获取包含所有记录的 RecordReader 对象。
   *
   * @return RecordReader 对象，用于遍历包内的所有记录。
   * @throws IOException 操作失败时抛出。
   */
  abstract public RecordReader getRecordReader() throws IOException;
}
```

### 创建 RecordPack

`TableTunnel` 类提供了创建 `RecordPack` 的多种方式，支持自定义压缩选项、缓冲区大小等，以适应不同场景的需求：

- **无参创建**：默认创建一个 `ProtobufRecordPack` 实例。

  ```java
  public RecordPack newRecordPack() throws IOException;
  ```

- **带压缩选项创建**：允许指定压缩算法。

  ```java
  public RecordPack newRecordPack(CompressOption option) throws IOException;
  ```

- **完整参数创建**：同时指定缓冲区大小和压缩选项，为高级用户提供了更细致的控制。

  ```java
  public RecordPack newRecordPack(int capacity, CompressOption option) throws IOException;
  ```

#### 通过 RecordPack 写入数据块

`UploadSession` 拥有如下方法，可以利用 `RecordPack` 写入数据到指定的 `blockId`
，支持超时控制和自定义块版本，提供了覆盖写和版本控制的能力：

- **基本写入**：向指定块写入 RecordPack，后续写入会覆盖原有数据。

  ```java
  public void writeBlock(long blockId, RecordPack pack) throws IOException;
  ```

- **带超时写入**：增加了超时控制，适用于对延时敏感的上传操作。

  ```java
  public void writeBlock(long blockId, RecordPack pack, long timeout) throws IOException;
  ```

- **带版本控制写入**：在超时控制基础上，加入了块版本验证，确保数据一致性。

  ```java
  public void writeBlock(long blockId, RecordPack pack, long timeout, long blockVersion)
      throws IOException, TunnelException;
  ```

通过上述接口和方法，开发者可以根据具体需求灵活构造和上传 `RecordPack`，实现数据的高效、定制化上传至
MaxCompute 表。

## 进阶操作：利用 Apache Arrow 格式高效写入

为了提升大规模数据的处理效率，本 SDK 支持使用 Apache Arrow 格式进行数据写入，这尤其适合需要高性能列式存储和零拷贝数据处理的场景。

### 获取 Arrow Schema

```java
/**
 * 获取与当前表结构对应的 Arrow Schema。
 *
 * @return Apache Arrow 格式的表结构描述。
 */
public Schema getArrowSchema();
```

### 打开 ArrowRecordWriter

以下方法展示了如何初始化 `ArrowRecordWriter` 以进行 Arrow 格式的数据写入。支持压缩选项和块版本控制，以满足不同场景下的需求。

- **基础打开方法**:

  ```java
  /**
   * 使用默认压缩设置（无压缩）打开 ArrowRecordWriter。
   *
   * @param blockId 数据块标识。
   * @return 初始化好的 ArrowRecordWriter 实例。
   */
  public ArrowRecordWriter openArrowRecordWriter(long blockId)
      throws TunnelException, IOException;
  ```

- **带压缩选项打开**:

  ```java
  /**
   * 指定压缩选项打开 ArrowRecordWriter。
   *
   * @param blockId 数据块标识。
   * @param option 压缩选项。
   * @return 初始化好的 ArrowRecordWriter 实例。
   */
  public ArrowRecordWriter openArrowRecordWriter(long blockId, CompressOption option)
      throws TunnelException, IOException;
  ```

- **带压缩选项与块版本控制打开**:

  ```java
  /**
   * 指定压缩选项和块版本控制打开 ArrowRecordWriter。
   *
   * @param blockId 数据块标识。
   * @param option 压缩选项。
   * @param blockVersion 块版本号。
   * @return 初始化好的 ArrowRecordWriter 实例。  
   */
  public ArrowRecordWriter openArrowRecordWriter(long blockId, CompressOption option, long blockVersion)
      throws TunnelException, IOException;
  ```

通过上述接口，您可以灵活地根据实际需求选择合适的压缩策略和版本控制逻辑，高效地利用 Arrow 格式进行数据上传。

