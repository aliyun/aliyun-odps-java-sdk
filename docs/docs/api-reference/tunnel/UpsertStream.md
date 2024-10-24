---
title: UpsertStream
sidebar_position: 6
---

# UpsertStream

## 概述

在当前版本，`UpsertStreamImpl` 类是 `UpsertStream`接口唯一的实现类，因此本文主要介绍`UpsertStreamImpl`
的类接口。
通过该类，用户可以对`Delta Table`进行数据的插入、更新、删除操作，并支持数据的缓冲和批量提交。

## 构造方法

### UpsertStreamImpl(Builder builder)

```java
public UpsertStreamImpl(Builder builder)throws IOException,TunnelException
```

通过 `Builder` 对象构建 `UpsertStreamImpl`
实例的构造方法，通常由 [`UpsertSession#buildUpsertStream()`](UpsertSession.md#构建upsertstream) 方法调用。

## 方法

### 插入或更新记录

```java
@Override
public void upsert(Record record)throws IOException,TunnelException
```

插入或更新一条记录。

参数列表：
record：**注：参数`record`应当是`UpsertRecord`的实例**
，这个实例可以通过 [`UpsertSession#newRecord()`](UpsertSession.md#创建一个record对象) 方法创建。

:::info
`UpsertRecord`内部维护了一些元数据隐藏列，因此用户不应当手动创建`ArrayRecord`对象将其传入。
:::

### 插入或更新部分列

```java
@Override
public void upsert(Record record,List<String> upsertCols)throws IOException,TunnelException
```

插入或更新一条记录的部分列。

record : `UpsertRecord`
的实例，这个实例可以通过 [`UpsertSession#newRecord()`](UpsertSession.md#创建一个record对象) 方法创建。

upsertCols : 一个包含要更新的列名的列表。

### 删除记录

```java
@Override
public void delete(Record record)throws IOException,TunnelException
```

删除一条记录。

record : `UpsertRecord`
的实例，这个实例可以通过 [`UpsertSession#newRecord()`](UpsertSession.md#创建一个record对象) 方法创建。

### 刷新缓冲区

```java
@Override
public void flush()throws IOException,TunnelException
```

刷新缓冲区，将缓冲区中的数据提交到服务器。

### 关闭流

```java
@Override
public void close()throws IOException,TunnelException
```

关闭流，提交所有未提交的数据。

### 重置缓冲区

```java
@Override
public void reset()throws IOException
```

重置缓冲区，清空所有未提交的数据。

## 重试逻辑

UpsertStream 通过用户传入 `Listener` 监听器来处理重试逻辑。当用户不传入 `Listener` 时，UpsertStream
将使用一个默认的 `Listener` 实例(`UpsertSessionImpl.DefaultUpsertSteamListener`)

这个默认的`Listener`实例，遵循`TableTunnel`默认的[重试逻辑](RetryLogic.md)。

:::tip
十分推荐当用户实现自定义`Listener`时，继承`UpsertSessionImpl.DefaultUpsertSteamListener`
，以使`TableTunnel`的重试逻辑生效。
:::


## Builder 类

`UpsertStreamImpl.Builder` 类是构建 `UpsertStreamImpl` 实例的基石，它提供了灵活的配置选项以便用户根据需求定制 `UpsertStream` 的行为。
此构造器主要通过 [`UpsertSession#buildUpsertStream()`](UpsertSession.md#构建upsertstream) 方法被调用，允许用户在创建 `UpsertStream` 时精确控制多个参数，
以优化数据插入或更新的性能和资源使用。

### 方法详情

#### 设置会话对象

- **方法签名**:
  ```java
  public Builder setSession(UpsertSessionImpl session)
  ```
- **功能说明**:
  为 `UpsertStream` 指定关联的 `UpsertSessionImpl` 实例。这是UpsertSession#buildUpsertStream()的默认步骤，通常不需要用户手动调用。 

#### 获取会话对象

- **方法签名**:
  ```java
  public UpsertSessionImpl getSession()
  ```
- **功能说明**:
  返回已设置的 `UpsertSessionImpl` 实例，供外部查询或验证使用。

#### 设置最大缓冲区大小

- **方法签名**:
  ```java
  public Builder setMaxBufferSize(long maxBufferSize)
  ```
- **功能说明**:
  允许用户设定数据缓冲的最大容量（单位：字节）。这直接影响到内存使用和批量写入的效率，较大的缓冲区可以减少I/O操作次数，但会占用更多内存。

#### 获取最大缓冲区大小

- **方法签名**:
  ```java
  public long getMaxBufferSize()
  ```
- **功能说明**:
  查询当前设置的最大缓冲区大小。

#### 设置槽缓冲区大小

- **方法签名**:
  ```java
  public Builder setSlotBufferSize(long slotBufferSize)
  ```
- **功能说明**:
  为每个桶设置缓冲区大小（单位：字节）。

#### 获取槽缓冲区大小

- **方法签名**:
  ```java
  public long getSlotBufferSize()
  ```
- **功能说明**:
  获取当前设置的每个桶的缓冲区大小。

#### 压缩选项配置

- **获取压缩选项**:
  ```java
  @Override public CompressOption getCompressOption()
  ```
- **设置压缩选项**:
  ```java
  @Override public Builder setCompressOption(CompressOption compressOption)
  ```
- **功能说明**:
  控制数据在传输前的压缩策略。通过选择合适的压缩算法，可以在减少网络传输成本和存储空间的同时，可能会增加一定的CPU使用。

#### 设置监听器

- **获取监听器**:
  ```java
  @Override public Listener getListener()
  ```
- **设置监听器**:
  ```java
  @Override public Builder setListener(Listener listener)
  ```
- **功能说明**:
  注册一个监听器，用于接收 `UpsertStream` 在数据写入过程中的事件通知，和重试逻辑的配置。UpsertSession#buildUpsertStream() 方法会为 UpsertStream 配置一个默认的 Listener 实例。详情参考[重试逻辑](#重试逻辑)


#### 构建 UpsertStream 实例

- **方法签名**:
  ```java
  @Override public UpsertStream build() throws IOException, TunnelException
  ```
- **功能说明**:
  根据之前设置的所有配置项构建并返回一个 `UpsertStream` 实例。

## 总结

通过 `UpsertStreamImpl` 类，用户可以方便地进行数据的插入、更新和删除操作，并支持数据的缓冲和批量提交。该类还提供了丰富的配置选项和错误处理机制，确保数据操作的高效和可靠。