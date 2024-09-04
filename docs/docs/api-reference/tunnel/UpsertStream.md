---
title: UpsertStreamImpl
sidebar_position: 5
---

# UpsertStream 类文档

## 概述

在当前版本，`UpsertStreamImpl` 类是 `UpsertStream`接口唯一的实现类，因此本文主要介绍`UpsertStreamImpl`的类接口。
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
public void upsert(Record record) throws IOException,TunnelException
```

插入或更新一条记录。

参数列表：
record：**注：参数`record`应当是`UpsertRecord`的实例**，这个实例可以通过 [`UpsertSession#newRecord()`](UpsertSession.md#创建一个record对象) 方法创建。

`UpsertRecord`内部维护了一些元数据隐藏列，因此用户不应当手动创建`ArrayRecord`对象将其传入。


### 插入或更新部分列

```java
@Override
public void upsert(Record record,List<String> upsertCols)throws IOException,TunnelException
```

插入或更新一条记录的部分列。

record : `UpsertRecord`的实例，这个实例可以通过 [`UpsertSession#newRecord()`](UpsertSession.md#创建一个record对象) 方法创建。

upsertCols : 一个包含要更新的列名的列表。

### 删除记录

```java
@Override
public void delete(Record record)throws IOException,TunnelException
```

删除一条记录。

record : `UpsertRecord`的实例，这个实例可以通过 [`UpsertSession#newRecord()`](UpsertSession.md#创建一个record对象) 方法创建。

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


## Builder 类

`UpsertStreamImpl.Builder` 类用于构建 `UpsertStreamImpl` 对象。通常由 [`UpsertSession#buildUpsertStream()`](UpsertSession.md#构建upsertstream) 方法调用。

### 方法

- `public Builder setSession(UpsertSessionImpl session)` 
- `public UpsertSessionImpl getSession()`
- `public long getMaxBufferSize()`
- `public Builder setMaxBufferSize(long maxBufferSize)`
- `public long getSlotBufferSize()`
- `public Builder setSlotBufferSize(long slotBufferSize)`
- `@Override public CompressOption getCompressOption()`
- `@Override public Builder setCompressOption(CompressOption compressOption)`
- `@Override public Listener getListener()`
- `@Override public Builder setListener(Listener listener)`
- `@Override public UpsertStream build() throws IOException, TunnelException`

## 总结

通过 `UpsertStreamImpl` 类，用户可以方便地进行数据的插入、更新和删除操作，并支持数据的缓冲和批量提交。该类还提供了丰富的配置选项和错误处理机制，确保数据操作的高效和可靠。