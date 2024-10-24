---
title: TableTunnel
sidebar_position: 1
---

# TableTunnel 文档

## 初始化

在使用 `TableTunnel` 之前，需要进行 Odps 对象的初始化操作，对 Odps 对象的初始化操作参考[初始化 Odps 对象](../../core-concept/init-odps-client.md)。

根据是否需要自定义TableTunnel的配置，可以使用如下两个接口来初始化TableTunnel。

**无参初始化**
```java
TableTunnel tunnel = odps.tableTunnel();
```

**使用配置文件进行初始化**
```java
// com.aliyun.odps.tunnel.Configuration;
Configuration configuration=Configuration.builder(odps).build();
TableTunnel tunnel = odps.tableTunnel(configuration);
```
有关配置文件的配置项，请参考[Configuration 文档](Configuration.md)。

## 初始化 UploadSession

## 初始化 StreamUploadSession

## 初始化 UpsertSession

`TableTunnel` 提供了 `buildUpsertSession` 方法来初始化一个 Upsert 会话的 Builder。
```java
TableTunnel.UpsertSession.Builder buildUpsertSession(String projectName, String tableName);
```
有关 Upsert 会话 Builder 的使用方法和全部配置项，请参考[UpsertSession 初始化](UpsertSession.md#builder-接口)。

## 初始化 DownloadSession
