---
title: StreamUploadSession 流式上传会话
sidebar_position: 7
---

StreamUploadSession 作为 TableTunnel 的核心组件，是Stream Tunnel组件的一员部分。
与 [UploadSession](UploadSession.md) 不同的是，StreamUploadSession
专为流式数据上传场景而设计，支持写入即可见（auto-commit）特性，不需要用户手动提交会话。

## 初始化
创建 StreamUploadSession 需要先初始化一个 TableTunnel 示例。TableTunnel 示例初始化流程参考[初始化TableTunnel](TableTunnel.md#初始化)