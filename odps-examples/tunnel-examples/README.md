## 批量数据上传下载示例

本项目演示了如何使用 odps tunnel sdk 完成批量数据的上传下载。

Tunnel 是 ODPS 的数据通道，用户可以通过 Tunnel 向 ODPS 中上传或者下载数据。
TableTunnel 是访问 ODPS Tunnel 服务的入口类，仅支持表数据（非视图）的上传和下载。

对一张表或 partition 上传下载的过程，称为一个session。session 由一或多个到 Tunnel RESTful API 的 HTTP Request 组成。

session 用 session ID 来标识，session 的超时时间是24小时，如果大批量数据传输导致超过24小时，需要自行拆分成多个 session。
数据的上传和下载分别由 `UploadSession` 和 `DownloadSession` 这两个会话来负责。

TableTunnel 提供创建 `UploadSession` 对象和 `DownloadSession` 对象的方法。

- 典型表数据下载流程：
  1) 创建 TableTunnel
  2) 创建 DownloadSession
  3) 创建 RecordReader,读取 Record

- 典型表数据上传流程：
  1) 创建 TableTunnel
  2) 创建 UploadSession
  3) 创建 RecordWriter （`TunnelRecordWriter` 或者 `TunnelBufferedWriter`）,写入 Record
  4）提交上传操作

- TunnelBufferedWriter 接口

在数据上传操作中，除了普通的 TunnelRecordWriter, 还提供了 TunnelBufferedWriter。
TunnelBufferedWriter 是一个使用缓冲区的、容错的 Tunnel 上传接口，具有如下特性：

    - 通过调用 write 接口将 record 写入缓冲区，当缓冲区大小超过 bufferSize 时将触发上传动作。
    - 上传过程中如果发生错误将自动进行重试。
 
- TunnelBufferedWriter 和 TunnelRecordWriter 的区别
 
在使用 `TunnelRecordWriter` 时用户需要先划分数据块，然后对每一个数据块分别执行打开、写入、关闭和提交操作。
这个过程中用户需要自己来容错(例如记录下上传失败的 block，以便重新上传)。

而 `TunnelBufferedWriter` 隐藏了数据块的细节，并将记录持久化在内存中，用户在会话中打开以后，就可以往里面写记录，TunnelBufferedWriter 会尽最大可能容错，
保证数据上传上去。降低了使用的门槛。

不过由于隐藏了数据块的细节，`TunnelBufferedWriter` 并不适合断点续传的场景。

## 示例代码

- TunnelDownloadSample
   简单的单线程数据下载示例

- TunnelUploadSample
   简单的单线程数据上传示例

- MultiThreadDownload
   多线程下载示例，适用于数据量较大的场景

- MultiThreadUpload
   多线程上传示例，适用于数据量较大的场景

- TunnelBufferedUploadSample
    简单的单线程使用 `TunnelBufferedWriter` 接口的数据上传示例

- MultiThreadBufferedWriter
    多线程上传示例，使用 `TunnelRecordWriter` 接口

## Usage
在示例代码中，填入 odps 账户信息、odps endpoint、project 名称以及table 名称，即可以完成该 table 数据的上传和下载。
如果是分区表，还需要填入 partition 信息。

如下代码所示：

```
  private static String accessId = "<your access id>";
  private static String accessKey = "<your access Key>";
  private static String odpsUrl = "<your odps endpoint>";
  private static String project = "<your project>";
  private static String table = "<your table name>";

  private static String partition = "<your partition spec>";
```
