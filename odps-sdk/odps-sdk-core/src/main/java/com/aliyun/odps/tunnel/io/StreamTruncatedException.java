package com.aliyun.odps.tunnel.io;

import java.io.IOException;

/**
 * 当 Tunnel 下载数据流在没有收到 footer（TUNNEL_META_COUNT）的情况下结束时抛出此异常。
 *
 * <p>与普通 IOException 不同，此异常表示数据流被截断，重试无法恢复丢失的数据，
 * 因此 {@link TunnelRecordReader} 的重试逻辑不会捕获此异常进行重试。
 */
public class StreamTruncatedException extends IOException {

  private final long recordsRead;

  public StreamTruncatedException(String message, long recordsRead) {
    super(message);
    this.recordsRead = recordsRead;
  }

  public long getRecordsRead() {
    return recordsRead;
  }
}
