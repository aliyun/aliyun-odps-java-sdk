package com.aliyun.odps.tunnel.io;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TunnelMetrics;

/**
 * {@link TunnelBufferedReader} 读一个批次所需要的能力集合。
 *
 * <p>包级可见，不是对外 API，实现者目前只有 {@link TunnelRecordReader}。抽出来只有一个原因：让
 * "批次读到一半抛错时资源有没有收尾"能被离线单测覆盖。{@link TunnelRecordReader} 的构造必须发 HTTP
 * 请求去拿 session 与连接，测试里造不出一个"读到第 N 条抛错、并且能记下 close() 被调了几次"的替身，
 * 而漏掉的正是 close() 里的 conn.disconnect() 与解压层收尾。
 *
 * @see TunnelBufferedReader#openBatchReader(long, long)
 */
interface BatchRecordReader {

  Record read() throws IOException;

  void setTransform(boolean shouldTransform);

  void setRequireStreamFooter(boolean requireStreamFooter);

  TunnelMetrics getMetrics();

  void close() throws IOException;
}
