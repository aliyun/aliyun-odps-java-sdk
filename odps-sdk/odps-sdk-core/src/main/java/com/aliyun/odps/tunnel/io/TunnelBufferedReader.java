package com.aliyun.odps.tunnel.io;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TunnelBufferedReader implements RecordReader, AutoCloseable {
  private static final long DEFAULT_BATCH_SIZE = 1024 * 1024 * 64;
  private long start;
  private long count;
  private final long batchSize;
  private final List<Column> columnList;
  private final CompressOption option;
  private final InstanceTunnel.DownloadSession instanceSession;
  private final TableTunnel.DownloadSession tableSession;
  private boolean disableModifiedCheck;
  private final LinkedList<Record> recordBuffer = new LinkedList<>();
  private boolean shouldTransform = false;

  /**
   * 构造此类对象
   *
   * @param columns              需要读取的列 {@link Column}
   * @param option               {@link CompressOption}
   * @param start                本次要读取记录的起始位置
   * @param count                本次要读取记录的数量
   * @param batchSize            每次读取的记录大小, 如填写 <= 0, 默认值为 64MB
   * @param session              本次读取所在 session
   * @param disableModifiedCheck 不检查下载的数据是否是表中最新数据
   * @throws IOException
   */
  public TunnelBufferedReader(long start, long count, long batchSize, List<Column> columns,
                              CompressOption option,
                              TableTunnel.DownloadSession session, boolean disableModifiedCheck) {
    this.start = start;
    this.count = count;
    this.batchSize = batchSize <= 0 ? DEFAULT_BATCH_SIZE : batchSize;
    this.option = option;
    this.columnList = columns;
    this.tableSession = session;
    this.instanceSession = null;
    this.disableModifiedCheck = disableModifiedCheck;
  }

  public TunnelBufferedReader(long start, long count, long batchSize, List<Column> columns,
                              CompressOption option,
                              InstanceTunnel.DownloadSession session) {
    this.start = start;
    this.count = count;
    this.batchSize = batchSize <= 0 ? DEFAULT_BATCH_SIZE : batchSize;
    this.option = option;
    this.columnList = columns;
    this.instanceSession = session;
    this.tableSession = null;
  }

  @Override
  public Record read() throws IOException {
    try {
      if (recordBuffer.isEmpty()) {
        openReader();
        if (recordBuffer.isEmpty()) {
          return null;
        }
      }
      return recordBuffer.pollFirst();
    } catch (TunnelException e) {
      throw new IOException(e);
    }
  }

  private void openReader() throws IOException, TunnelException {
    if (count <= 0) {
      return;
    }
    TunnelRecordReader recordReader;
    if (tableSession != null) {
      recordReader =
          tableSession.openRecordReader(start, count, option, columnList, disableModifiedCheck);
    } else if (instanceSession != null) {
      recordReader =
          instanceSession.openRecordReader(start, count, option, columnList);
    } else {
      throw new IllegalArgumentException("Cannot create record reader if session is null.");
    }
    recordReader.setTransform(this.shouldTransform);
    long alreadyReadBytes = 0;
    Record record;
    while ((record = recordReader.read()) != null) {
      recordBuffer.add(record);
      alreadyReadBytes += recordReader.getTotalBytes();
      if (alreadyReadBytes >= batchSize) {
        break;
      }
    }
    recordReader.close();
    start += recordBuffer.size();
    count -= recordBuffer.size();
  }

  public void setTransform(boolean shouldTransform) {
    this.shouldTransform = shouldTransform;
  }

  @Override
  public void close() throws IOException {
    recordBuffer.clear();
  }
}
