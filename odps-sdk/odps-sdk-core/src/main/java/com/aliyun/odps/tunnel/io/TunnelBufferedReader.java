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
import com.aliyun.odps.tunnel.TunnelMetrics;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TunnelBufferedReader implements RecordReader, AutoCloseable {

  private long start;
  private long count;
  private long currOffset;
  private final long batchSize;
  private final long bufferSize;
  private final List<Column> columnList;
  private final CompressOption option;
  private final InstanceTunnel.DownloadSession instanceSession;
  private final TableTunnel.DownloadSession tableSession;
  private boolean disableModifiedCheck;
  private final TunnelMetrics metrics = new TunnelMetrics();
  private final LinkedList<Record> recordBuffer = new LinkedList<>();
  private boolean shouldTransform = false;
  private boolean requireStreamFooter = false;

  /**
   * 构造此类对象
   *
   * @param columns              需要读取的列 {@link Column}
   * @param option               {@link CompressOption}
   * @param start                本次要读取记录的起始位置
   * @param count                本次要读取记录的数量
   * @param batchSize            每次读取的记录数量, 与 bufferSize 只会有一个生效
   * @param bufferSize           每次读取的记录大小，与 batchSize 只会有一个生效
   * @param session              本次读取所在 session
   * @param disableModifiedCheck 不检查下载的数据是否是表中最新数据
   * @throws IOException
   */
  public TunnelBufferedReader(long start, long count, long batchSize, long bufferSize,
                              List<Column> columns,
                              CompressOption option,
                              TableTunnel.DownloadSession session, boolean disableModifiedCheck) {
    this.start = start;
    this.count = count;
    this.batchSize = batchSize <= 0 ? 1000 : batchSize;
    this.bufferSize = bufferSize;
    this.option = option;
    this.columnList = columns;
    this.tableSession = session;
    this.instanceSession = null;
    this.disableModifiedCheck = disableModifiedCheck;
  }

  public TunnelBufferedReader(long start, long count, long batchSize,
                              long bufferSize,
                              List<Column> columns,
                              CompressOption option,
                              InstanceTunnel.DownloadSession session) {
    this.start = start;
    this.count = count;
    this.batchSize = batchSize <= 0 ? 1000 : batchSize;
    this.bufferSize = bufferSize;
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
      Record record = recordBuffer.pollFirst();
      currOffset++;
      return record;
    } catch (TunnelException e) {
      throw new IOException(e);
    }
  }

  private void openReader() throws IOException, TunnelException {
    if (count - currOffset <= 0) {
      return;
    }
    long recordNum = Math.min(count - currOffset, batchSize);
    TunnelRecordReader recordReader;
    if (tableSession != null) {
      recordReader =
          tableSession.openRecordReader(start + currOffset, recordNum, bufferSize, option, columnList, disableModifiedCheck);
    } else if (instanceSession != null) {
      recordReader =
          instanceSession.openRecordReader(start + currOffset, recordNum, bufferSize, option, columnList);
    } else {
      throw new IllegalArgumentException("Cannot create record reader if session is null.");
    }

    recordReader.setTransform(this.shouldTransform);
    recordReader.setRequireStreamFooter(this.requireStreamFooter);
    Record record = recordReader.read();
    while (record != null) {
      recordBuffer.add(record);
      record = recordReader.read();
    }
    recordReader.close();
    metrics.add(recordReader.getMetrics());
  }

  public void setTransform(boolean shouldTransform) {
    this.shouldTransform = shouldTransform;
  }

  public void setRequireStreamFooter(boolean requireStreamFooter) {
    this.requireStreamFooter = requireStreamFooter;
  }

  public TunnelMetrics getMetrics() {
    return metrics;
  }

  @Override
  public void close() throws IOException {
    recordBuffer.clear();
  }
}
