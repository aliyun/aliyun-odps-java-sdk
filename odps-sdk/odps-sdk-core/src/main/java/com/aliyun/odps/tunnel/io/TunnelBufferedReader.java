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
  /** 非 null = 这一批的 reader 还没收尾；调用方 close() 时兜底关掉它。 */
  private BatchRecordReader currentReader;
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
    BatchRecordReader recordReader = openBatchReader(start + currOffset, recordNum);
    currentReader = recordReader;
    boolean batchSettled = false;
    try {
      recordReader.setTransform(this.shouldTransform);
      recordReader.setRequireStreamFooter(this.requireStreamFooter);
      Record record = recordReader.read();
      while (record != null) {
        recordBuffer.add(record);
        record = recordReader.read();
      }
      recordReader.close();
      metrics.add(recordReader.getMetrics());
      batchSettled = true;
    } finally {
      // 批次读到一半抛错（流被截断、校验和不对、protobuf 帧不完整都是实测过的形状）时也必须把这一批的
      // reader 关掉：RawTunnelRecordReader.close() 里的 conn.disconnect() 与解压层收尾都挂在它身上，
      // 原来没有 try/finally，一抛错就整段跳过。成功路径的关闭时机与 metrics 行为保持原样。
      boolean unsettledReaderClosed = true;
      if (!batchSettled) {
        try {
          recordReader.close();
        } catch (IOException | RuntimeException closeFailure) {
          // 收尾失败不能顶掉让本批失败的原始异常。currentReader 保持非 null，调用方 close() 时再试
          // 一次，那时仍失败就把收尾错误交给调用方。
          unsettledReaderClosed = false;
        }
      }
      if (unsettledReaderClosed) {
        currentReader = null;
      }
    }
  }

  /**
   * 开一个批次的 reader。默认实现就是抽取前 openReader() 里的 session 分派，行为一字未改；
   * 留成包级方法是测试注入替身的接缝，见 {@link BatchRecordReader}。
   */
  BatchRecordReader openBatchReader(long readerStart, long readerCount)
      throws IOException, TunnelException {
    if (tableSession != null) {
      return tableSession.openRecordReader(readerStart, readerCount, bufferSize, option, columnList,
          disableModifiedCheck);
    } else if (instanceSession != null) {
      return instanceSession.openRecordReader(readerStart, readerCount, bufferSize, option, columnList);
    } else {
      throw new IllegalArgumentException("Cannot create record reader if session is null.");
    }
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
    // 兜底：批次中途抛错且那次收尾没成功时，调用方的 close() 仍能把连接断掉；
    // 已经正常收尾过的批次（currentReader == null）在这里不会被二次 close。
    BatchRecordReader unsettled = currentReader;
    if (unsettled != null) {
      currentReader = null;
      unsettled.close();
    }
  }
}
