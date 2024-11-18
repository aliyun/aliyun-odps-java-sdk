package com.aliyun.odps.tunnel.streams;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;

public interface UpsertStream extends Closeable {
  /**
   * 按upsert操作，写入一条{@link Record}对象到缓冲区
   *
   * @param record - record对象
   */
  public void upsert(Record record) throws IOException, TunnelException;

  public void upsert(Record record, List<String> upsertCols) throws IOException, TunnelException;

  /**
   * 按delete操作，写入一条{@link Record}对象到缓冲区
   *
   * @param record - record对象
   */
  public void delete(Record record) throws IOException, TunnelException;

  /**
   * 发送缓冲区数据到服务端
   */
  public void flush() throws IOException, TunnelException;

  /**
   * 关闭UpsertStream, 调用后upsert/delete和flush接口均不可用
   * #TODO remove TunnelException
   */
  public void close() throws IOException;

  /**
   * 关闭UpsertStream, 调用后数据和状态将被清空重置
   */
  public void reset() throws IOException;

  class FlushResult {
    public String traceId;
    public long flushSize;
    public long recordCount;
    public long flushTime;
  }

  interface Listener {
    /**
     * flush成功回调接口
     * @param result - flush结果
     */
    void onFlush(FlushResult result);

    /**
     * flush失败回调接口
     * @param error - 错误信息
     * @param retry - 已重试次数
     * @return 是否重试
     */
    default boolean onFlushFail(Exception error, int retry) {
      return onFlushFail(error.getMessage(), retry);
    }

    /**
     * use {@link #onFlushFail(Exception, int)} instead
     */
    @Deprecated
    default boolean onFlushFail(String error, int retry) {
      return false;
    }
  }

  interface Builder {
    public long getMaxBufferSize();

    public Builder setMaxBufferSize(long maxBufferSize);

    public long getSlotBufferSize();

    public Builder setSlotBufferSize(long slotBufferSize);

    public CompressOption getCompressOption();

    public Builder setCompressOption(CompressOption compressOption);

    public Listener getListener();

    public Builder setListener(Listener listener);

    public UpsertStream build() throws IOException, TunnelException;
  }
}
