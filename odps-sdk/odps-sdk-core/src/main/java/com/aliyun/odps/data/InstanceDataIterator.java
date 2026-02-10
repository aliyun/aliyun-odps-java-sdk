package com.aliyun.odps.data;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

/**
 * Use sharded concurrent download mode to download sample data.
 * Variable parameter split size, and number of preload splits.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class InstanceDataIterator implements Iterator<Record>, AutoCloseable {

  private static final Record EOF_RECORD = new EOFRecord();
  private final ExecutorService executor;
  private final int splitNum;
  private final BlockingQueue<Record>[] queues;
  private final AtomicReference<Throwable> error = new AtomicReference<>();
  private final long offset;
  private final long recordCount;
  private final int preloadSplitNum;
  private final long splitSize;
  private final InstanceTunnel.DownloadSession downloadSession;
  private final int threadNum;

  private int currentSplit = 0;

  // This field holds the next record to be returned. It is the "look-ahead" buffer.
  private Record nextRecord;

  public InstanceDataIterator(InstanceTunnel.DownloadSession downloadSession, long offset, long readCount, long splitSize, int preloadSplitNum, int threadNum) throws TunnelException {
    this.downloadSession = downloadSession;
    this.offset = offset;
    this.recordCount = (readCount < 0) ? downloadSession.getRecordCount() - offset : Math.min(readCount, (downloadSession.getRecordCount() - offset));
    this.splitSize = (splitSize <= 0) ? this.recordCount : splitSize;
    this.splitNum = computeSplitNum(this.splitSize, recordCount);
    this.preloadSplitNum = (preloadSplitNum == -1) ? splitNum : Math.max(preloadSplitNum, 1);
    this.threadNum = (threadNum == -1) ? Math.min(this.preloadSplitNum, Runtime.getRuntime()
        .availableProcessors() * 2) : threadNum;

    this.executor = Executors.newFixedThreadPool(this.threadNum);
    this.queues = new LinkedBlockingQueue[this.splitNum];
    // Initialize first batch of splits
    for (int i = 0; i < this.preloadSplitNum && i < this.splitNum; i++) {
      submitNextSplit(i);
    }
  }

  public InstanceDataIterator(Odps odps, Instance instance, long offset, long readCount, long splitSize, int preloadSplitNum, int threadNum) throws TunnelException {
    this(new InstanceTunnel(odps).createDownloadSession(instance.getProject(), instance.getId(), false), offset, readCount, splitSize, preloadSplitNum, threadNum);
  }

  private int computeSplitNum(long splitSize, long recordCount) {
    return (int) ((recordCount + splitSize - 1) / splitSize);
  }

  private synchronized void submitNextSplit(int splitIndex) {
    if (splitIndex >= splitNum) return;

    long start = offset + splitIndex * splitSize;
    long count = Math.min(splitSize, recordCount - (splitIndex * splitSize));

    queues[splitIndex] = new LinkedBlockingQueue<>();
    executor.submit(() -> {
      TunnelRecordReader reader = null;
      try {
        reader = downloadSession.openRecordReader(start, count);
        Record record;
        while ((record = reader.read()) != null) {
          queues[splitIndex].put(record);
        }
        queues[splitIndex].put(EOF_RECORD);
      } catch (Throwable t) {
        error.compareAndSet(null, t);
        queues[splitIndex].offer(EOF_RECORD); // Ensure queue is marked as complete
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ignored) {
          }
        }
      }
    });
  }

  /**
   * Tries to fetch the next record and store it in the `nextRecord` field.
   * This method contains the core logic of blocking and advancing through splits.
   * It's synchronized to protect shared state like `currentSplit`.
   */
  private synchronized void tryToFetchNext() {
    // This loop will continue until a data record is found or all splits are exhausted.
    while (true) {
      checkError();
      if (currentSplit >= splitNum) {
        this.nextRecord = EOF_RECORD;
        return;
      }

      BlockingQueue<Record> currentQueue = queues[currentSplit];
      try {
        // Block and wait for a record from the background thread.
        Record record = currentQueue.take();

        if (record == EOF_RECORD) {
          // End of the current split, let's move to the next one.
          queues[currentSplit] = null; // Allow GC to collect the queue
          submitNextSplit(currentSplit + preloadSplitNum); // Submit a new split to keep the preload window full
          currentSplit++;
          // Continue the loop to fetch from the new current split
          continue;
        } else {
          // We found a valid data record. Store it and exit the method.
          this.nextRecord = record;
          return;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for next record", e);
      }
    }
  }

  /**
   * Checks if there is another record in the iteration.
   * This method is now idempotent. Multiple calls without calling next() will
   * return the same result and not consume any elements.
   *
   * @return {@code true} if the iteration has more elements.
   */
  @Override
  public boolean hasNext() {
    // If we haven't fetched the next record yet (or it was consumed by next()), fetch it.
    if (this.nextRecord == null) {
      tryToFetchNext();
    }
    // The iterator has a next element if the fetched record is not our special EOF marker.
    return this.nextRecord != EOF_RECORD;
  }

  /**
   * Returns the next record in the iteration.
   *
   * @return the next record.
   * @throws NoSuchElementException if the iteration has no more elements.
   */
  @Override
  public Record next() {
    // First, ensure the next record is available and the iterator hasn't reached the end.
    // hasNext() will call tryToFetchNext() if needed.
    if (!hasNext()) {
      throw new NoSuchElementException("No more records.");
    }
    // Get the pre-fetched record.
    Record result = this.nextRecord;
    // VERY IMPORTANT: Clear the pre-fetched record. This signals that the record
    // has been "consumed" and forces the next call to hasNext() to fetch a new one.
    this.nextRecord = null;
    return result;
  }

  private void checkError() {
    if (error.get() != null) {
      throw new RuntimeException("Download failed", error.get());
    }
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }

  public long getSplitSize() {
    return splitSize;
  }

  public int getPreloadSplitNum() {
    return preloadSplitNum;
  }

  public int getThreadNum() {
    return threadNum;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public int getCurrentSplit() {
    return currentSplit;
  }

  private static class EOFRecord implements Record {
    @Override public int getColumnCount() { return 0; }

    @Override
    public Column[] getColumns() {
      return new Column[0];
    }

    @Override
    public boolean isNull(int idx) {
      return false;
    }

    @Override
    public boolean isNull(String columnName) {
      return false;
    }

    @Override
    public void set(int idx, Object value) {
    }
    @Override public Object get(int i) { return null; }

    @Override
    public void set(String columnName, Object value) {
    }

    @Override
    public Object get(String columnName) {
      return null;
    }

    @Override
    public void setBigint(int idx, Long value) {
    }

    @Override
    public Long getBigint(int idx) {
      return 0L;
    }

    @Override
    public void setBigint(String columnName, Long value) {
    }

    @Override
    public Long getBigint(String columnName) {
      return 0L;
    }

    @Override
    public void setDouble(int idx, Double value) {
    }

    @Override
    public Double getDouble(int idx) {
      return 0.0;
    }

    @Override
    public void setDouble(String columnName, Double value) {
    }

    @Override
    public Double getDouble(String columnName) {
      return 0.0;
    }

    @Override
    public void setBoolean(int idx, Boolean value) {
    }

    @Override
    public Boolean getBoolean(int idx) {
      return null;
    }

    @Override
    public void setBoolean(String columnName, Boolean value) {
    }

    @Override
    public Boolean getBoolean(String columnName) {
      return null;
    }

    @Override
    public void setDatetime(int idx, Date value) {
    }

    @Override
    public Date getDatetime(int idx) {
      return null;
    }

    @Override
    public void setDatetime(String columnName, Date value) {
    }

    @Override
    public Date getDatetime(String columnName) {
      return null;
    }

    @Override
    public void setDecimal(int idx, BigDecimal value) {
    }

    @Override
    public BigDecimal getDecimal(int idx) {
      return null;
    }

    @Override
    public void setDecimal(String columnName, BigDecimal value) {
    }

    @Override
    public BigDecimal getDecimal(String columnName) {
      return null;
    }

    @Override
    public void setString(int idx, String value) {
    }

    @Override
    public String getString(int idx) {
      return "";
    }

    @Override
    public void setString(String columnName, String value) {
    }

    @Override
    public String getString(String columnName) {
      return "";
    }

    @Override
    public void setString(int idx, byte[] value) {
    }

    @Override
    public void setString(String columnName, byte[] value) {
    }

    @Override
    public byte[] getBytes(int idx) {
      return new byte[0];
    }

    @Override
    public byte[] getBytes(String columnName) {
      return new byte[0];
    }

    @Override
    public void set(Object[] values) {
    }

    @Override
    public Object[] toArray() {
      return new Object[0];
    }

    @Override
    public Record clone() {
      return null;
    }

    @Override
    public void clear() {
    }
  }
}
