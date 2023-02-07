package com.aliyun.odps.tunnel.io;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.impl.StreamUploadSessionImpl;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

import java.io.IOException;

class FlushResultImpl implements TableTunnel.FlushResult {
  private String traceId;
  private long flushSize;
  private long recordCount;

  public FlushResultImpl(String traceId, long flushSize, long recordCount) {
    this.traceId = traceId;
    this.flushSize = flushSize;
    this.recordCount = recordCount;
  }

  @Override
  public String getTraceId() {
    return traceId;
  }

  @Override
  public long getFlushSize() {
    return flushSize;
  }

  @Override
  public long getRecordCount() {
    return recordCount;
  }
}

public class StreamRecordPackImpl implements TableTunnel.StreamRecordPack {
  private ProtobufRecordPack pack;
  private StreamUploadSessionImpl session;
  private boolean flushing = false;

  public StreamRecordPackImpl(StreamUploadSessionImpl session, CompressOption option) throws IOException {
    this.session = session;
    this.pack = new ProtobufRecordPack(session.getSchema(), new Checksum(), 0, option);
  }

  @Override
  public void append(Record record) throws IOException {
    if (flushing) {
      throw new IOException(new TunnelException("There's an unsuccessful flush called, you should call flush to retry or call reset to drop the data"));
    }
    pack.append(record);
  }

  @Override
  public long getRecordCount() {
    return pack.getSize();
  }

  @Override
  public long getDataSize() {
    return pack.getTotalBytes();
  }

  @Override
  public String flush() throws IOException {
      return flush(new TableTunnel.FlushOption()).getTraceId();
  }

  @Override
  public TableTunnel.FlushResult flush(TableTunnel.FlushOption opt) throws IOException {
    flushing = true;
    long recordCount = pack.getSize();
    pack.checkTransConsistency(false);
    pack.complete();
    String id = session.writeBlock(pack, opt.getTimeout());
    long size = pack.getTotalBytes();
    reset();
    return new FlushResultImpl(id, size, recordCount);
  }

  @Override
  public void reset() throws IOException {
    pack.reset();
    flushing = false;
  }
}
