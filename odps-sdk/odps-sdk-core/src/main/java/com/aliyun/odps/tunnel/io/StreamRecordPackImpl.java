package com.aliyun.odps.tunnel.io;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.StreamUploadSessionImpl;
import com.aliyun.odps.tunnel.TableTunnel;

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

  public StreamRecordPackImpl(StreamUploadSessionImpl session, CompressOption option) throws IOException {
    this.session = session;
    this.pack = new ProtobufRecordPack(session.getSchema(), new Checksum(), 0, option);
  }

  @Override
  public void append(Record record) throws IOException {
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
  public TableTunnel.FlushResult flush() throws IOException {
    long recordCount = pack.getSize();
    pack.checkTransConsistency(false);
    pack.complete();
    String id = session.writeBlock(pack);
    long size = pack.getTotalBytes();
    pack.reset();
    return new FlushResultImpl(id, size, recordCount);
  }
}
