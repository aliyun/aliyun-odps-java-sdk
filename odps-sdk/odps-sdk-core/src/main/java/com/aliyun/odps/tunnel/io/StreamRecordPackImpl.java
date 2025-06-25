package com.aliyun.odps.tunnel.io;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.TunnelMetrics;
import com.aliyun.odps.tunnel.impl.StreamUploadSessionImpl;

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
    long startTime = System.currentTimeMillis();
    pack.append(record);
    pack.addLocalWallTimeMs(System.currentTimeMillis() - startTime);
  }

  @Override
  public long getRecordCount() {
    return pack.getSize();
  }

  @Override
  public long getDataSize() {
    return pack.getTotalBytes();
  }

  public TunnelMetrics getMetrics() {
    return pack.getMetrics();
  }

  @Override
  public String flush() throws IOException {
      return flush(new TableTunnel.FlushOption()).getTraceId();
  }

  @Override
  public TableTunnel.FlushResult flush(TableTunnel.FlushOption opt) throws IOException {
    return flush(opt, null);
  }

  public TableTunnel.FlushResult flush(TableTunnel.FlushOption opt, String partitionSpec) throws IOException {
    flushing = true;
    long recordCount = pack.getSize();
    pack.checkTransConsistency(false);
    pack.complete();
    StreamUploadSessionImpl.WriteResult result = session.writeBlock(pack, opt.getTimeout(), partitionSpec);
    long size = pack.getTotalBytes();
    reset();
    return new FlushResultImpl(result, size, recordCount);
  }

  @Override
  public void reset() throws IOException {
    pack.reset();
    flushing = false;
  }
}
