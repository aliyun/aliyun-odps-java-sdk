package com.aliyun.odps.tunnel.io;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.StreamUploadSessionImpl;
import com.aliyun.odps.tunnel.TableTunnel;

import java.io.IOException;

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
  public String flush() throws IOException {
    pack.checkTransConsistency(false);
    pack.complete();
    String id = session.writeBlock(pack);
    pack.reset();
    return id;
  }
}
