package com.aliyun.odps.tunnel.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.*;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.hasher.OdpsHasher;
import com.aliyun.odps.tunnel.hasher.TypeHasher;
import com.aliyun.odps.tunnel.io.Checksum;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;
import com.aliyun.odps.tunnel.streams.UpsertStream;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;

public class UpsertStreamImpl implements UpsertStream {
  // required
  private long maxBufferSize;
  private long slotBufferSize;
  private final CompressOption compressOption;
  private final URI endpoint;
  private final UpsertSessionImpl session;

  // meta
  private Map<Integer, Slot> buckets;
  private List<Integer> hashKeys = new ArrayList<>();
  private TunnelTableSchema schema;

  // buffer
  private final Map<Integer, ProtobufRecordPack> bucketBuffer = new HashMap<>();
  private long totalBufferSize = 0;

  // netty
  private final Bootstrap bootstrap;
  private CountDownLatch latch;

  // status
  private Status status = Status.NORMAL;

  private Listener listener = null;

  private enum Operation {
    UPSERT,
    DELETE
  }

  private enum Status {
    NORMAL,
    ERROR,
    CLOSED
  }

  public static class Builder implements UpsertStream.Builder {
    private UpsertSessionImpl session;
    private long maxBufferSize = 64 * 1024 * 1024;
    private long slotBufferSize = 1024 * 1024;
    private CompressOption compressOption = new CompressOption();
    private Listener listener = null;

    public Builder setSession(UpsertSessionImpl session) {
      this.session = session;
      return this;
    }

    public UpsertSessionImpl getSession() {
      return session;
    }

    public long getMaxBufferSize() {
      return maxBufferSize;
    }

    public Builder setMaxBufferSize(long maxBufferSize) {
      this.maxBufferSize = maxBufferSize;
      return this;
    }

    public long getSlotBufferSize() {
      return slotBufferSize;
    }

    public Builder setSlotBufferSize(long slotBufferSize) {
      this.slotBufferSize = slotBufferSize;
      return this;
    }

    @Override
    public CompressOption getCompressOption() {
      return compressOption;
    }

    @Override
    public Builder setCompressOption(CompressOption compressOption) {
      this.compressOption = compressOption;
      return this;
    }

    @Override
    public Listener getListener() {
      return listener;
    }

    @Override
    public Builder setListener(Listener listener) {
      this.listener = listener;
      return this;
    }

    @Override
    public UpsertStream build() throws IOException, TunnelException {
      return new UpsertStreamImpl(this);
    }
  }

  public UpsertStreamImpl(Builder builder) throws IOException, TunnelException {
    this.compressOption = builder.getCompressOption();
    this.slotBufferSize = builder.getSlotBufferSize();
    this.maxBufferSize = builder.getMaxBufferSize();
    this.session = builder.session;
    this.endpoint = session.getEndpoint();
    this.buckets = session.getBuckets();
    this.schema = session.getRecordSchema();
    for (Integer slot : buckets.keySet()) {
      this.bucketBuffer.put(slot, new ProtobufRecordPack(schema, new Checksum(), 0, new CompressOption()));
    }
    this.hashKeys = session.getHashKeys();
    this.bootstrap = session.getBootstrap();
    this.listener = builder.getListener();
  }

  @Override
  public void upsert(Record record) throws IOException, TunnelException {
    write(record, UpsertStreamImpl.Operation.UPSERT, null);
  }

  @Override
  public void upsert(Record record, List<String> validColumns) throws IOException, TunnelException {
    write(record, UpsertStreamImpl.Operation.UPSERT, validColumns);
  }

  @Override
  public void delete(Record record) throws IOException, TunnelException {
    write(record, UpsertStreamImpl.Operation.DELETE, null);
  }

  @Override
  public void flush() throws IOException, TunnelException {
    flush(true);
  }

  @Override
  public void close() {
    status = Status.CLOSED;
  }

  private void write(Record record, UpsertStreamImpl.Operation op, List<String> validColumns)
          throws TunnelException, IOException {
    checkStatus();

    List<Integer> hashValues = new ArrayList<>();
    for (int key : hashKeys) {
      Object value = record.get(key);
      if (value == null) {
        throw new TunnelException("Hash key " + key + " can not be null!");
      }
      OdpsHasher hasher = TypeHasher.getHasher(schema.getColumn(key)
                                                     .getTypeInfo()
                                                     .getTypeName()
                                                     .toLowerCase(),
                                               session.getHasher());
      hashValues.add(hasher.hash(value));
    }

    int bucket = TypeHasher.CombineHashVal(hashValues) % buckets.size();

    if (!bucketBuffer.containsKey(bucket)) {
      throw new TunnelException(
              "Tunnel internal error! Do not have bucket for hash key " + bucket);
    }

    ProtobufRecordPack pack = bucketBuffer.get(bucket);
    UpsertRecord r = (UpsertRecord) record;
    r.setOperation(op == UpsertStreamImpl.Operation.UPSERT ? (byte)'U' : (byte)'D');
    r.setValueCols(validColumns == null ?
                     new ArrayList<>() :
                     validColumns.stream().map(this.schema::getColumnIndex).collect(Collectors.toList()));

    long bytes = pack.getTotalBytes();
    pack.append(r.getRecord());
    bytes = pack.getTotalBytes() - bytes;
    totalBufferSize += bytes;
    if (pack.getTotalBytes() > slotBufferSize) {
      flush(false);
    } else if (totalBufferSize > maxBufferSize) {
      flush(true);
    }
  }

  private void flush(boolean flushAll) throws TunnelException, IOException {
    List<FlushResultHandler> handlers = new ArrayList<>();
    boolean success;
    int retry = 0;

    // update slot map
    Map<Integer, Slot> bucketMap = session.getBuckets();
    if (bucketMap.size() != buckets.size()) {
      throw new TunnelException("session slot map is changed");
    } else {
      buckets = bucketMap;
    }

    do {
      success = true;
      handlers.clear();
      try {
        checkStatus();
        latch = new CountDownLatch(bucketBuffer.size());
        for (Map.Entry<Integer, ProtobufRecordPack> entry : bucketBuffer.entrySet()) {
          ProtobufRecordPack pack = entry.getValue();
          if (pack.getSize() > 0) {
            if (pack.getTotalBytes() > slotBufferSize || flushAll) {
              int k = entry.getKey();
              long bytes = pack.getTotalBytes();
              pack.checkTransConsistency(false);
              pack.complete();
              bytes = pack.getTotalBytes() - bytes;
              if (!flushAll) {
                totalBufferSize += bytes;
              }
              Request request = session.buildRequest("PUT", k, buckets.get(k), pack.getTotalBytes(), pack.getSize(), compressOption);
              String host = request.getURI().getHost();
              int port = request.getURI().getPort();
              FlushResultHandler handler = new FlushResultHandler(pack, latch, listener, retry);
              Channel channel = bootstrap.connect(host, port).sync().channel();
              channel.pipeline().addLast(handler);
              handlers.add(handler);
              channel.writeAndFlush(buildFullHttpRequest(request, pack.getProtobufStream()));
            } else {
              latch.countDown();
            }
          } else {
            latch.countDown();
          }
        }
        latch.await();
      } catch (InterruptedException e) {
        throw new TunnelException("flush interrupted", e);
      }

      for (FlushResultHandler handler : handlers) {
        if (handler.getException() != null) {
          success = false;
          if (!handler.isNeedRetry()) {
            status = Status.ERROR;
            throw handler.getException();
          }
        } else {
          if (!flushAll) {
            totalBufferSize -= handler.getFlushResult().flushSize;
          }
        }
      }
      ++retry;
    } while (!success);
    if (flushAll) {
      totalBufferSize = 0;
    }
  }

  private void checkStatus() throws TunnelException {
    if (Status.CLOSED == status) {
      throw new TunnelException("Stream is closed!");
    } else if (Status.ERROR == status) {
      throw new TunnelException("Stream has error!");
    }
  }

  private HttpRequest buildFullHttpRequest(Request request, ByteArrayOutputStream content) {
    String uri = request.getURI().toString().replace(endpoint.toString(), "");
    HttpRequest req = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.PUT, uri, Unpooled.wrappedBuffer(content.toByteArray()));
    request.getHeaders().forEach((key, value) -> req.headers().set(key, value));
    req.headers().set(HttpHeaderNames.HOST, request.getURI().getHost());
    return req;
  }

  private class FlushResultHandler extends ChannelInboundHandlerAdapter {

    private UpsertStream.FlushResult flushResult = new UpsertStream.FlushResult();
    private ProtobufRecordPack pack;
    private TunnelException exception = null;
    CountDownLatch latch;
    long start;
    Listener listener;
    int retry;
    boolean needRetry = false;

    public UpsertStream.FlushResult getFlushResult() {
      return flushResult;
    }

    public TunnelException getException() {
      return exception;
    }

    public boolean isNeedRetry() {
      return needRetry;
    }

    FlushResultHandler(ProtobufRecordPack pack, CountDownLatch latch, Listener listener, int retry) {
      this.flushResult.recordCount = pack.getSize();
      this.pack = pack;
      this.flushResult.flushSize = pack.getTotalBytes();
      this.latch = latch;
      this.start = System.currentTimeMillis();
      this.listener = listener;
      this.retry = retry;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      try {

        FullHttpResponse response = (FullHttpResponse) msg;
        this.flushResult.traceId = response.headers().get(HttpHeaders.HEADER_ODPS_REQUEST_ID);
        if (response.status() == HttpResponseStatus.OK) {
          pack.reset();
          if (listener != null) {
            try {
              listener.onFlush(flushResult);
            } catch (Exception ignore) {}
          }
        } else {
          exception = new TunnelException(this.flushResult.traceId,
                  new ByteBufInputStream(response.content()),
                  response.status().code());
          if (listener != null) {
            try {
              listener.onFlushFail(exception.getMessage(), retry);
            } catch (Exception ignore) {}
          }
        }
      } catch (Exception e) {
        exception = new TunnelException(e.getMessage(), e);
        try {
          needRetry = listener.onFlushFail(e.getMessage(), retry);
        } catch (Exception ignore) {}
      } finally {
        latch.countDown();
        ctx.close();
        this.flushResult.flushTime = System.currentTimeMillis() - start;
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      exception = new TunnelException(cause.getMessage(), cause);
      latch.countDown();
      ctx.close();
      this.flushResult.flushTime = System.currentTimeMillis() - start;
    }
  }
}
