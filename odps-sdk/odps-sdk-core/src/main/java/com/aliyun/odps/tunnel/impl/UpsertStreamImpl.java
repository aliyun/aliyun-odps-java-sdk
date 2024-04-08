package com.aliyun.odps.tunnel.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.aliyun.odps.Column;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.TunnelTableSchema;
import com.aliyun.odps.tunnel.hasher.TypeHasher;
import com.aliyun.odps.tunnel.io.Checksum;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.utils.FixedNettyChannelPool;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;

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
  private FixedNettyChannelPool channelPool;
  private long connectTimeout;
  private long readTimeout;

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
    this.hashKeys = session.getHashKeys();
    this.bootstrap = session.getBootstrap();
    this.channelPool = session.getChannelPool();
    this.connectTimeout = session.getConnectTimeout();
    this.readTimeout = session.getReadTimeout();
    this.listener = builder.getListener();

    newBucketBuffer();
  }

  private void newBucketBuffer() throws IOException {
    for (Integer slot : this.buckets.keySet()) {
      this.bucketBuffer.put(slot, new ProtobufRecordPack(this.schema, new Checksum(), 0, new CompressOption()));
    }
  }
  @Override
  public void upsert(Record record) throws IOException, TunnelException {
    write(record, UpsertStreamImpl.Operation.UPSERT, null);
  }

  @Override
  public void upsert(Record record, List<String> upsertCols) throws IOException, TunnelException {
    if (upsertCols != null && !upsertCols.isEmpty() && !session.supportPartialUpdate()) {
      throw new TunnelException(
          "Table " + session.tableName
          + " do not support partial update, consider set table properties 'acid.partial.fields.update.enable=true'");
    }
    if (upsertCols != null && !upsertCols.isEmpty()) {
      Set<String> columnSet =
          schema.getColumns().stream().map(Column::getName).collect(Collectors.toSet());
      upsertCols.forEach((col) -> {
        if (!columnSet.contains(col)) {
          throw new IllegalArgumentException("Invalid column name:" + col);
        }
      });
    }
    write(record, UpsertStreamImpl.Operation.UPSERT, upsertCols);
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
  public void close() throws IOException, TunnelException {
    if (status == Status.NORMAL) {
      flush();
      status = Status.CLOSED;
    }
  }

  @Override
  public void reset() throws IOException {
    if (!bucketBuffer.isEmpty()) {
      for (ProtobufRecordPack pack : bucketBuffer.values()) {
        pack.reset();
      }
    }

    totalBufferSize = 0;
    status = Status.NORMAL;
  }

  private void write(Record record, UpsertStreamImpl.Operation op, List<String> valueColumns)
          throws TunnelException, IOException {
    checkStatus();

    List<Integer> hashValues = new ArrayList<>();
    for (int key : hashKeys) {
      Object value = record.get(key);
      if (value == null) {
        throw new TunnelException(
            " UpsertRecord must have primary key value, consider provide values for column '"
            + schema.getColumn(key).getName() + "'");
      }
      hashValues.add(TypeHasher.hash(schema.getColumn(key).getTypeInfo().getOdpsType(), value,
                                     session.getHasher()));
    }

    int bucket = TypeHasher.CombineHashVal(hashValues) % buckets.size();

    if (!bucketBuffer.containsKey(bucket)) {
      throw new TunnelException(
              "Tunnel internal error! Do not have bucket for hash key " + bucket);
    }

    ProtobufRecordPack pack = bucketBuffer.get(bucket);
    UpsertRecord r = (UpsertRecord) record;
    r.setOperation(op == UpsertStreamImpl.Operation.UPSERT ? (byte)'U' : (byte)'D');
    ArrayList<Long> valueCols = new ArrayList<>();
    if (valueColumns != null) {
      for (String validColumnName : valueColumns) {
        valueCols.add(this.schema.getColumnId(validColumnName));
      }
    }
    r.setValueCols(valueCols);
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
      Channel channel = null;
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
              channel = channelPool.acquire();
              FlushResultHandler handler = new FlushResultHandler(pack, latch, listener, retry);
              channel.pipeline().addLast(handler);
              handlers.add(handler);
              ChannelFuture
                  channelFuture =
                  channel.writeAndFlush(buildFullHttpRequest(request, pack.getProtobufStream()));
              channelFuture.addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                  latch.countDown();
                  channelPool.release(future.channel());
                  handler.setException(
                      new TunnelException("Connect : " + future.cause().getMessage(),
                                          future.cause()));
                  future.channel().close();
                } else {
                  future.channel().pipeline().addFirst(new ReadTimeoutHandler(readTimeout, TimeUnit.MILLISECONDS));
                }
              });
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
          if (listener != null) {
            if (!listener.onFlushFail(handler.getException().getMessage(), retry)) {
              status = Status.ERROR;
              TunnelException e = new TunnelException(handler.getException().getErrorMsg(), handler.getException());
              e.setRequestId(handler.getException().getRequestId());
              e.setErrorCode(handler.getException().getErrorCode());
              throw e;
            }
          } else {
            TunnelException e = new TunnelException(handler.getException().getErrorMsg(), handler.getException());
            e.setRequestId(handler.getException().getRequestId());
            e.setErrorCode(handler.getException().getErrorCode());
            throw e;
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

    public UpsertStream.FlushResult getFlushResult() {
      return flushResult;
    }

    public TunnelException getException() {
      return exception;
    }

    public void setException(TunnelException exception) {
      this.exception = exception;
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
      FullHttpResponse response = null;
      try {
        response = (FullHttpResponse) msg;
        this.flushResult.traceId = response.headers().get(HttpHeaders.HEADER_ODPS_REQUEST_ID);
        if (response.status() == HttpResponseStatus.OK) {
          this.flushResult.flushTime = System.currentTimeMillis() - start;
          pack.reset();
          if (listener != null) {
            try {
              listener.onFlush(flushResult);
            } catch (Exception ignore) {
            }
          }
        } else {
          try (ByteBufInputStream contentStream = new ByteBufInputStream(response.content())) {
            exception = new TunnelException(this.flushResult.traceId, contentStream, response.status().code());
          }
        }
      } catch (Exception e) {
        exception = new TunnelException(e.getMessage(), e);
      } finally {
        latch.countDown();
        if (response != null) {
          response.release();
        }
        channelPool.release(ctx.channel());
        ctx.close();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (cause instanceof ReadTimeoutException) {
        exception = new TunnelException("Flush time out, cannot get response from server");
      } else {
        exception = new TunnelException(cause.getMessage(), cause);
      }
      latch.countDown();
      channelPool.release(ctx.channel());
      ctx.close();
    }
  }
}
