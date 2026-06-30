package com.aliyun.odps.tunnel.impl;

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_ROUTED_SERVER;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.aliyun.odps.Column;
import com.aliyun.odps.commons.transport.HttpStatus;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.TunnelTableSchema;
import com.aliyun.odps.tunnel.hasher.DecimalHashObject;
import com.aliyun.odps.tunnel.hasher.TypeHasher;
import com.aliyun.odps.tunnel.io.Checksum;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;
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
  private Map<Integer, ProtobufRecordPack> writeBuffer;
  private Map<Integer, ProtobufRecordPack> flushBuffer; // For async mode
  private long totalWriteBufferSize = 0;
  private ExecutorService asyncFlushService; // For async mode
  private Future<?> flushFuture; // For async mode

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
    private ExecutorService asyncFlushService = null;

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
    public Builder setAsyncFlushService(ExecutorService service) {
      this.asyncFlushService = service;
      return this;
    }

    public ExecutorService getAsyncFlushService() {
      return this.asyncFlushService;
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

    this.asyncFlushService = builder.getAsyncFlushService();
    this.writeBuffer = createNewBucketBuffer();

    if (this.asyncFlushService != null) {
      this.flushBuffer = createNewBucketBuffer();
    }
  }

  private Map<Integer, ProtobufRecordPack> createNewBucketBuffer() throws IOException {
    Map<Integer, ProtobufRecordPack> newBuffer = new HashMap<>();
    for (Integer slot : this.buckets.keySet()) {
      newBuffer.put(slot, new ProtobufRecordPack(this.schema, new Checksum(), 0, compressOption));
    }
    return newBuffer;
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
    // flush() is now a convenience method for a full, blocking sync
    sync();
  }

  public void sync() throws IOException, TunnelException {
    if (asyncFlushService != null) {
      // For async mode, trigger a blocking, full flush
      asyncFlush(true);
    } else {
      // For sync mode, trigger a standard full flush
      syncFlush(true);
    }
  }

  @Override
  public void close() throws IOException, TunnelException {
    if (status == Status.NORMAL) {
      sync();
      status = Status.CLOSED;
    }
  }

  @Override
  public void reset() throws IOException {
    try {
      waitUntilFlushFinish();
    } catch (TunnelException e) {
      throw new IOException("The latest async flush is failed. " + e.getMessage(), e);
    }
    if (writeBuffer != null) {
      for (ProtobufRecordPack pack : writeBuffer.values()) {
        pack.reset();
      }
    }
    if (flushBuffer != null) {
      for (ProtobufRecordPack pack : flushBuffer.values()) {
        pack.reset();
      }
    }

    totalWriteBufferSize = 0;
    status = Status.NORMAL;
  }

  private void write(Record record, UpsertStreamImpl.Operation op, List<String> valueColumns)
          throws TunnelException, IOException {
    checkStatusAndError();

    List<Integer> hashValues = new ArrayList<>();
    for (int key : hashKeys) {
      Object value = record.get(key);
      if (value == null) {
        throw new TunnelException(
            " UpsertRecord must have primary key value, consider provide values for column '"
            + schema.getColumn(key).getName() + "'");
      }
      TypeInfo typeInfo = schema.getColumn(key).getTypeInfo();

      // java type BigDecimal's precision and scale may be different from typeInfo,
      // so here converted to DecimalHashObject
      if (typeInfo.getOdpsType() == OdpsType.DECIMAL) {
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
        value =
            new DecimalHashObject((BigDecimal) value, decimalTypeInfo.getPrecision(),
                                  decimalTypeInfo.getScale());
      }
      hashValues.add(TypeHasher.hash(typeInfo.getOdpsType(), value, session.getHasher()));
    }

    int bucket = TypeHasher.CombineHashVal(hashValues) % buckets.size();

    if (!writeBuffer.containsKey(bucket)) {
      throw new TunnelException(
              "Tunnel internal error! Do not have bucket for hash key " + bucket);
    }

    ProtobufRecordPack pack = writeBuffer.get(bucket);
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
    totalWriteBufferSize += bytes;

    boolean isSlotFull = pack.getTotalBytes() > slotBufferSize;
    boolean isTotalFull = totalWriteBufferSize > maxBufferSize;

    if (isSlotFull || isTotalFull) {
      boolean flushAll = isTotalFull; // If total is full, flush everything
      if (asyncFlushService != null) {
        // In async mode, any flush trigger results in a full flush of the buffer.
        asyncFlush(false);
      } else {
        syncFlush(flushAll);
      }
    }
  }

  /**
   * Triggers a synchronous flush. Blocks until completion.
   */
  private void syncFlush(boolean flushAll) throws IOException, TunnelException {
    flushInternal(this.writeBuffer, flushAll);
  }

  /**
   * Triggers an asynchronous flush. Manages buffer swapping and task submission.
   */
  private synchronized void asyncFlush(boolean blocking)
    throws IOException, TunnelException {
    try {
      // Wait for the previous flush to finish. This provides backpressure.
      waitUntilFlushFinish();

      if (totalWriteBufferSize <= 0) {
        // Check if there's any data in writeBuffer (getSize() is more reliable than getTotalBytes())
        // because getTotalBytes() may not reflect buffered data immediately after append
        boolean hasData = writeBuffer.values().stream().anyMatch(p -> p.getSize() > 0);
        if (!hasData) {
          return;
        }
      }

      // Swap buffers
      Map<Integer, ProtobufRecordPack> temp = flushBuffer;
      flushBuffer = writeBuffer;
      writeBuffer = temp;

      if (writeBuffer == null) {
        writeBuffer = createNewBucketBuffer();
      }
      
      // After swapping, the new write buffer is empty, so reset the counter.
      totalWriteBufferSize = 0;

      flushFuture =
        asyncFlushService.submit(
          () -> {
            flushInternal(flushBuffer, true);
            return null;
          });

      if (blocking) {
        waitUntilFlushFinish();
      }
    } catch (IOException | TunnelException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Asynchronous flush operation failed to start.", e);
    }
  }

  private void flushInternal(Map<Integer, ProtobufRecordPack> bufferToFlush,
                             boolean flushAll) throws TunnelException, IOException {
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
        checkStatusAndError();
        latch = new CountDownLatch(bufferToFlush.size());
        for (Map.Entry<Integer, ProtobufRecordPack> entry : bufferToFlush.entrySet()) {
          ProtobufRecordPack pack = entry.getValue();
          if (pack.getSize() > 0) {
            if (pack.getTotalBytes() > slotBufferSize || flushAll) {
              int bucketId = entry.getKey();
              long bytes = pack.getTotalBytes();
              pack.checkTransConsistency(false);
              pack.complete();
              bytes = pack.getTotalBytes() - bytes;
              if (!flushAll) {
                totalWriteBufferSize += bytes;
              }
              Request request = session.buildRequest("PUT", bucketId, buckets.get(bucketId), pack.getTotalBytes(), pack.getSize(), compressOption);
              channel = channelPool.acquire();
              FlushResultHandler handler = new FlushResultHandler(pack, latch, listener, retry, bucketId);
              channel.pipeline().addLast(handler);
              handlers.add(handler);
              HttpRequest httpRequest = buildFullHttpRequest(request, pack.getProtobufStream());
              handler.startWriteTimeout(channel);
              ChannelFuture
                  channelFuture =
                  channel.writeAndFlush(httpRequest);
              channelFuture.addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                  handler.failAndClose(
                      future.channel(),
                      new TunnelException("Connect : " + future.cause().getMessage(),
                                          future.cause()));
                } else {
                  handler.cancelWriteTimeout();
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
            if (!listener.onFlushFail(handler.getException(), retry)) {
              status = Status.ERROR;
              TunnelException e = new TunnelException(handler.getException().getErrorMsg(), handler.getException());
              e.setRequestId(handler.getException().getRequestId());
              e.setStatus(handler.getException().getStatus());
              e.setErrorCode(handler.getException().getErrorCode());
              throw e;
            }
          } else {
            TunnelException e = new TunnelException(handler.getException().getErrorMsg(), handler.getException());
            e.setRequestId(handler.getException().getRequestId());
            e.setStatus(handler.getException().getStatus());
            e.setErrorCode(handler.getException().getErrorCode());
            throw e;
          }
        } else {
          if (!flushAll) {
            totalWriteBufferSize -= handler.getFlushResult().flushSize;
          }
        }
      }
      ++retry;
    } while (!success);
    if (flushAll) {
      totalWriteBufferSize = 0;
    }
  }

  private void waitUntilFlushFinish() throws IOException, TunnelException {
    if (flushFuture != null) {
      try {
        flushFuture.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Flush wait was interrupted.", e);
      } catch (ExecutionException e) {
        // Exception from the background thread. Unwrap and rethrow.
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else if (cause instanceof TunnelException) {
          throw (TunnelException) cause;
        } else {
          throw new IOException("Async flush failed in background.", cause);
        }
      } finally {
        flushFuture = null;
      }
    }
  }

  private void checkStatusAndError() throws TunnelException, IOException {
    if (Status.CLOSED == status) {
      throw new TunnelException("Stream is closed!");
    } else if (Status.ERROR == status) {
      throw new TunnelException("Stream has error!");
    }
    if (flushFuture != null && flushFuture.isDone()) {
      waitUntilFlushFinish();
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
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private ScheduledFuture<?> writeTimeoutFuture;
    CountDownLatch latch;
    long start;
    Listener listener;
    int retry;
    int bucketId;

    public UpsertStream.FlushResult getFlushResult() {
      return flushResult;
    }

    public TunnelException getException() {
      return exception;
    }

    public void setException(TunnelException exception) {
      this.exception = exception;
    }

    FlushResultHandler(ProtobufRecordPack pack, CountDownLatch latch, Listener listener, int retry, int bucketId) {
      this.flushResult.recordCount = pack.getSize();
      this.pack = pack;
      this.flushResult.flushSize = pack.getTotalBytes();
      this.latch = latch;
      this.start = System.currentTimeMillis();
      this.listener = listener;
      this.retry = retry;
      this.bucketId = bucketId;
    }

    void startWriteTimeout(Channel channel) {
      writeTimeoutFuture = channel.eventLoop().schedule(() -> failAndClose(
          channel, newRetryableTimeoutException(
              TunnelConstants.UPSERT_FLUSH_WRITE_TIMEOUT,
              "Flush write timed out after %d ms while sending request body to server "
              + "(bucket=%d, records=%d, bytes=%d). The server may have stopped reading the "
              + "request body or the connection may be stalled.",
              readTimeout, bucketId, flushResult.recordCount, flushResult.flushSize)),
          readTimeout, TimeUnit.MILLISECONDS);
    }

    private void cancelWriteTimeout() {
      if (writeTimeoutFuture != null) {
        writeTimeoutFuture.cancel(false);
      }
    }

    void failAndClose(Channel channel, TunnelException e) {
      cancelWriteTimeout();
      if (completed.compareAndSet(false, true)) {
        exception = e;
        latch.countDown();
        channelPool.release(channel);
        channel.close();
      }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      FullHttpResponse response = null;
      TunnelException resultException = null;
      try {
        response = (FullHttpResponse) msg;
        this.flushResult.traceId = response.headers().get(HttpHeaders.HEADER_ODPS_REQUEST_ID);
        if (response.status().equals(HttpResponseStatus.OK)) {
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
            resultException = new TunnelException(
                this.flushResult.traceId, contentStream, response.status().code());

            // 308 means should update slot map and retry
            if (response.status().code() == HttpStatus.SLOT_REASSIGNMENT) {
              if (response.headers().contains(HEADER_ODPS_ROUTED_SERVER)) {
                String newSlotServer = response.headers().get(HEADER_ODPS_ROUTED_SERVER);
                session.updateBuckets(bucketId, newSlotServer);
              } else {
                session.updateBuckets(bucketId, null);
              }
              buckets = session.getBuckets();
            }
          }
        }
      } catch (Exception e) {
        resultException = new TunnelException(e.getMessage(), e);
      } finally {
        cancelWriteTimeout();
        if (completed.compareAndSet(false, true)) {
          exception = resultException;
          latch.countDown();
          if (response != null) {
            response.release();
          }
          channelPool.release(ctx.channel());
          ctx.close();
        } else if (response != null) {
          response.release();
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (cause instanceof ReadTimeoutException) {
        failAndClose(
            ctx.channel(), newRetryableTimeoutException(
                TunnelConstants.UPSERT_FLUSH_RESPONSE_TIMEOUT,
                "Flush response timed out after %d ms while waiting for server response "
                + "(bucket=%d, records=%d, bytes=%d). The server may have stopped sending "
                + "the response or declared a larger Content-Length than it delivered.",
                readTimeout, bucketId, flushResult.recordCount, flushResult.flushSize));
      } else {
        failAndClose(ctx.channel(), new TunnelException(cause.getMessage(), cause));
      }
    }

    private TunnelException newRetryableTimeoutException(String errorCode,
                                                         String messageTemplate,
                                                         Object... args) {
      TunnelException exception = new TunnelException(String.format(messageTemplate, args));
      exception.setStatus(HttpStatus.INTERNAL_SERVER_ERROR);
      exception.setErrorCode(errorCode);
      return exception;
    }
  }
}
