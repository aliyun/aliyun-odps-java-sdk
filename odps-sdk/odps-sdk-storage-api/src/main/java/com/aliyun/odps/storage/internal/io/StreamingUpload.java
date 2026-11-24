/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.storage.internal.io;

import com.aliyun.odps.storage.internal.models.HttpResponse;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.Buffer;
import okio.BufferedSink;

/** One-shot upload with bounded buffering, backpressure and acknowledged network flushes. */
public final class StreamingUpload extends RequestBody implements AutoCloseable {
  private static final int DEFAULT_BUFFER_BYTES = 256 * 1024;
  private final Buffer pending = new Buffer();
  private final Object bufferLock = new Object();
  private final Object producerLock = new Object();
  private final Object failureLock = new Object();
  private final int chunkSize;
  private int bufferSize; // Guarded by bufferLock.
  private final long writeTimeoutMillis;
  private final CompletableFuture<HttpResponse> response = new CompletableFuture<>();
  private final AtomicReference<IOException> failure = new AtomicReference<>();
  private final AtomicReference<CompletableFuture<Void>> flush = new AtomicReference<>();
  private volatile Call call;
  private volatile Thread worker;
  private volatile boolean outputClosed;
  private boolean written; // Guarded by bufferLock.

  public StreamingUpload(int chunkSize, long writeTimeoutMillis) {
    if (chunkSize <= 0 || writeTimeoutMillis < 0) {
      throw new IllegalArgumentException("Invalid upload buffer size or timeout");
    }
    this.chunkSize = chunkSize;
    this.bufferSize = Math.min(chunkSize, DEFAULT_BUFFER_BYTES);
    this.writeTimeoutMillis = writeTimeoutMillis;
  }

  public int getChunkSize() {
    return chunkSize;
  }

  /** Configures the single aggregation stage before the caller writes any bytes. */
  public void configureBufferSize(int outputBufferSize) throws IOException {
    if (outputBufferSize < 0) {
      throw new IllegalArgumentException("Invalid upload buffer size");
    }
    synchronized (producerLock) {
      synchronized (bufferLock) {
        checkFailure();
        if (written || outputClosed) {
          throw new IllegalStateException("Upload buffering must be configured before writing");
        }
        bufferSize = Math.min(chunkSize, Math.max(8192, outputBufferSize));
      }
    }
  }

  public void start(Call call, Callable<HttpResponse> action) {
    synchronized (failureLock) {
      this.call = call;
      if (failure.get() != null) {
        call.cancel();
        return;
      }
    }
    // A live block must not queue behind another block waiting for its caller to close it.
    worker =
        new Thread(
            () -> {
              try {
                response.complete(action.call());
              } catch (Throwable e) {
                fail(e instanceof IOException ? (IOException) e : new IOException(e));
                if (e instanceof Error) {
                  throw (Error) e;
                }
              }
            },
            "odps-block-upload");
    worker.setDaemon(true);
    worker.start();
  }

  @Override
  public MediaType contentType() {
    return MediaType.get("application/vnd.apache.arrow.stream");
  }

  @Override
  public boolean isOneShot() {
    return true;
  }

  @Override
  public void writeTo(BufferedSink sink) throws IOException {
    Buffer batch = new Buffer();
    try {
      // Send the HTTP headers while the producer is filling the first body batch.
      checkFailure();
      sink.flush();
      while (true) {
        long bytes;
        CompletableFuture<Void> barrier;
        boolean end;
        synchronized (bufferLock) {
          while (pending.size() < bufferSize && flush.get() == null && !outputClosed) {
            checkFailure();
            bufferLock.wait();
          }
          checkFailure();
          bytes = pending.size();
          if (bytes > 0) {
            // Transfer segment ownership instead of allocating/copying another byte array.
            batch.write(pending, bytes);
          }
          barrier = bytes == 0 ? flush.get() : null;
          end = bytes == 0 && outputClosed && barrier == null;
          bufferLock.notifyAll();
        }
        if (bytes > 0) {
          sink.write(batch, bytes);
          continue;
        }
        sink.flush();
        checkFailure();
        if (barrier != null) {
          barrier.complete(null);
          flush.compareAndSet(barrier, null);
        } else if (end) {
          return;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IOException error = interrupted(e);
      fail(error);
      throw error;
    } catch (IOException e) {
      fail(e);
      throw e;
    } finally {
      batch.clear();
    }
  }

  public OutputStream outputStream() {
    return new OutputStream() {
      @Override
      public void write(int value) throws IOException {
        write(new byte[] {(byte) value}, 0, 1);
      }

      @Override
      public void write(byte[] data, int offset, int length) throws IOException {
        if (offset < 0 || length < 0 || offset > data.length - length) {
          throw new IndexOutOfBoundsException();
        }
        synchronized (producerLock) {
          ensureWritable();
          try {
            while (length > 0) {
              synchronized (bufferLock) {
                waitForSpace();
                int size = (int) Math.min(length, bufferSize - pending.size());
                pending.write(data, offset, size);
                written = true;
                offset += size;
                length -= size;
                bufferLock.notifyAll();
              }
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            IOException error = interrupted(e);
            fail(error);
            throw error;
          } catch (IOException e) {
            fail(e);
            throw e;
          }
        }
      }

      @Override
      public void flush() throws IOException {
        synchronized (producerLock) {
          ensureWritable();
          CompletableFuture<Void> barrier = new CompletableFuture<>();
          synchronized (bufferLock) {
            flush.set(barrier);
            bufferLock.notifyAll();
          }
          try {
            await(barrier, writeTimeoutMillis);
          } finally {
            flush.compareAndSet(barrier, null);
          }
        }
      }

      @Override
      public void close() throws IOException {
        synchronized (producerLock) {
          checkFailure();
          synchronized (bufferLock) {
            outputClosed = true;
            bufferLock.notifyAll();
          }
        }
      }
    };
  }

  public HttpResponse finish() throws IOException {
    outputStream().close();
    // OkHttp enforces the configured read timeout once the request has been sent.
    return await(response, 0);
  }

  @Override
  public void close() {
    fail(new IOException("Block upload aborted"));
  }

  private void ensureWritable() throws IOException {
    checkFailure();
    if (outputClosed) {
      throw new IOException("Block upload is closed");
    }
  }

  // Called with bufferLock held. The timeout bounds a wait without producer progress.
  private void waitForSpace() throws IOException, InterruptedException {
    long start = System.nanoTime();
    long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(writeTimeoutMillis);
    while (pending.size() == bufferSize) {
      ensureWritable();
      if (writeTimeoutMillis == 0) {
        bufferLock.wait();
      } else {
        long remaining = timeoutNanos - (System.nanoTime() - start);
        if (remaining <= 0) {
          throw new InterruptedIOException("Block upload write timed out");
        }
        TimeUnit.NANOSECONDS.timedWait(bufferLock, remaining);
      }
    }
    ensureWritable();
  }

  private <T> T await(CompletableFuture<T> future, long timeoutMillis) throws IOException {
    try {
      checkFailure();
      return timeoutMillis == 0 ? future.get() : future.get(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IOException error = interrupted(e);
      fail(error);
      throw error;
    } catch (TimeoutException e) {
      IOException error = new InterruptedIOException("Block upload flush timed out");
      fail(error);
      throw error;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      throw cause instanceof IOException ? (IOException) cause : new IOException(cause);
    }
  }

  private void checkFailure() throws IOException {
    synchronized (failureLock) {
      IOException error = failure.get();
      if (error != null) {
        throw error;
      }
    }
  }

  private void fail(IOException error) {
    synchronized (failureLock) {
      if (!failure.compareAndSet(null, error)) {
        return;
      }
      // No caller may observe this failure until cancellation has closed the socket. The
      // cancellation listener can reenter close(); it preserves the first failure above.
      if (call != null) {
        call.cancel();
      }
    }
    // Never acquire bufferLock while holding failureLock: the consumer checks failure under it.
    synchronized (bufferLock) {
      pending.clear();
      bufferLock.notifyAll();
    }
    Thread thread = worker;
    if (thread != null && thread != Thread.currentThread()) {
      thread.interrupt();
    }
    CompletableFuture<Void> barrier = flush.get();
    if (barrier != null) {
      barrier.completeExceptionally(error);
    }
    response.completeExceptionally(error);
  }

  private static IOException interrupted(InterruptedException e) {
    InterruptedIOException error = new InterruptedIOException("Block upload interrupted");
    error.initCause(e);
    return error;
  }
}
