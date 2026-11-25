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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import javax.net.SocketFactory;
import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.EventListener;

/** Bounded socket aggregation for the dedicated cleartext HTTP/1.1 block-upload client only. */
public final class UploadSocketFactory extends SocketFactory {
  private static final int MAX_BUFFER_BYTES = 4 * 1024 * 1024;

  public static EventListener.Factory listenerFactory() {
    return call ->
        new EventListener() {
          @Override
          public void canceled(Call call) {
            if (call.request().body() instanceof StreamingUpload) {
              // Call.cancel() alone does not wake a request body waiting for its producer.
              ((StreamingUpload) call.request().body()).close();
            }
          }

          @Override
          public void connectionAcquired(Call call, Connection connection) {
            Socket socket = connection.socket();
            if (socket instanceof UploadSocket
                && call.request().body() instanceof StreamingUpload) {
              ((UploadSocket) socket)
                  .configure(call, ((StreamingUpload) call.request().body()).getChunkSize());
            }
          }

          @Override
          public void connectionReleased(Call call, Connection connection) {
            Socket socket = connection.socket();
            if (socket instanceof UploadSocket) {
              ((UploadSocket) socket).release(call);
            }
          }
        };
  }

  @Override
  public Socket createSocket() {
    return new UploadSocket();
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    return connect(new InetSocketAddress(host, port), null);
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    return connect(new InetSocketAddress(host, port), null);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress local, int localPort)
      throws IOException {
    return connect(new InetSocketAddress(host, port), new InetSocketAddress(local, localPort));
  }

  @Override
  public Socket createSocket(InetAddress host, int port, InetAddress local, int localPort)
      throws IOException {
    return connect(new InetSocketAddress(host, port), new InetSocketAddress(local, localPort));
  }

  private Socket connect(InetSocketAddress remote, InetSocketAddress local) throws IOException {
    Socket socket = createSocket();
    try {
      if (local != null) {
        socket.bind(local);
      }
      socket.connect(remote);
      return socket;
    } catch (IOException | RuntimeException e) {
      try {
        socket.close();
      } catch (IOException closeError) {
        e.addSuppressed(closeError);
      }
      throw e;
    }
  }

  private static final class UploadSocket extends Socket {
    private AggregatingOutputStream output;
    private int bufferSize;
    private Call owner;

    @Override
    public synchronized OutputStream getOutputStream() throws IOException {
      if (output == null) {
        output = new AggregatingOutputStream(super.getOutputStream());
        output.configure(bufferSize);
      }
      return output;
    }

    synchronized void configure(Call call, int chunkSize) {
      bufferSize = Math.min(chunkSize, MAX_BUFFER_BYTES);
      if (output != null) {
        output.configure(bufferSize);
      }
      owner = call;
    }

    synchronized void release(Call call) {
      // OkHttp makes the connection reusable before delivering this callback. Ignore a late
      // release from the previous call if another call already acquired and buffered data.
      if (owner != call) {
        return;
      }
      bufferSize = 0;
      if (output != null) {
        output.release();
      }
      owner = null;
    }
    // Inherit Socket.close(): cancellation must close the socket without taking the output lock.
  }

  static final class AggregatingOutputStream extends OutputStream {
    private final OutputStream output;
    private byte[] buffer;
    private int bufferSize;
    private int count;

    AggregatingOutputStream(OutputStream output) {
      this.output = output;
    }

    synchronized void configure(int bufferSize) {
      if (count != 0) {
        throw new IllegalStateException("Cannot reuse a socket with pending upload bytes");
      }
      this.bufferSize = bufferSize;
      buffer = null;
    }

    synchronized void release() {
      // Called after the exchange releases its connection, also on failure. Never flush on abort.
      buffer = null;
      bufferSize = 0;
      count = 0;
    }

    @Override
    public synchronized void write(int value) throws IOException {
      if (bufferSize == 0) {
        output.write(value);
        return;
      }
      if (count == bufferSize) {
        flushBuffer();
      }
      ensureCapacity(count + 1);
      buffer[count++] = (byte) value;
    }

    @Override
    public synchronized void write(byte[] data, int offset, int length) throws IOException {
      if (offset < 0 || length < 0 || offset > data.length - length) {
        throw new IndexOutOfBoundsException();
      }
      if (length == 0) {
        return;
      }
      if (bufferSize == 0 || length >= bufferSize) {
        flushBuffer();
        output.write(data, offset, length);
        return;
      }
      if (length > bufferSize - count) {
        flushBuffer();
      }
      ensureCapacity(count + length);
      System.arraycopy(data, offset, buffer, count, length);
      count += length;
    }

    private void ensureCapacity(int required) {
      if (buffer == null) {
        // Small, highly compressed blocks should not pay for a full 4 MiB allocation.
        buffer = new byte[Math.min(bufferSize, 64 * 1024)];
      }
      if (required > buffer.length) {
        byte[] expanded = new byte[bufferSize];
        System.arraycopy(buffer, 0, expanded, 0, count);
        buffer = expanded;
      }
    }

    private void flushBuffer() throws IOException {
      if (count > 0) {
        output.write(buffer, 0, count);
        count = 0;
      }
    }

    @Override
    public synchronized void flush() throws IOException {
      flushBuffer();
      output.flush();
    }

    @Override
    public void close() throws IOException {
      try {
        flush();
      } finally {
        output.close();
      }
    }
  }
}
