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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.EventListener;
import okhttp3.Request;
import org.junit.jupiter.api.Test;

class UploadSocketFactoryTest {
  @Test
  void lateConnectionReleaseCannotDiscardNextCallsBufferedBytes() throws Exception {
    // OkHttp returns the connection to its pool before invoking connectionReleased. Another
    // request may already have acquired it and buffered its headers/body when that callback runs.
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        Socket socket = new UploadSocketFactory().createSocket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()));
      try (Socket peer = server.accept()) {
        peer.setSoTimeout(2000);
        Connection connection = mock(Connection.class);
        when(connection.socket()).thenReturn(socket);
        Call previous = uploadCall(32768), next = uploadCall(65536);
        EventListener.Factory factory = UploadSocketFactory.listenerFactory();
        EventListener oldListener = factory.create(previous), newListener = factory.create(next);
        oldListener.connectionAcquired(previous, connection);
        OutputStream output = socket.getOutputStream();
        output.write(new byte[] {1, 2});
        output.flush();
        newListener.connectionAcquired(next, connection);
        output.write(new byte[] {3, 4, 5});
        oldListener.connectionReleased(previous, connection);
        output.flush();
        newListener.connectionReleased(next, connection);
        socket.shutdownOutput();
        ByteArrayOutputStream received = new ByteArrayOutputStream();
        InputStream input = peer.getInputStream();
        int value;
        while ((value = input.read()) != -1) received.write(value);
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, received.toByteArray());
      }
    }
  }

  private Call uploadCall(int chunkSize) {
    Call call = mock(Call.class);
    when(call.request())
        .thenReturn(
            new Request.Builder()
                .url("http://localhost/")
                .post(new StreamingUpload(chunkSize, 1000))
                .build());
    return call;
  }

  @Test
  void smallWritesAreCombinedAndFlushPreservesEveryByte() throws Exception {
    CountingOutput target = new CountingOutput();
    UploadSocketFactory.AggregatingOutputStream output =
        new UploadSocketFactory.AggregatingOutputStream(target);
    output.configure(32768);
    byte[] data = new byte[128 * 8192];
    new Random(1729).nextBytes(data);
    for (int i = 0; i < data.length; i += 8192) {
      output.write(data, i, 8192);
    }
    output.flush();
    assertArrayEquals(data, target.toByteArray());
    assertEquals(32, target.writes);
  }

  @Test
  void releaseDiscardsFailedExchangeAndAllowsAnotherBufferSize() throws Exception {
    CountingOutput target = new CountingOutput();
    UploadSocketFactory.AggregatingOutputStream output =
        new UploadSocketFactory.AggregatingOutputStream(target);
    output.configure(32768);
    output.write(new byte[17]);
    assertThrows(IllegalStateException.class, () -> output.configure(16));
    output.release();
    assertEquals(0, target.size());
    output.configure(16);
    output.write(new byte[] {1, 2, 3});
    output.flush();
    assertArrayEquals(new byte[] {1, 2, 3}, target.toByteArray());
  }

  @Test
  void closeDrainsTheTailAndClosesTheUnderlyingStream() throws Exception {
    CountingOutput target = new CountingOutput();
    UploadSocketFactory.AggregatingOutputStream output =
        new UploadSocketFactory.AggregatingOutputStream(target);
    output.configure(32768);
    output.write(42);
    output.close();
    assertArrayEquals(new byte[] {42}, target.toByteArray());
    assertTrue(target.closed);
  }

  @Test
  void growingTheLazyBufferDoesNotFlushBeforeTheConfiguredTarget() throws Exception {
    CountingOutput target = new CountingOutput();
    UploadSocketFactory.AggregatingOutputStream output =
        new UploadSocketFactory.AggregatingOutputStream(target);
    output.configure(131072);
    byte[] data = new byte[17 * 8192];
    new Random(1729).nextBytes(data);
    for (int i = 0; i < data.length; i += 8192) output.write(data, i, 8192);
    assertEquals(131072, target.size());
    output.flush();
    assertArrayEquals(data, target.toByteArray());
    assertEquals(2, target.writes);
  }

  private static class CountingOutput extends ByteArrayOutputStream {
    int writes;
    boolean closed;

    @Override
    public synchronized void write(byte[] data, int offset, int count) {
      writes++;
      super.write(data, offset, count);
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }
  }
}
