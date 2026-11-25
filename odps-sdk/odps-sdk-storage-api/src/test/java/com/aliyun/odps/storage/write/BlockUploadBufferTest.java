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

package com.aliyun.odps.storage.write;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.io.StreamingUpload;
import com.aliyun.odps.storage.internal.models.BatchCompatibleSessionResponse;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
import com.aliyun.odps.storage.internal.models.HttpResponse;
import com.aliyun.odps.storage.models.SessionDescriptor;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.compression.OdpsCompressionFactory;
import com.google.gson.Gson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okio.Buffer;
import okio.BufferedSink;
import okio.Okio;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockUploadBufferTest {
  private StorageStub stub;
  private RootAllocator allocator;
  private final TableIdentifier table = TableIdentifier.of("project", "schema", "table");

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(16L * 1024 * 1024);
    stub = mock(StorageStub.class);
    Gson gson = new Gson();
    when(stub.getGson()).thenReturn(gson);
    BatchCompatibleSessionResponse response = gson.fromJson("{\"SessionId\":\"s1\","
        + "\"SessionStatus\":\"NORMAL\",\"MaxBlockNumber\":8,\"DataSchema\":{"
        + "\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"INT\",\"Nullable\":true}],"
        + "\"PartitionColumns\":[]}}", BatchCompatibleSessionResponse.class);
    when(stub.createBatchCompatibleSession(eq(table), any())).thenReturn(response);
    CreateWriteStreamResponse reservation = new CreateWriteStreamResponse();
    reservation.setQuotaToken("quota");
    reservation.setRouteToken("route");
    when(stub.createTableWriteStream(eq(table), eq("s1"), any(), any(),
        eq(WriteMode.BATCH_COMPATIBLE))).thenReturn(reservation);
  }

  @AfterEach
  void tearDown() {
    allocator.close();
  }

  private TableWriteSessionBuilder builder() {
    return new TableWriteSessionBuilder(stub, allocator, table)
        .withWriteMode(WriteMode.BATCH_COMPATIBLE);
  }

  private void useUpload(StreamingUpload upload) {
    when(stub.openBatchCompatibleBlock(eq(table), eq("s1"), anyInt(), anyInt(),
        eq("route"), eq("quota"), anyInt())).thenReturn(upload);
  }

  private Call capture(OutputStream output, int rows) throws IOException {
    StreamingUpload upload = new StreamingUpload(64 * 1024, 5000);
    BufferedSink sink = Okio.buffer(Okio.sink(output));
    HttpResponse response = new HttpResponse();
    response.setBody("{\"RecordCount\":" + rows + ",\"CommitMessage\":\"message\"}");
    Call call = mock(Call.class);
    upload.start(call, () -> {
      upload.writeTo(sink);
      output.close();
      return response;
    });
    useUpload(upload);
    return call;
  }

  private void write(TableBlockWriter writer, int batches, int rows) throws IOException {
    Random random = new Random(1729);
    try (VectorSchemaRoot root = writer.createVectorSchemaRoot()) {
      root.allocateNew();
      IntVector values = (IntVector) root.getVector("a");
      for (int batch = 0; batch < batches; batch++) {
        for (int row = 0; row < rows; row++) {
          values.setSafe(row, random.nextInt());
        }
        root.setRowCount(rows);
        writer.writeBatch(root);
      }
    }
  }

  private CaptureOutput writeAll(Integer bufferSize, boolean restore) throws Exception {
    CaptureOutput output = new CaptureOutput();
    capture(output, 16 * 4096);
    TableWriteSessionBuilder builder = builder();
    if (restore) {
      SessionDescriptor descriptor = builder.build().getDescriptor();
      // The buffer is worker-local; descriptor restore must not overwrite the chosen value.
      builder = new TableWriteSessionBuilder(stub, allocator, table)
          .withSessionDescriptor(SessionDescriptor.fromJson(descriptor.toJson()));
    }
    if (bufferSize != null) {
      builder.withUploadBufferSize(bufferSize);
    }
    TableBlockWriter writer = builder.build().createBlockWriter(0, 0);
    write(writer, 16, 4096);
    java.lang.reflect.Field configured =
        TableBlockWriter.class.getDeclaredField("uploadBufferSize");
    configured.setAccessible(true);
    assertEquals(
        bufferSize == null ? 256 * 1024 : bufferSize.intValue(), configured.getInt(writer));
    BlockWriteResult result = writer.commit();
    assertEquals(16 * 4096, result.getRecordCount());
    assertSame(result, writer.commit());
    assertEquals(1, output.closes);
    return output;
  }

  @Test
  void aggregationAndWorkerOverridePreserveExactZstdBytes() throws Exception {
    CaptureOutput raw = writeAll(0, false);
    CaptureOutput defaults = writeAll(null, false);
    CaptureOutput restored = writeAll(16 * 1024, true);
    assertArrayEquals(raw.toByteArray(), defaults.toByteArray());
    assertArrayEquals(raw.toByteArray(), restored.toByteArray());
    Random expected = new Random(1729);
    int rows = 0;
    try (ArrowStreamReader reader = new ArrowStreamReader(
        new ByteArrayInputStream(defaults.toByteArray()), allocator,
        OdpsCompressionFactory.INSTANCE)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        IntVector values = (IntVector) root.getVector("a");
        for (int i = 0; i < root.getRowCount(); i++) {
          assertEquals(expected.nextInt(), values.get(i));
          rows++;
        }
      }
    }
    assertEquals(16 * 4096, rows);
  }

  @Test
  void flushDrainsPartialBufferAndCloseWritesTheTail() throws Exception {
    CaptureOutput output = new CaptureOutput();
    capture(output, 2);
    TableBlockWriter writer = builder().build().createBlockWriter(0, 0);
    write(writer, 1, 1);
    assertEquals(0, output.size());
    writer.flush();
    int flushed = output.size();
    assertTrue(flushed > 0);
    assertEquals(1, output.flushes);
    write(writer, 1, 1);
    assertEquals(flushed, output.size());
    writer.commit();
    assertTrue(output.size() > flushed);
  }

  @Test
  void zeroRowBlockCommitsLocallyWithoutReservingOrUploading() throws Exception {
    TableBlockWriter writer = builder().build().createBlockWriter(0, 0);
    try (VectorSchemaRoot root = writer.createVectorSchemaRoot()) {
      root.allocateNew();
      root.setRowCount(0);
      writer.writeBatch(root);
      writer.flush();
    }
    BlockWriteResult result = writer.commit();
    assertTrue(result.isEmpty());
    assertSame(result, writer.commit());
    assertEquals(0, writer.bytesWritten());
    verify(stub, never()).createTableWriteStream(any(), any(), any(), any(), any());
    verify(stub, never()).openBatchCompatibleBlock(
        any(), any(), anyInt(), anyInt(), any(), any(), anyInt());
  }

  @Test
  void abortDiscardsPendingBufferWithoutFinishingUpload() throws Exception {
    CaptureOutput output = new CaptureOutput();
    Call call = capture(output, 1);
    TableBlockWriter writer = builder().build().createBlockWriter(0, 0);
    write(writer, 1, 1);
    writer.abort();
    writer.close();
    assertEquals(0, output.size());
    verify(call).cancel();
    assertEquals(0, output.closes);
    assertThrows(IOException.class, writer::commit);
  }

  @Test
  void failureWhileDrainingCannotProduceCommitResult() throws Exception {
    OutputStream output = mock(OutputStream.class);
    doThrow(new IOException("network failed"))
        .when(output)
        .write(any(byte[].class), anyInt(), anyInt());
    Call call = capture(output, 1);
    TableBlockWriter writer = builder().build().createBlockWriter(0, 0);
    write(writer, 1, 1);
    assertThrows(IOException.class, writer::commit);
    assertThrows(IOException.class, writer::commit);
    verify(call).cancel();
  }

  @Test
  void abortUnblocksFlushWaitingForNetwork() throws Exception {
    StreamingUpload upload = new StreamingUpload(64 * 1024, 0);
    useUpload(upload);
    CountDownLatch flushing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    BufferedSink sink = mock(BufferedSink.class);
    doAnswer(
            invocation -> {
              ((Buffer) invocation.getArgument(0)).skip((Long) invocation.getArgument(1));
              return null;
            })
        .when(sink)
        .write(any(Buffer.class), anyLong());
    doAnswer(invocation -> {
      flushing.countDown();
      release.await();
      return null;
    }).when(sink).flush();
    upload.start(mock(okhttp3.Call.class), () -> {
      upload.writeTo(sink);
      return new HttpResponse();
    });
    TableBlockWriter writer = builder().build().createBlockWriter(0, 0);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      write(writer, 1, 1);
      Future<IOException> result = executor.submit(() ->
          assertThrows(IOException.class, writer::flush));
      assertTrue(flushing.await(5, TimeUnit.SECONDS));
      writer.abort();
      assertNotNull(result.get(5, TimeUnit.SECONDS));
      assertThrows(IOException.class, writer::commit);
    } finally {
      writer.abort();
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void invalidOptionsFailBeforeRpc() {
    assertThrows(IllegalArgumentException.class, () -> builder().withUploadBufferSize(-1));
    for (WriteMode mode : new WriteMode[] {WriteMode.BATCH, WriteMode.STREAMING}) {
      assertThrows(IllegalStateException.class,
          () -> builder().withWriteMode(mode).withUploadBufferSize(0).build());
    }
    verifyNoInteractions(stub);
  }

  private static class CaptureOutput extends ByteArrayOutputStream {
    int flushes;
    int closes;

    @Override
    public void flush() {
      if (size() > 0) {
        flushes++;
      }
    }

    @Override
    public void close() {
      closes++;
    }
  }
}
