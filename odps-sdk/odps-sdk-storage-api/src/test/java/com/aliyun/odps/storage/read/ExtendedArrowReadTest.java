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

package com.aliyun.odps.storage.read;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.aliyun.odps.table.arrow.readers.ExtendedArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import com.aliyun.odps.Column;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.StubSettings;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionResponse;
import com.aliyun.odps.storage.internal.models.CreateTableReadStreamRequest;
import com.aliyun.odps.storage.internal.models.ReadSchema;
import com.aliyun.odps.storage.models.SplitMode;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowReader;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.type.TypeInfoFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ExtendedArrowReadTest {
  @Test
  void readsMultipleExtendedStreamsAndConsumesMetrics() throws Exception {
    verifyRead(true, false);
  }

  @Test
  void readsExtendedStreamsAsynchronously() throws Exception {
    verifyRead(true, true);
  }

  @Test
  void stillReadsOrdinaryArrowStreams() throws Exception {
    verifyRead(false, false);
  }

  @Test
  void asynchronousWorkersTerminateAfterAllSubstreams() throws Exception {
    try (RootAllocator allocator = new RootAllocator()) {
      ByteArrayOutputStream wire = new ByteArrayOutputStream();
      wire.write(1);
      writeData(wire, allocator, 42);
      wire.write(1);
      writeData(wire, allocator, 43);
      WorkerTrackingInputStream input = new WorkerTrackingInputStream(wire.toByteArray());
      try (ArrowReader reader = new ExtendedArrowReader(input, allocator, null, true, null)) {
        assertTrue(reader.nextBatch());
        assertTrue(reader.nextBatch());
        assertFalse(reader.nextBatch());
      }
      assertEquals(2, input.workers.size(), "One asynchronous worker per Arrow substream");
      assertWorkersStopped(input);
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void earlyCloseReleasesQueuedBatchesAndStopsAsynchronousWorker() throws Exception {
    verifyEarlyClose(false);
  }

  @Test
  void interruptedCloseStillReleasesResourcesAndPreservesInterrupt() throws Exception {
    verifyEarlyClose(true);
  }

  private void verifyEarlyClose(boolean interrupt) throws Exception {
    try (RootAllocator allocator = new RootAllocator()) {
      ByteArrayOutputStream wire = new ByteArrayOutputStream();
      wire.write(1);
      try (BigIntVector vector = new BigIntVector("a", allocator)) {
        vector.allocateNew(1);
        vector.setSafe(0, 42);
        vector.setValueCount(1);
        try (VectorSchemaRoot root = VectorSchemaRoot.of(vector);
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, wire)) {
          root.setRowCount(1);
          writer.start();
          for (int i = 0; i < 4; i++) {
            writer.writeBatch();
          }
          writer.end();
        }
      }
      CompletedReadQueue queue = new CompletedReadQueue();
      WorkerTrackingInputStream input = new WorkerTrackingInputStream(wire.toByteArray());
      try {
        try (ArrowReader reader = new ExtendedArrowReader(input, allocator, null, true, queue)) {
          assertTrue(reader.nextBatch());
          assertTrue(queue.completed.await(3, TimeUnit.SECONDS), "Producer did not finish in time");
          assertEquals(3, queue.stream().filter(ArrowRecordBatch.class::isInstance).count(),
              "Exercise close with unconsumed batches, not only an idle worker");
          if (interrupt) {
            Thread.currentThread().interrupt();
          }
        }
        if (interrupt) {
          assertTrue(Thread.interrupted(), "Close must preserve cancellation status");
        }
        assertAll(
            () -> assertEquals(0, allocator.getAllocatedMemory(), "Queued batches must be released"),
            () -> assertWorkersStopped(input));
      } finally {
        Thread.interrupted();
        // Keep a failing regression from leaking its queued Arrow buffers into other tests.
        Object pending;
        while ((pending = queue.poll()) != null) {
          if (pending instanceof ArrowRecordBatch) {
            ((ArrowRecordBatch) pending).close();
          }
        }
      }
    }
  }

  private static void assertWorkersStopped(WorkerTrackingInputStream input) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    for (Thread worker : input.workers) {
      long remaining = deadline - System.nanoTime();
      if (remaining > 0) {
        worker.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
      }
    }
    assertTrue(input.workers.stream().noneMatch(Thread::isAlive),
        "Closing Arrow readers must terminate all of their asynchronous workers");
  }

  private static final class CompletedReadQueue extends LinkedBlockingQueue<Object> {
    private final CountDownLatch completed = new CountDownLatch(1);

    @Override
    public void put(Object value) throws InterruptedException {
      super.put(value);
      if (!(value instanceof ArrowRecordBatch)) {
        completed.countDown();
      }
    }
  }

  private static final class WorkerTrackingInputStream extends ByteArrayInputStream {
    private final Thread caller = Thread.currentThread();
    private final Set<Thread> workers = ConcurrentHashMap.newKeySet();

    private WorkerTrackingInputStream(byte[] wire) {
      super(wire);
    }

    @Override
    public synchronized int read(byte[] bytes, int offset, int length) {
      if (Thread.currentThread() != caller) {
        workers.add(Thread.currentThread());
      }
      return super.read(bytes, offset, length);
    }
  }

  private void verifyRead(boolean extended, boolean async) throws Exception {
    try (RootAllocator allocator = new RootAllocator()) {
      ByteArrayOutputStream wire = new ByteArrayOutputStream();
      if (extended) {
        wire.write(1);
      }
      writeData(wire, allocator, 42);
      if (extended) {
        wire.write(2);
        try (VectorSchemaRoot metrics = VectorSchemaRoot.create(
            new Schema(Collections.emptyList(),
                Collections.singletonMap("server_processing_duration_us", "10")), allocator);
             ArrowStreamWriter writer = new ArrowStreamWriter(metrics, null, wire)) {
          writer.start();
          writer.end();
        }
        wire.write(1);
        writeData(wire, allocator, 43);
      }
      TrackingInputStream input = new TrackingInputStream(wire.toByteArray());
      StorageStub stub = new StorageStub(StubSettings.newBuilder()
          .withEndpoint("http://localhost:1").withTunnelEndpoint("http://localhost:1")
          .withProject("proj")
          .withCredentialsProvider(com.aliyun.odps.credentials.StaticCredentialProvider.of("test", "test"))
          .withHttpSettings(com.aliyun.odps.storage.settings.HttpSettings.newBuilder().build()).build()) {
        @Override
        public InputStream createTableReadStream(TableIdentifier table, InputSplit split,
            CreateTableReadStreamRequest request, String routeToken) {
          return input;
        }
      };
      ReadSchema schema = new ReadSchema();
      schema.setColumns(Collections.singletonList(new Column("a", TypeInfoFactory.BIGINT)));
      CreateTableReadSessionResponse response = new CreateTableReadSessionResponse();
      response.setSessionId("s");
      response.setDataSchema(schema);
      response.setSplitMode(SplitMode.SIZE);
      response.setSplitsCount(1);
      response.setExtendedArrowIPCEnabled(extended);
      TableIdentifier id = TableIdentifier.of("proj", "default", "tbl");
      TableReadSession session = new TableReadSession(stub, id, allocator, response,
          new TableReadSessionBuilder(stub, allocator, id));
      try (ArrowReader reader = session.createReaderBuilder(session.getSplits().get(0))
          .withAsync(async).build()) {
        assertTrue(reader.nextBatch());
        assertEquals(42, ((BigIntVector) reader.getCurrentValue().getVector("a")).get(0));
        if (extended) {
          assertTrue(reader.nextBatch());
          assertEquals(43, ((BigIntVector) reader.getCurrentValue().getVector("a")).get(0));
        }
        assertFalse(reader.nextBatch());
        assertFalse(reader.nextBatch());
        assertTrue(reader.bytesRead() > 0);
      }
      assertTrue(input.closed);
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  private void writeData(ByteArrayOutputStream output, RootAllocator allocator, long value)
      throws Exception {
    try (BigIntVector vector = new BigIntVector("a", allocator)) {
      vector.allocateNew(1);
      vector.setSafe(0, value);
      vector.setValueCount(1);
      try (VectorSchemaRoot root = VectorSchemaRoot.of(vector);
           ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
        root.setRowCount(1);
        writer.start();
        writer.writeBatch();
        writer.end();
      }
    }
  }

  private static class TrackingInputStream extends ByteArrayInputStream {
    private boolean closed;
    TrackingInputStream(byte[] data) {
      super(data);
    }
    @Override
    public void close() {
      closed = true;
    }
  }
}
