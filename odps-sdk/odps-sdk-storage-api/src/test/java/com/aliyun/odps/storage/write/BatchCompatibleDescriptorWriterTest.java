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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.BatchCompatibleCreateSessionRequest;
import com.aliyun.odps.storage.internal.models.BatchCompatibleSessionResponse;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamRequest;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
import com.aliyun.odps.storage.models.SessionDescriptor;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.Collections;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class BatchCompatibleDescriptorWriterTest {
  private final TableIdentifier table = TableIdentifier.of("p", "s", "t");

  private BatchCompatibleSessionResponse response(String status) {
    BatchCompatibleSessionResponse response = new Gson().fromJson(
        "{\"SessionId\":\"session\",\"SessionStatus\":\"" + status
            + "\",\"MaxBlockNumber\":8,\"DataSchema\":{"
            + "\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"TIMESTAMP\"}],"
            + "\"PartitionColumns\":[]}}", BatchCompatibleSessionResponse.class);
    response.setRouteToken("descriptor-route");
    return response;
  }

  private TableWriteSession coordinator(StorageStub stub, RootAllocator allocator) {
    when(stub.createBatchCompatibleSession(eq(table), any(BatchCompatibleCreateSessionRequest.class)))
        .thenReturn(response("NORMAL"));
    return new TableWriteSessionBuilder(stub, allocator, table)
        .withWriteMode(WriteMode.BATCH_COMPATIBLE)
        .withBatchCompatibleOptions(BatchCompatibleOptions.newBuilder().withArrowOptions(
            ArrowOptions.newBuilder().withTimestampUnit(ArrowOptions.TimestampUnit.MICRO)
                .withDatetimeUnit(ArrowOptions.TimestampUnit.MILLI).build()).build()).build();
  }

  @Test
  void publicDescriptorRestoresLocallyAndReservesOnlyItsAttempt() throws Exception {
    StorageStub ownerStub = mock(StorageStub.class);
    StorageStub workerStub = mock(StorageStub.class);
    try (RootAllocator allocator = new RootAllocator(1024 * 1024)) {
      SessionDescriptor descriptor = coordinator(ownerStub, allocator).getDescriptor();
      descriptor = SessionDescriptor.fromJson(descriptor.toJson());
      TableWriteSession worker = new TableWriteSessionBuilder(workerStub, allocator, table)
          .withSessionDescriptor(descriptor).build();
      assertEquals("session", worker.getId());
      assertEquals(WriteMode.BATCH_COMPATIBLE, worker.getWriteMode());
      assertEquals(8, worker.getMaxBlockNumber().getAsLong());
      assertEquals(ArrowOptions.TimestampUnit.MICRO,
          worker.getDescriptor().getArrowOptions().getTimestampUnit());
      verifyNoInteractions(workerStub);
      CreateWriteStreamResponse reservation = new CreateWriteStreamResponse();
      reservation.setRouteToken("reservation-route");
      reservation.setQuotaToken("quota");
      when(workerStub.createTableWriteStream(eq(table), eq("session"),
          any(CreateWriteStreamRequest.class), eq("descriptor-route"),
          eq(WriteMode.BATCH_COMPATIBLE))).thenReturn(reservation);
      TableBlockWriter writer = worker.createBlockWriter(7, 2);
      when(workerStub.openBatchCompatibleBlock(eq(table), eq("session"), eq(7), eq(2),
          eq("reservation-route"), eq("quota"), anyInt()))
          .thenThrow(new ClientException("test upload failure"));
      try (VectorSchemaRoot root = writer.createVectorSchemaRoot()) {
        assertEquals("a", root.getSchema().getFields().get(0).getName());
        assertSame(allocator, root.getVector(0).getAllocator());
        assertEquals(org.apache.arrow.vector.types.TimeUnit.MICROSECOND,
            ((org.apache.arrow.vector.types.pojo.ArrowType.Timestamp)
                root.getSchema().getFields().get(0).getType()).getUnit());
        verifyNoInteractions(workerStub);
        root.allocateNew();
        ((TimeStampVector) root.getVector(0)).setSafe(0, 1L);
        root.setRowCount(1);
        assertThrows(IOException.class, () -> writer.writeBatch(root));
      }
      writer.abort();
      assertThrows(IOException.class, writer::close);
      worker.close();
      try (ArrowBuf buffer = allocator.buffer(8)) { assertEquals(8, buffer.capacity()); }
      ArgumentCaptor<CreateWriteStreamRequest> request =
          ArgumentCaptor.forClass(CreateWriteStreamRequest.class);
      verify(workerStub).createTableWriteStream(eq(table), eq("session"), request.capture(),
          eq("descriptor-route"), eq(WriteMode.BATCH_COMPATIBLE));
      assertEquals("block-7-attempt-2", request.getValue().getStreamId());
      assertEquals(1, request.getValue().getStreamVersion());
      verify(workerStub).openBatchCompatibleBlock(eq(table), eq("session"), eq(7), eq(2),
          eq("reservation-route"), eq("quota"), anyInt());
      verifyNoMoreInteractions(workerStub);
    }
  }

  @Test
  void restoredWorkerHasNoGlobalLifecycleAuthority() {
    StorageStub stub = mock(StorageStub.class);
    try (RootAllocator allocator = new RootAllocator(1024)) {
      SessionDescriptor descriptor = coordinator(mock(StorageStub.class), allocator).getDescriptor();
      TableWriteSession worker = new TableWriteSessionBuilder(stub, allocator, table)
          .withSessionDescriptor(descriptor).build();
      assertThrows(IllegalStateException.class, worker::commit);
      assertThrows(IllegalStateException.class, () -> worker.commit(null, null));
      assertThrows(IllegalStateException.class, () -> worker.commit(Collections.emptyList()));
      assertThrows(IllegalStateException.class, () -> worker.commit(Collections.emptyList(), 600));
      assertThrows(IllegalStateException.class, worker::abort);
      worker.close();
      worker.close();
      verifyNoInteractions(stub);
    }
  }

  @Test
  void mismatchedKindTableAndInvalidAttemptFailBeforeRpc() {
    StorageStub stub = mock(StorageStub.class);
    try (RootAllocator allocator = new RootAllocator(1024)) {
      SessionDescriptor descriptor = coordinator(mock(StorageStub.class), allocator).getDescriptor();
      assertThrows(IllegalArgumentException.class,
          () -> new TableWriteSessionBuilder(stub, allocator, TableIdentifier.of("p", "s", "other"))
              .withSessionDescriptor(descriptor).build());
      SessionDescriptor read = SessionDescriptor.fromJson(descriptor.toJson()
          .replace("batch-write", "batch-read")
          .replace("\"metadata\":{", "\"metadata\":{\"splitMode\":\"Size\",\"splitsCount\":1,"));
      assertThrows(IllegalArgumentException.class,
          () -> new TableWriteSessionBuilder(stub, allocator, table)
              .withSessionDescriptor(read).build());
      assertThrows(IllegalArgumentException.class,
          () -> new TableWriteSessionBuilder(stub, allocator, table)
              .withSessionDescriptor(descriptor).withSessionId("other").build());
      TableWriteSession worker = new TableWriteSessionBuilder(stub, allocator, table)
          .withSessionDescriptor(descriptor).build();
      assertThrows(IllegalArgumentException.class, () -> worker.createBlockWriter(8, 0));
      assertThrows(IllegalArgumentException.class, () -> worker.createBlockWriter(0, -1));
      verifyNoInteractions(stub);
    }
  }

  @Test
  void commitAttemptExportCannotReopenWritersEvenWhenReplyIsLost() {
    StorageStub ownerStub = mock(StorageStub.class);
    StorageStub workerStub = mock(StorageStub.class);
    try (RootAllocator allocator = new RootAllocator(1024)) {
      TableWriteSession owner = coordinator(ownerStub, allocator);
      when(ownerStub.commitBatchCompatibleSession(eq(table), eq("session"),
          eq("descriptor-route"), anyCollection())).thenThrow(new ClientException("lost reply"));
      assertThrows(ClientException.class, () -> owner.commit(Collections.emptyList()));
      SessionDescriptor descriptor = owner.getDescriptor();
      assertTrue(descriptor.toJson().contains("\"commitStarted\":true"));
      TableWriteSession worker = new TableWriteSessionBuilder(workerStub, allocator, table)
          .withSessionDescriptor(descriptor).build();
      assertThrows(IllegalStateException.class, () -> worker.createBlockWriter(0, 0));
      worker.close();
      verifyNoInteractions(workerStub);
    }
  }

  @Test
  void unknownCommitCloseNeverAbortsTheCoordinator() {
    StorageStub ownerStub = mock(StorageStub.class);
    try (RootAllocator allocator = new RootAllocator(1024)) {
      TableWriteSession owner = coordinator(ownerStub, allocator);
      when(ownerStub.commitBatchCompatibleSession(eq(table), eq("session"),
          eq("descriptor-route"), anyCollection())).thenThrow(new ClientException("lost reply"));
      assertThrows(ClientException.class, () -> owner.commit(Collections.emptyList()));
      clearInvocations(ownerStub);
      owner.close();
      owner.close();
      verifyNoInteractions(ownerStub);
      try (ArrowBuf buffer = allocator.buffer(8)) { assertEquals(8, buffer.capacity()); }
    }
  }

  @Test
  void descriptorUnitsOverrideCreationOptionsInEitherSetterOrder() {
    StorageStub workerStub = mock(StorageStub.class);
    try (RootAllocator allocator = new RootAllocator(1024)) {
      SessionDescriptor descriptor = coordinator(mock(StorageStub.class), allocator).getDescriptor();
      BatchCompatibleOptions differentUnits = BatchCompatibleOptions.newBuilder()
          .withArrowOptions(ArrowOptions.newBuilder()
              .withTimestampUnit(ArrowOptions.TimestampUnit.NANO)
              .withDatetimeUnit(ArrowOptions.TimestampUnit.MICRO).build()).build();
      TableWriteSession before = new TableWriteSessionBuilder(workerStub, allocator, table)
          .withBatchCompatibleOptions(differentUnits).withSessionDescriptor(descriptor).build();
      TableWriteSession after = new TableWriteSessionBuilder(workerStub, allocator, table)
          .withSessionDescriptor(descriptor).withBatchCompatibleOptions(differentUnits).build();
      for (TableWriteSession worker : new TableWriteSession[]{before, after}) {
        assertEquals(ArrowOptions.TimestampUnit.MICRO,
            worker.getDescriptor().getArrowOptions().getTimestampUnit());
        assertEquals(ArrowOptions.TimestampUnit.MILLI,
            worker.getDescriptor().getArrowOptions().getDateTimeUnit());
        worker.close();
      }
      verifyNoInteractions(workerStub);
    }
  }

  @Test
  void terminalSnapshotPreservesStateAndRejectsWriters() {
    StorageStub workerStub = mock(StorageStub.class);
    try (RootAllocator allocator = new RootAllocator(1024)) {
      String json = coordinator(mock(StorageStub.class), allocator).getDescriptor().toJson();
      for (String state : new String[]{"COMMITTING", "COMMITTED", "EXPIRED"}) {
        SessionDescriptor descriptor = SessionDescriptor.fromJson(json.replace("NORMAL", state));
        TableWriteSession worker = new TableWriteSessionBuilder(workerStub, allocator, table)
            .withSessionDescriptor(descriptor).build();
        assertTrue(worker.getDescriptor().toJson().contains(state));
        assertThrows(IllegalStateException.class, () -> worker.createBlockWriter(0, 0));
        worker.close();
      }
      verifyNoInteractions(workerStub);
    }
  }

  @Test
  void otherWriteModesExplicitlyRejectDescriptorExport() {
    StorageStub stub = mock(StorageStub.class);
    TableWriteSession streaming = new TableWriteSessionBuilder(stub, null, table)
        .withWriteMode(WriteMode.STREAMING).build();
    assertThrows(UnsupportedOperationException.class, streaming::getDescriptor);
    verifyNoInteractions(stub);
  }
}
