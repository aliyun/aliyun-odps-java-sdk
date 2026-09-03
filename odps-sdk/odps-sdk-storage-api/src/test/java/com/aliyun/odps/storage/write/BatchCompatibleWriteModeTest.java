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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.BatchCompatibleCreateSessionRequest;
import com.aliyun.odps.storage.internal.models.BatchCompatibleSessionResponse;
import com.aliyun.odps.storage.internal.models.CreateTableWriteSessionRequest;
import com.aliyun.odps.storage.internal.models.CreateTableWriteSessionResponse;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamRequest;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
import com.aliyun.odps.table.TableIdentifier;
import com.google.gson.Gson;

class BatchCompatibleWriteModeTest {

  private StorageStub storageStub;
  private BufferAllocator allocator;
  private TableIdentifier table;
  private BatchCompatibleSessionResponse sessionResponse;

  @BeforeEach
  void setUp() {
    storageStub = mock(StorageStub.class);
    allocator = new RootAllocator(16L * 1024 * 1024);
    table = TableIdentifier.of("project", "schema", "table");
    sessionResponse = new Gson().fromJson("{"
        + "\"SessionId\":\"session-1\","
        + "\"SessionStatus\":\"NORMAL\","
        + "\"DataSchema\":{"
        + "\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"INT\",\"Nullable\":true}],"
        + "\"PartitionColumns\":[]},"
        + "\"MaxBlockNumber\":8,"
        + "\"EnhanceWriteCheck\":false}", BatchCompatibleSessionResponse.class);

    CreateWriteStreamResponse reservation = new CreateWriteStreamResponse();
    reservation.setQuotaToken("quota-token");
    reservation.setRouteToken("route-reservation");
    when(storageStub.createTableWriteStream(
        eq(table),
        eq("session-1"),
        any(CreateWriteStreamRequest.class),
        eq("route-token"),
        eq(WriteMode.BATCH_COMPATIBLE)))
        .thenReturn(reservation);
  }

  @AfterEach
  void tearDown() {
    allocator.close();
  }

  @Test
  void builderSelectsCompatibleSessionApiWithoutChangingNormalModes() {
    when(storageStub.createBatchCompatibleSession(
        eq(table), any(BatchCompatibleCreateSessionRequest.class)))
        .thenReturn(sessionResponse);

    TableWriteSession session = new TableWriteSessionBuilder(storageStub, allocator, table)
        .withWriteMode(WriteMode.BATCH_COMPATIBLE)
        .build();

    assertEquals(WriteMode.BATCH_COMPATIBLE, session.getWriteMode());
    assertEquals("session-1", session.getId());
    assertTrue(session.getMaxBlockNumber().isPresent());
    assertEquals(8, session.getMaxBlockNumber().getAsLong());
    verify(storageStub).createBatchCompatibleSession(
        eq(table), any(BatchCompatibleCreateSessionRequest.class));
    verify(storageStub, never()).createTableWriteSession(
        eq(table), any(CreateTableWriteSessionRequest.class), any(WriteMode.class));

    CreateTableWriteSessionResponse batchResponse = mock(CreateTableWriteSessionResponse.class);
    when(batchResponse.getSessionId()).thenReturn("batch-session");
    when(storageStub.createTableWriteSession(
        eq(table), any(CreateTableWriteSessionRequest.class), eq(WriteMode.BATCH)))
        .thenReturn(batchResponse);
    TableWriteSession batch = new TableWriteSessionBuilder(storageStub, allocator, table).build();
    assertEquals(WriteMode.BATCH, batch.getWriteMode());
    assertEquals("batch-session", batch.getId());
    verify(storageStub).createTableWriteSession(
        eq(table), any(CreateTableWriteSessionRequest.class), eq(WriteMode.BATCH));

    assertThrows(IllegalStateException.class, () ->
        new TableWriteSessionBuilder(storageStub, allocator, table)
            .withBatchCompatibleOptions(BatchCompatibleOptions.createDefault())
            .build());
  }

  @Test
  void streamingModeStillSkipsSessionCreation() {
    StorageStub isolatedStub = mock(StorageStub.class);
    TableWriteSession session = new TableWriteSessionBuilder(isolatedStub, allocator, table)
        .withWriteMode(WriteMode.STREAMING)
        .build();

    assertEquals(WriteMode.STREAMING, session.getWriteMode());
    verifyNoInteractions(isolatedStub);
  }

  @Test
  void reloadUsesCompatibleGetApi() {
    sessionResponse.setRouteToken("route-token");
    when(storageStub.getBatchCompatibleSession(table, "session-1", null))
        .thenReturn(sessionResponse);

    TableWriteSession session = new TableWriteSessionBuilder(storageStub, allocator, table)
        .withWriteMode(WriteMode.BATCH_COMPATIBLE)
        .withSessionId("session-1")
        .build();

    assertEquals(WriteMode.BATCH_COMPATIBLE, session.getWriteMode());
    verify(storageStub).getBatchCompatibleSession(table, "session-1", null);
    verify(storageStub, never()).getTableWriteSession(
        eq(table), eq("session-1"), isNull(), any(WriteMode.class));
  }

  @Test
  void streamAndBlockWriterApisAreModeIsolated() {
    TableWriteSession compatible = compatibleSession();
    assertThrows(UnsupportedOperationException.class,
        () -> compatible.createWriterBuilder("stream", 1));
    assertThrows(UnsupportedOperationException.class,
        () -> compatible.createWriterBuilder("stream"));
    assertThrows(IllegalArgumentException.class,
        () -> compatible.createBlockWriter(8, 0));
    assertThrows(IllegalArgumentException.class,
        () -> compatible.createBlockWriter(0, -1));

    TableBlockWriter writer = compatible.createBlockWriter(7, 0);
    assertEquals(7, writer.getBlockNumber());
    assertEquals(0, writer.getAttemptNumber());

    ArgumentCaptor<CreateWriteStreamRequest> request =
        ArgumentCaptor.forClass(CreateWriteStreamRequest.class);
    verify(storageStub).createTableWriteStream(
        eq(table),
        eq("session-1"),
        request.capture(),
        eq("route-token"),
        eq(WriteMode.BATCH_COMPATIBLE));
    assertEquals("block-7-attempt-0", request.getValue().getStreamId());
    assertEquals(1, request.getValue().getStreamVersion());

    TableWriteSession batch = new TableWriteSession(
        storageStub, table, null, allocator, "batch-session", WriteMode.BATCH, null);
    assertThrows(UnsupportedOperationException.class,
        () -> batch.createBlockWriter(0, 0));
    assertFalse(batch.getMaxBlockNumber().isPresent());
  }

  @Test
  void typedCommitHidesMessagesAndRejectsInvalidResultSets() {
    BatchCompatibleSessionResponse committed = mock(BatchCompatibleSessionResponse.class);
    when(committed.getSessionStatus()).thenReturn("COMMITTED");
    when(storageStub.commitBatchCompatibleSession(
        eq(table), eq("session-1"), eq("route-token"), any()))
        .thenReturn(committed);

    TableWriteSession session = compatibleSession();
    BlockWriteResult block0 = new BlockWriteResult("session-1", 0, 0, 10, "message-0");
    BlockWriteResult block1 = new BlockWriteResult("session-1", 1, 2, 20, "message-1");
    session.commit(Arrays.asList(block0, block1));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Collection<String>> messages = ArgumentCaptor.forClass(Collection.class);
    verify(storageStub).commitBatchCompatibleSession(
        eq(table), eq("session-1"), eq("route-token"), messages.capture());
    assertEquals(Arrays.asList("message-0", "message-1"), messages.getValue());

    TableWriteSession duplicateSession = compatibleSession();
    assertThrows(IllegalArgumentException.class, () -> duplicateSession.commit(Arrays.asList(
        block0, new BlockWriteResult("session-1", 0, 1, 10, "retry"))));

    TableWriteSession foreignSession = compatibleSession();
    assertThrows(IllegalArgumentException.class, () -> foreignSession.commit(
        Collections.singletonList(
            new BlockWriteResult("other-session", 0, 0, 10, "foreign"))));

    TableWriteSession untypedSession = compatibleSession();
    assertThrows(UnsupportedOperationException.class, untypedSession::commit);
  }

  @Test
  void failedOrIncompleteCommitDoesNotCloseSession() {
    BatchCompatibleSessionResponse normal = mock(BatchCompatibleSessionResponse.class);
    when(normal.getSessionStatus()).thenReturn("NORMAL");
    when(storageStub.commitBatchCompatibleSession(
        eq(table), eq("session-1"), eq("route-token"), any()))
        .thenReturn(normal);

    TableWriteSession session = compatibleSession();
    assertThrows(ClientException.class, () -> session.commit(Collections.emptyList()));

    session.abort();
    verify(storageStub).abortTableWriteSession(
        table, "session-1", "route-token", WriteMode.BATCH_COMPATIBLE);
  }

  @Test
  void abortedBlockWriterCannotBeCommitted() {
    TableBlockWriter writer = compatibleSession().createBlockWriter(0, 0);
    writer.abort();
    assertThrows(IOException.class, writer::commit);
  }

  @Test
  void serviceFailureIsExposedAsWriterIoFailure() {
    when(storageStub.writeBatchCompatibleBlock(
        eq(table),
        eq("session-1"),
        anyInt(),
        anyInt(),
        any(),
        eq("route-reservation"),
        eq("quota-token")))
        .thenThrow(new ServiceException(500, "InternalError", "write failed", "request-1"));

    TableBlockWriter writer = compatibleSession().createBlockWriter(0, 0);
    IOException failure = assertThrows(IOException.class, writer::close);
    assertTrue(failure.getCause() instanceof ServiceException);
    assertThrows(IOException.class, writer::commit);
  }

  @Test
  void missingReservationTokensFailBeforeCreatingWriter() {
    CreateWriteStreamResponse missingQuota = new CreateWriteStreamResponse();
    missingQuota.setRouteToken("route-reservation");
    when(storageStub.createTableWriteStream(
        eq(table),
        eq("session-1"),
        any(CreateWriteStreamRequest.class),
        eq("route-token"),
        eq(WriteMode.BATCH_COMPATIBLE)))
        .thenReturn(missingQuota);

    ClientException failure = assertThrows(
        ClientException.class,
        () -> compatibleSession().createBlockWriter(0, 0));
    assertTrue(failure.getMessage().contains("no quota token"));
  }

  @Test
  void missingReservationRouteFailsBeforeCreatingWriter() {
    CreateWriteStreamResponse missingRoute = new CreateWriteStreamResponse();
    missingRoute.setQuotaToken("quota-token");
    when(storageStub.createTableWriteStream(
        eq(table),
        eq("session-1"),
        any(CreateWriteStreamRequest.class),
        eq("route-token"),
        eq(WriteMode.BATCH_COMPATIBLE)))
        .thenReturn(missingRoute);

    ClientException failure = assertThrows(
        ClientException.class,
        () -> compatibleSession().createBlockWriter(0, 0));
    assertTrue(failure.getMessage().contains("no route token"));
  }

  private TableWriteSession compatibleSession() {
    return new TableWriteSession(
        storageStub,
        table,
        null,
        allocator,
        "session-1",
        WriteMode.BATCH_COMPATIBLE,
        "route-token",
        8,
        sessionResponse.getDataSchema(),
        false);
  }
}
