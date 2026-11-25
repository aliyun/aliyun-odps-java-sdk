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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aliyun.odps.credentials.StaticCredentialProvider;
import com.aliyun.odps.storage.internal.SessionDescriptorCodec;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.StubSettings;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionResponse;
import com.aliyun.odps.storage.internal.models.CreateTableReadStreamRequest;
import com.aliyun.odps.storage.internal.models.ReadSchema;
import com.aliyun.odps.storage.models.SessionDescriptor;
import com.aliyun.odps.storage.models.SplitMode;
import com.aliyun.odps.storage.settings.HttpSettings;
import com.aliyun.odps.storage.settings.IncrementalReadOptions;
import com.aliyun.odps.storage.settings.SplitOptions;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowReader;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitWithRowRange;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Test;

class NativeReadSessionDescriptorTest {
  private static final TableIdentifier TABLE = TableIdentifier.of("proj", "default", "tbl");

  private static CreateTableReadSessionResponse response() {
    return StorageStub.createWireGson()
        .fromJson(
            "{\"SessionId\":\"read-session\",\"SessionStatus\":\"NORMAL\","
                + "\"SplitMode\":\"Size\",\"SplitsCount\":2,\"RecordCount\":8,"
                + "\"DataSchema\":{\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"bigint\",\"ColumnId\":7}],"
                + "\"PartitionColumns\":[],\"SystemColumns\":[{\"Name\":\"_op\",\"Type\":\"bigint\",\"ColumnId\":8}],"
                + "\"BlobColumns\":[{\"Name\":\"payload\",\"Type\":\"string\",\"ColumnId\":9}]}}",
            CreateTableReadSessionResponse.class);
  }

  @Test
  void restoresMetadataWithoutAnyClientCallsAndSnapshotsMutableResponse() {
    CreateTableReadSessionResponse response = response();
    response.setRouteToken("route-test");
    TableReadSession original =
        new TableReadSession(
            null, TABLE, null, response, new TableReadSessionBuilder(null, null, TABLE));
    response.setSessionId("mutated");
    response.getDataSchema().setColumns(java.util.Collections.emptyList());
    SessionDescriptor descriptor = SessionDescriptor.fromJson(original.getDescriptor().toJson());
    // A null stub makes any accidental create/get call fail immediately.
    TableReadSession restored =
        new TableReadSessionBuilder(null, null, TABLE).withSessionDescriptor(descriptor).build();
    assertEquals("read-session", restored.getId());
    assertEquals("route-test", restored.getRouteToken());
    assertEquals(2, restored.getSplits().size());
    ReadSchema schema = (ReadSchema) restored.getTableSchema();
    assertEquals(Long.valueOf(7), schema.getColumn("a").getColumnId());
    assertEquals(Long.valueOf(8), schema.getSystemColumns().get(0).getColumnId());
    assertEquals(Long.valueOf(9), schema.getBlobColumns().get(0).getColumnId());
    assertEquals(descriptor.toJson(), restored.getDescriptor().toJson());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new TableReadSessionBuilder(null, null, TableIdentifier.of("proj", "default", "other"))
                .withSessionDescriptor(descriptor));
  }

  @Test
  void rowOffsetRestorationRetainsOriginalChunkSize() {
    CreateTableReadSessionResponse response = response();
    response.setSplitMode(SplitMode.ROW_OFFSET);
    TableReadSession original =
        new TableReadSession(
            null,
            TABLE,
            null,
            response,
            new TableReadSessionBuilder(null, null, TABLE)
                .withSplitOptions(SplitOptions.newBuilder().withSplitRowCount(3).build()));
    TableReadSession restored =
        new TableReadSessionBuilder(null, null, TABLE)
            .withSessionDescriptor(SessionDescriptor.fromJson(original.getDescriptor().toJson()))
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new TableReadSessionBuilder(null, null, TABLE)
                .withSplitOptions(SplitOptions.newBuilder().withSplitRowCount(100).build())
                .withSessionDescriptor(original.getDescriptor())
                .build());
    assertEquals(3, restored.getSplits().size());
    for (int i = 0; i < 3; i++) {
      InputSplitWithRowRange split = (InputSplitWithRowRange) restored.getSplits().get(i);
      assertEquals(i * 3L, split.getRowRange().getStartIndex());
      assertEquals(i == 2 ? 2 : 3, split.getRowRange().getNumRecord());
    }
    assertEquals(3, SessionDescriptorCodec.parse(restored.getDescriptor().toJson()).rowsPerSplit());
  }

  @Test
  void incrementalOptionsOmittedByServerSurviveRestore() {
    TableReadSession original =
        new TableReadSession(
            null,
            TABLE,
            null,
            response(),
            new TableReadSessionBuilder(null, null, TABLE)
                .withIncrementalReadOptions(
                    IncrementalReadOptions.newBuilder()
                        .withMode("version")
                        .withStartVersion(10)
                        .withEndVersion(20)
                        .build()));
    SessionDescriptor descriptor = SessionDescriptor.fromJson(original.getDescriptor().toJson());
    assertEquals(SessionDescriptor.Kind.INCREMENTAL_READ, descriptor.getKind());
    TableReadSession restored =
        new TableReadSessionBuilder(null, null, TABLE).withSessionDescriptor(descriptor).build();
    CreateTableReadSessionResponse metadata =
        StorageStub.createWireGson()
            .fromJson(
                SessionDescriptorCodec.parse(restored.getDescriptor().toJson()).metadata(),
                CreateTableReadSessionResponse.class);
    assertEquals(10, metadata.getIncrementalReadOptions().getStartVersion());
    assertEquals(20, metadata.getIncrementalReadOptions().getEndVersion());
  }

  @Test
  void restoredWorkerReadsExtendedStreamUsingBorrowedAllocatorAndRoute() throws Exception {
    try (RootAllocator allocator = new RootAllocator()) {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      output.write(1);
      try (BigIntVector vector = new BigIntVector("a", allocator)) {
        vector.allocateNew(1);
        vector.setSafe(0, 42);
        vector.setValueCount(1);
        try (VectorSchemaRoot root = VectorSchemaRoot.of(vector);
            ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
          root.setRowCount(1);
          writer.start();
          writer.writeBatch();
          writer.end();
        }
      }
      int[] calls = {0};
      StorageStub worker =
          new StorageStub(
              StubSettings.newBuilder()
                  .withEndpoint("http://localhost:1")
                  .withTunnelEndpoint("http://localhost:1")
                  .withProject("proj")
                  .withCredentialsProvider(
                      StaticCredentialProvider.of("fake-worker-ak", "fake-worker-sk"))
                  .withHttpSettings(HttpSettings.newBuilder().build())
                  .build()) {
            @Override
            public InputStream createTableReadStream(
                TableIdentifier table,
                InputSplit split,
                CreateTableReadStreamRequest request,
                String routeToken) {
              calls[0]++;
              assertEquals(TABLE, table);
              assertEquals("route-test", routeToken);
              return new ByteArrayInputStream(output.toByteArray());
            }
          };
      CreateTableReadSessionResponse response = response();
      response.setExtendedArrowIPCEnabled(true);
      response.setRouteToken("route-test");
      SessionDescriptor descriptor =
          new TableReadSession(
                  null, TABLE, null, response, new TableReadSessionBuilder(null, null, TABLE))
              .getDescriptor();
      assertFalse(descriptor.toJson().contains("fake-worker"));
      try (TableReadSession restored =
          new TableReadSessionBuilder(worker, allocator, TABLE)
              .withSessionDescriptor(descriptor)
              .build()) {
        assertEquals(0, calls[0]);
        try (ArrowReader reader =
            restored.createReaderBuilder(restored.getSplits().get(0)).withAsync(false).build()) {
          assertTrue(reader.nextBatch());
          assertEquals(42, ((BigIntVector) reader.getCurrentValue().getVector("a")).get(0));
          assertFalse(reader.nextBatch());
        }
      }
      assertEquals(1, calls[0]);
      assertEquals(0, allocator.getAllocatedMemory());
      try (org.apache.arrow.memory.ArrowBuf buffer = allocator.buffer(8)) {
        buffer.setLong(0, 123);
        assertEquals(123, buffer.getLong(0));
      }
    }
  }
}
