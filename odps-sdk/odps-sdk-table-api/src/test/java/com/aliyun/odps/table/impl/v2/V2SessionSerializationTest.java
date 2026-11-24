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

package com.aliyun.odps.table.impl.v2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.storage.internal.SessionDescriptorCodec;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.Session;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.SessionType;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.distribution.ClusteredDistribution;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableIncrementalReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplitWithBucket;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Test;

/** Tests the descriptor contract and Java proxy without any live service. */
public class V2SessionSerializationTest {
  private static final String DRIVER_ID = "fake-driver-id-must-not-travel";
  private static final String DRIVER_KEY = "fake-driver-key-must-not-travel";
  private static final TableIdentifier TABLE = TableIdentifier.of("proj", "default", "tbl");
  private static final String SCHEMA =
      "{\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"bigint\",\"Nullable\":true}],"
          + "\"PartitionColumns\":[{\"Name\":\"ds\",\"Type\":\"string\",\"Nullable\":true}]}";

  private static JsonObject metadata() {
    return JsonParser.parseString(
            "{\"SessionId\":\"s1\",\"SessionStatus\":\"NORMAL\","
                + "\"DataSchema\":"
                + SCHEMA
                + ",\"SplitMode\":\"Size\",\"SplitsCount\":3,"
                + "\"RecordCount\":-1,\"LatestVersion\":12,\"LatestTxnId\":\"txn-12\","
                + "\"ExtendedArrowIPCEnabled\":true,\"SupportedDataFormat\":[{\"Type\":\"Arrow\",\"Version\":\"V5\"}],"
                + "\"SessionStats\":{\"EstimatedSize\":123,\"EstimatedRowCount\":9}}")
        .getAsJsonObject();
  }

  private static ArrowOptions units() {
    return ArrowOptions.newBuilder()
        .withTimestampUnit(ArrowOptions.TimestampUnit.MICRO)
        .withDatetimeUnit(ArrowOptions.TimestampUnit.SECOND)
        .build();
  }

  private static String descriptor(String kind, JsonObject metadata) {
    return SessionDescriptorCodec.create(
            kind,
            TABLE,
            metadata,
            "fake-session-route",
            SessionDescriptorCodec.WRITE.equals(kind) ? units() : null)
        .toJson();
  }

  private static byte[] serialize(Object value) throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(buffer)) {
      output.writeObject(value);
    }
    return buffer.toByteArray();
  }

  private static Session roundTrip(Session session) throws Exception {
    try (ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(serialize(session)))) {
      return (Session) input.readObject();
    }
  }

  private static EnvironmentSettings settings(MockWebServer server) {
    return EnvironmentSettings.newBuilder()
        .withServiceEndpoint(server.url("/").toString())
        .withTunnelEndpoint(server.url("/").toString())
        .withCredentials(
            Credentials.newBuilder().withAccount(new AliyunAccount(DRIVER_ID, DRIVER_KEY)).build())
        .withRestOptions(RestOptions.newBuilder().withRetryTimes(0).build())
        .build();
  }

  private static void readMetadata(TableBatchReadSession session) {
    assertEquals("s1", session.getId());
    assertEquals("tbl", session.getTableIdentifier().getTable());
    assertEquals(SessionStatus.NORMAL, session.getStatus());
    assertEquals(2, session.readSchema().getColumnCount());
    assertTrue(
        session.supportsDataFormat(new DataFormat(DataFormat.Type.ARROW, DataFormat.Version.V5)));
    assertEquals(123L, session.getEstimatedStats().getEstimatedSize());
    assertEquals(9L, session.getEstimatedStats().getEstimatedRowCount());
    assertEquals("txn-12", session.getLatestTxnId().get());
  }

  @Test
  public void readMetadataAndSplitsSurviveJsonAndJavaWithoutRpc() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      for (String mode : new String[] {"Size", "RowOffset", "Bucket"}) {
        JsonObject data = metadata();
        data.addProperty("SplitMode", mode);
        if ("RowOffset".equals(mode)) data.addProperty("RecordCount", 9);
        if ("Bucket".equals(mode)) {
          data.addProperty("SplitsCount", 2);
          data.add("SplitBucketId", JsonParser.parseString("[2,7]"));
        }
        TableBatchReadSession json =
            (TableBatchReadSession)
                SessionSerialization.fromJson(descriptor(SessionDescriptorCodec.READ, data));
        TableBatchReadSession java = (TableBatchReadSession) roundTrip(json);
        readMetadata(json);
        readMetadata(java);
        assertEquals(Long.valueOf(12), java.getSnapshot().getVersion().get());
        if ("RowOffset".equals(mode)) {
          assertEquals(9L, java.getInputSplitAssigner().getTotalRowCount());
          assertEquals("s1", java.getInputSplitAssigner().getSplitByRowOffset(2, 3).getSessionId());
        } else {
          assertEquals(
              json.getInputSplitAssigner().getSplitsCount(),
              java.getInputSplitAssigner().getSplitsCount());
        }
        if ("Size".equals(mode)) assertEquals(3, java.getInputSplitAssigner().getSplitsCount());
        if ("Bucket".equals(mode))
          assertEquals(
              7, ((InputSplitWithBucket) java.getInputSplitAssigner().getSplit(1)).getBucketId());
        assertEquals(SessionSerialization.toJson(json), SessionSerialization.toJson(java));
      }
      assertEquals(0, server.getRequestCount());
    }
  }

  @Test
  public void incrementalKindAndOptionsSurviveJavaProxy() throws Exception {
    JsonObject data = metadata();
    data.add(
        "IncrementalReadOptions",
        JsonParser.parseString(
            "{\"StartVersion\":0,\"EndVersion\":12,\"Mode\":\"cdc\",\"StartTxnId\":\"txn-zero\"}"));
    Session restored =
        roundTrip(SessionSerialization.fromJson(descriptor(SessionDescriptorCodec.INCREMENTAL, data)));
    assertTrue(restored instanceof TableIncrementalReadSession);
    TableIncrementalReadSession read = (TableIncrementalReadSession) restored;
    readMetadata(read);
    assertEquals(SessionType.INCREMENTAL_READ, read.getType());
    assertEquals(
        Long.valueOf(0), read.getIncrementalOptions().getStartVersion().get().getVersion().get());
    assertEquals("txn-zero", read.getIncrementalOptions().getStartTxnId().get());
    assertEquals(
        IncrementalOptions.IncrementalMode.CDC, read.getIncrementalOptions().getIncrementalMode());
    assertEquals(
        SessionDescriptorCodec.INCREMENTAL,
        SessionDescriptorCodec.parse(SessionSerialization.toJson(read)).kind());
  }

  private static JsonObject writeMetadata() {
    JsonObject data = metadata();
    data.addProperty("MaxBlockNumber", 100);
    data.addProperty("EnhanceWriteCheck", true);
    data.add(
        "RequiredOrdering",
        JsonParser.parseString(
            "[{\"Name\":\"a\",\"SortDirection\":\"ASC\",\"NullOrdering\":\"NULLS_FIRST\"}]"));
    data.add(
        "RequiredDistribution",
        JsonParser.parseString("{\"Type\":\"HASH\",\"ClusterKeys\":[\"a\"],\"BucketsNumber\":4}"));
    return data;
  }

  @Test
  public void writeMetadataAndUnitsSurviveProxyAndDetachedControlHasNoRpc() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      server.enqueue(new MockResponse().setBody(writeMetadata().toString()));
      TableBatchWriteSession driver =
          new TableWriteSessionBuilder()
              .identifier(TABLE)
              .withSessionProvider("v2")
              .withSettings(settings(server))
              .withArrowOptions(units())
              .buildBatchWriteSession();
      int planned = server.getRequestCount();
      assertEquals(1, planned);
      for (Session copy :
          new Session[] {
            roundTrip(driver), SessionSerialization.fromJson(SessionSerialization.toJson(driver))
          }) {
        TableBatchWriteSession write = (TableBatchWriteSession) copy;
        assertEquals(2, write.requiredSchema().getColumnCount());
        assertEquals(Long.valueOf(100), write.maxBlockNumber().get());
        assertEquals("a", write.requiredOrdering()[0].name());
        assertEquals(
            Collections.singletonList("a"),
            ((ClusteredDistribution) write.requiredDistribution()).getClusterKeys());
        assertEquals(
            Integer.valueOf(4),
            ((ClusteredDistribution) write.requiredDistribution()).getBucketsNumber().get());
        ArrowOptions actual =
            SessionDescriptorCodec.parse(SessionSerialization.toJson(write)).arrowOptions();
        assertEquals(ArrowOptions.TimestampUnit.MICRO, actual.getTimestampUnit());
        assertEquals(ArrowOptions.TimestampUnit.SECOND, actual.getDateTimeUnit());
        assertRejected(() -> write.commit(new WriterCommitMessage[0]));
        write.cleanup();
        write.cleanup();
      }
      assertEquals(planned, server.getRequestCount());
      assertNoCredentials(driver);
    }
  }

  @Test
  public void driverReadCredentialsNeverTravelInJsonOrJava() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      JsonObject legacy = new JsonObject();
      legacy.addProperty("identifier", com.aliyun.odps.utils.JsonUtils.toJson(TABLE));
      legacy.addProperty("details", metadata().toString());
      TableBatchReadSession driver =
          new TableReadSessionBuilder()
              .withSessionProvider("v2")
              .fromJson(legacy.toString())
              .withSettings(settings(server))
              .buildBatchReadSession();
      assertNoCredentials(driver);
      readMetadata((TableBatchReadSession) roundTrip(driver));
      assertEquals(0, server.getRequestCount());
    }
  }

  private static void assertNoCredentials(Session session) throws Exception {
    for (String encoded :
        new String[] {
          SessionSerialization.toJson(session),
          new String(serialize(session), StandardCharsets.ISO_8859_1)
        }) {
      assertFalse(encoded.contains(DRIVER_ID));
      assertFalse(encoded.contains(DRIVER_KEY));
      assertFalse(encoded.contains("EnvironmentSettings"));
      assertFalse(encoded.contains("V2ClientHolder"));
    }
  }

  @Test
  public void detachedDataOperationsWithoutSettingsFailBeforeRpc() throws Exception {
    TableBatchReadSession read =
        (TableBatchReadSession)
            SessionSerialization.fromJson(descriptor(SessionDescriptorCodec.READ, metadata()));
    TableBatchWriteSession write =
        (TableBatchWriteSession)
            SessionSerialization.fromJson(descriptor(SessionDescriptorCodec.WRITE, writeMetadata()));
    // Public builders require settings already; exercise the session's own guard with defaults.
    ReaderOptions reader = defaults(ReaderOptions.class);
    WriterOptions writer = defaults(WriterOptions.class);
    assertRejected(() -> read.createArrowReader(read.getInputSplitAssigner().getSplit(0), reader));
    assertRejected(() -> write.createArrowWriter(0, WriterAttemptId.of(0), writer));
  }

  private static <T> T defaults(Class<T> type) throws Exception {
    Constructor<T> constructor = type.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  @Test
  public void unknownAndMalformedDescriptorsAreRejected() throws Exception {
    String valid = descriptor(SessionDescriptorCodec.WRITE, writeMetadata());
    for (String field : new String[] {"version", "provider", "kind"}) {
      JsonObject invalid = JsonParser.parseString(valid).getAsJsonObject();
      invalid.addProperty(field, "unsupported");
      assertRejected(() -> SessionSerialization.fromJson(invalid.toString()));
    }
    for (String field : new String[] {"table", "metadata", "arrow"}) {
      JsonObject invalid = JsonParser.parseString(valid).getAsJsonObject();
      invalid.remove(field);
      assertRejected(() -> SessionSerialization.fromJson(invalid.toString()));
    }
    for (String invalid : new String[] {"{", "[]", "null", "{}"})
      assertRejected(() -> SessionSerialization.fromJson(invalid));
  }

  @Test
  public void malformedBucketDescriptorsAreRejectedOffline() throws Exception {
    for (String buckets : new String[] {"[7]", "[7,null]", "[7,-1]"}) {
      JsonObject data = metadata();
      data.addProperty("SplitMode", "Bucket");
      data.addProperty("SplitsCount", 2);
      data.add("SplitBucketId", JsonParser.parseString(buckets));
      assertRejected(() -> descriptor(SessionDescriptorCodec.READ, data));
    }
  }

  private interface CheckedAction {
    void run() throws Exception;
  }

  private static void assertRejected(CheckedAction action) throws Exception {
    try {
      action.run();
    } catch (IOException | IllegalArgumentException | IllegalStateException expected) {
      return;
    }
    fail("Expected the operation to reject invalid or detached input");
  }

  @Test
  public void concurrentReadersBindIndependentWorkerSettings() throws Exception {
    JsonObject data = metadata();
    data.addProperty("ExtendedArrowIPCEnabled", false);
    TableBatchReadSession read =
        (TableBatchReadSession)
            SessionSerialization.fromJson(descriptor(SessionDescriptorCodec.READ, data));
    java.util.concurrent.ExecutorService pool =
        java.util.concurrent.Executors.newFixedThreadPool(2);
    try (MockWebServer first = new MockWebServer();
        MockWebServer second = new MockWebServer();
        org.apache.arrow.memory.RootAllocator allocator =
            new org.apache.arrow.memory.RootAllocator()) {
      first.start();
      second.start();
      MockWebServer[] workers = {first, second};
      java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
      for (int i = 0; i < workers.length; i++) {
        final int index = i;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (org.apache.arrow.vector.BigIntVector vector =
                new org.apache.arrow.vector.BigIntVector("a", allocator);
            org.apache.arrow.vector.VectorSchemaRoot root =
                org.apache.arrow.vector.VectorSchemaRoot.of(vector);
            org.apache.arrow.vector.ipc.ArrowStreamWriter out =
                new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, bytes)) {
          vector.allocateNew(1);
          vector.setSafe(0, i);
          root.setRowCount(1);
          out.start();
          out.writeBatch();
          out.end();
        }
        workers[i].enqueue(
            new MockResponse().setBody(new okio.Buffer().write(bytes.toByteArray())));
        String endpoint = workers[i].url("/").toString();
        EnvironmentSettings workerSettings =
            EnvironmentSettings.newBuilder()
                .withServiceEndpoint(endpoint)
                .withTunnelEndpoint(endpoint)
                .withSharingQuotaToken("quota-" + i)
                .withCredentials(
                    Credentials.newBuilder()
                        .withAccount(new AliyunAccount("worker-" + i, "fake-worker-key"))
                        .build())
                .build();
        futures.add(
            pool.submit(
                () -> {
                  try (com.aliyun.odps.table.read.SplitReader<
                          org.apache.arrow.vector.VectorSchemaRoot>
                      reader =
                          read.createArrowReader(
                              read.getInputSplitAssigner().getSplit(0),
                              ReaderOptions.newBuilder()
                                  .withSettings(workerSettings)
                                  .withBufferAllocator(allocator)
                                  .build())) {
                    assertTrue(reader.hasNext());
                    assertEquals(
                        index,
                        ((org.apache.arrow.vector.BigIntVector) reader.get().getVector(0)).get(0));
                    assertFalse(reader.hasNext());
                    reader.close(); // repeated close remains safe
                  }
                  return null;
                }));
      }
      for (java.util.concurrent.Future<?> future : futures)
        future.get(10, java.util.concurrent.TimeUnit.SECONDS);
      for (int i = 0; i < workers.length; i++) {
        okhttp3.mockwebserver.RecordedRequest request =
            workers[i].takeRequest(2, java.util.concurrent.TimeUnit.SECONDS);
        assertNotNull(request);
        assertTrue(request.getHeader("Authorization").contains("worker-" + i));
        assertEquals("quota-" + i, request.getRequestUrl().queryParameter("sharingQuotaToken"));
        assertEquals(1, workers[i].getRequestCount());
      }
      try (org.apache.arrow.memory.ArrowBuf stillBorrowed = allocator.buffer(8)) {
        assertEquals(8, stillBorrowed.capacity());
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void builderIncrementalOptionsSurviveWhenServerOmitsThem() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      JsonObject response = metadata();
      assertFalse(response.has("IncrementalReadOptions"));
      server.enqueue(new MockResponse().setBody(response.toString()));
      IncrementalOptions requested =
          IncrementalOptions.newBuilder()
              .startVersion(7L)
              .endVersion(13L)
              .startTxnId("builder-txn")
              .withIncrementalMode(IncrementalOptions.IncrementalMode.CDC)
              .build();
      TableIncrementalReadSession driver =
          new TableReadSessionBuilder()
              .identifier(TABLE)
              .withSessionProvider("v2")
              .withSettings(settings(server))
              .withIncrementalOptions(requested)
              .buildIncrementalReadSession();
      assertEquals(1, server.getRequestCount());
      for (Session copy :
          new Session[] {
            SessionSerialization.fromJson(SessionSerialization.toJson(driver)), roundTrip(driver)
          }) {
        assertTrue(copy instanceof TableIncrementalReadSession);
        IncrementalOptions actual = ((TableIncrementalReadSession) copy).getIncrementalOptions();
        assertNotNull(actual);
        assertEquals(Long.valueOf(7), actual.getStartVersion().get().getVersion().get());
        assertEquals(Long.valueOf(13), actual.getEndVersion().get().getVersion().get());
        assertEquals("builder-txn", actual.getStartTxnId().get());
        assertEquals(IncrementalOptions.IncrementalMode.CDC, actual.getIncrementalMode());
        assertEquals(
            SessionDescriptorCodec.INCREMENTAL,
            SessionDescriptorCodec.parse(SessionSerialization.toJson(copy)).kind());
      }
      assertEquals(
          "Restoring incremental options must remain offline", 1, server.getRequestCount());
    }
  }

  @Test
  public void commitStartedDescriptorRejectsNewWritersWithoutRpc() throws Exception {
    try (MockWebServer server = new MockWebServer();
        org.apache.arrow.memory.RootAllocator allocator =
            new org.apache.arrow.memory.RootAllocator()) {
      server.start();
      server.setDispatcher(
          new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
              return new MockResponse()
                  .setResponseCode(400)
                  .setBody("{\"Code\":\"UnexpectedRpc\",\"Message\":\"must reject offline\"}");
            }
          });
      JsonObject data = writeMetadata();
      // The last observed status can still be NORMAL after a commit request was sent.
      data.addProperty("CommitStarted", true);
      Session json = SessionSerialization.fromJson(descriptor(SessionDescriptorCodec.WRITE, data));
      WriterOptions options =
          WriterOptions.newBuilder()
              .withSettings(settings(server))
              .withBufferAllocator(allocator)
              .build();
      for (Session copy : new Session[] {json, roundTrip(json)}) {
        TableBatchWriteSession write = (TableBatchWriteSession) copy;
        assertEquals(SessionStatus.NORMAL, write.getStatus());
        assertTrue(
            SessionDescriptorCodec.parse(SessionSerialization.toJson(write))
                .metadata()
                .get("CommitStarted")
                .getAsBoolean());
        assertRejected(() -> write.createArrowWriter(0, WriterAttemptId.of(0), options));
        write.cleanup();
      }
      assertEquals(
          "CommitStarted must prevent quota reservation and abort", 0, server.getRequestCount());
      try (org.apache.arrow.memory.ArrowBuf borrowed = allocator.buffer(8)) {
        assertEquals(8, borrowed.capacity());
      }
    }
  }

  @Test
  public void fixedVersionOneJsonFixturesRestoreWithoutProducer() throws Exception {
    String readFixture =
        "{\"version\":1,\"provider\":\"v2\",\"kind\":\"batch-read\","
            + "\"table\":{\"project\":\"proj\",\"schema\":\"default\",\"table\":\"tbl\"},"
            + "\"metadata\":{\"sessionId\":\"golden-read\",\"sessionStatus\":\"NORMAL\","
            + "\"dataSchema\":{\"dataColumns\":[{\"name\":\"a\",\"type\":\"bigint\",\"nullable\":true}],\"partitionColumns\":[]},"
            + "\"splitMode\":\"Size\",\"splitsCount\":2,\"recordCount\":-1,\"latestVersion\":4,"
            + "\"supportedDataFormat\":[{\"type\":\"Arrow\",\"version\":\"V5\"}]}}";
    TableBatchReadSession read = (TableBatchReadSession) SessionSerialization.fromJson(readFixture);
    assertEquals("golden-read", read.getId());
    assertEquals(1, read.readSchema().getColumnCount());
    assertEquals(2, read.getInputSplitAssigner().getSplitsCount());
    assertEquals(Long.valueOf(4), read.getSnapshot().getVersion().get());
    assertTrue(
        read.supportsDataFormat(new DataFormat(DataFormat.Type.ARROW, DataFormat.Version.V5)));

    String writeFixture =
        "{\"version\":1,\"provider\":\"v2\",\"kind\":\"batch-write\","
            + "\"table\":{\"project\":\"proj\",\"schema\":\"default\",\"table\":\"tbl\"},"
            + "\"metadata\":{\"sessionId\":\"golden-write\",\"sessionStatus\":\"NORMAL\","
            + "\"dataSchema\":{\"dataColumns\":[{\"name\":\"a\",\"type\":\"bigint\",\"nullable\":true}],\"partitionColumns\":[]},"
            + "\"maxBlockNumber\":8,\"enhanceWriteCheck\":true,\"commitStarted\":false,"
            + "\"requiredOrdering\":[{\"name\":\"a\",\"sortDirection\":\"ASC\",\"nullOrdering\":\"NULLS_FIRST\"}],"
            + "\"requiredDistribution\":{\"type\":\"HASH\",\"clusterKeys\":[\"a\"],\"bucketsNumber\":4}},"
            + "\"arrow\":{\"timestampUnit\":\"MICRO\",\"datetimeUnit\":\"SECOND\"}}";
    TableBatchWriteSession write =
        (TableBatchWriteSession) SessionSerialization.fromJson(writeFixture);
    assertEquals("golden-write", write.getId());
    assertEquals(1, write.requiredSchema().getColumnCount());
    assertEquals(Long.valueOf(8), write.maxBlockNumber().get());
    assertEquals("a", write.requiredOrdering()[0].name());
    assertEquals(
        Integer.valueOf(4),
        ((ClusteredDistribution) write.requiredDistribution()).getBucketsNumber().get());
    ArrowOptions arrow = SessionDescriptorCodec.parse(SessionSerialization.toJson(write)).arrowOptions();
    assertEquals(ArrowOptions.TimestampUnit.MICRO, arrow.getTimestampUnit());
    assertEquals(ArrowOptions.TimestampUnit.SECOND, arrow.getDateTimeUnit());
  }
}
