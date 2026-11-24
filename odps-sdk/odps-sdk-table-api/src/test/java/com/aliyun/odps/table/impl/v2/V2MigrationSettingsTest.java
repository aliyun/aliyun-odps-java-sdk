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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.AppAccount;
import com.aliyun.odps.account.AppStsAccount;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.google.gson.JsonParser;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class V2MigrationSettingsTest {
  private MockWebServer server;
  private static final String SESSION =
      "{\"SessionId\":\"s\",\"SessionStatus\":\"NORMAL\",\"SplitMode\":\"Size\",\"SplitsCount\":1,\"DataSchema\":{\"DataColumns\":[{\"Name\":\"t\",\"Type\":\"TIMESTAMP\"},{\"Name\":\"d\",\"Type\":\"DATETIME\"}],\"PartitionColumns\":[]}}";

  @Before
  public void setup() throws Exception {
    server = new MockWebServer();
    server.start();
  }

  @After
  public void cleanup() throws Exception {
    server.shutdown();
  }

  private EnvironmentSettings.Builder settings() {
    String url = server.url("/").toString();
    return EnvironmentSettings.newBuilder()
        .withServiceEndpoint(url)
        .withTunnelEndpoint(url)
        .withCredentials(
            Credentials.newBuilder()
                .withAccount(new AliyunAccount("main-id", "main-secret"))
                .build());
  }

  private void enqueue() {
    server.enqueue(
        new MockResponse().setHeader("Content-Type", "application/json").setBody(SESSION));
  }

  private TableWriteSessionBuilder writer(EnvironmentSettings settings) {
    return new TableWriteSessionBuilder()
        .withSessionProvider("v2")
        .withSettings(settings)
        .identifier(TableIdentifier.of("proj", "tbl"));
  }

  @Test
  public void customArrowUnitsReachRequest() throws Exception {
    enqueue();
    writer(settings().build())
        .withArrowOptions(
            ArrowOptions.newBuilder()
                .withTimestampUnit(ArrowOptions.TimestampUnit.MICRO)
                .withDatetimeUnit(ArrowOptions.TimestampUnit.SECOND)
                .build())
        .buildBatchWriteSession();
    com.google.gson.JsonObject options =
        JsonParser.parseString(server.takeRequest(2, TimeUnit.SECONDS).getBody().readUtf8())
            .getAsJsonObject()
            .getAsJsonObject("ArrowOptions");
    assertEquals("micro", options.get("TimestampUnit").getAsString());
    assertEquals("second", options.get("DatetimeUnit").getAsString());
  }

  @Test
  public void sharingQuotaAndTagsReachReadAndWrite() throws Exception {
    EnvironmentSettings env =
        settings()
            .withSharingQuotaToken("test-share-token")
            .withTags(Arrays.asList("tag-one", "tag-two"))
            .build();
    enqueue();
    writer(env).buildBatchWriteSession();
    enqueue();
    new TableReadSessionBuilder()
        .withSessionProvider("v2")
        .withSettings(env)
        .identifier(TableIdentifier.of("proj", "tbl"))
        .buildBatchReadSession();
    for (int i = 0; i < 2; i++) {
      RecordedRequest r = server.takeRequest(2, TimeUnit.SECONDS);
      assertEquals("test-share-token", r.getRequestUrl().queryParameter("sharingQuotaToken"));
      assertEquals("tag-one,tag-two", r.getHeader("odps-tunnel-tags"));
    }
  }

  @Test
  public void bothApplicationSignaturesMatchPrimaryAuthorization() throws Exception {
    AppAccount app = new AppAccount(new AliyunAccount("app-id", "app-secret"));
    AppStsAccount sts =
        new AppStsAccount(new AliyunAccount("sts-id", "sts-secret"), "app-sts-token");
    EnvironmentSettings env =
        settings()
            .withCredentials(
                Credentials.newBuilder()
                    .withAccount(new AliyunAccount("main-id", "main-secret"))
                    .withAppAccount(app)
                    .withAppStsAccount(sts)
                    .build())
            .build();
    enqueue();
    writer(env).buildBatchWriteSession();
    RecordedRequest actual = server.takeRequest(2, TimeUnit.SECONDS);
    com.aliyun.odps.commons.transport.Request expected =
        new com.aliyun.odps.commons.transport.Request();
    expected.setHeader(Headers.AUTHORIZATION, actual.getHeader(Headers.AUTHORIZATION));
    app.getRequestSigner().sign("", expected);
    sts.getRequestSigner().sign("", expected);
    assertEquals(
        expected.getHeaders().get(Headers.APP_AUTHENTICATION),
        actual.getHeader(Headers.APP_AUTHENTICATION));
    assertEquals(
        expected.getHeaders().get(Headers.STS_AUTHENTICATION),
        actual.getHeader(Headers.STS_AUTHENTICATION));
    assertEquals("app-sts-token", actual.getHeader(Headers.STS_TOKEN));
  }

  @Test
  public void explicitRetryCountStopsTransientErrors() throws Exception {
    for (int i = 0; i < 4; i++)
      server.enqueue(
          new MockResponse().setResponseCode(500).setBody("{\"Message\":\"retry probe\"}"));
    try {
      writer(
              settings()
                  .withRestOptions(
                      RestOptions.newBuilder()
                          .withRetryTimes(1)
                          .withRetryWaitTimeInSeconds(0)
                          .build())
                  .build())
          .buildBatchWriteSession();
      fail("expected service error");
    } catch (com.aliyun.odps.storage.ServiceException expected) {
      assertEquals(2, server.getRequestCount());
    }
  }

  @Test
  public void customUserAgentReachesWire() throws Exception {
    enqueue();
    writer(
            settings()
                .withRestOptions(RestOptions.newBuilder().witUserAgent("migration-agent").build())
                .build())
        .buildBatchWriteSession();
    assertTrue(
        server
            .takeRequest(2, TimeUnit.SECONDS)
            .getHeader("User-Agent")
            .contains("migration-agent"));
  }

  @Test
  public void newAndReloadedWritersUseRequestedArrowUnits() throws Exception {
    for (boolean reload : new boolean[] {false, true}) {
      ArrowOptions.Builder arrow =
          ArrowOptions.newBuilder()
              .withTimestampUnit(ArrowOptions.TimestampUnit.MICRO)
              .withDatetimeUnit(ArrowOptions.TimestampUnit.SECOND);
      TableWriteSessionBuilder builder = writer(settings().build()).withArrowOptions(arrow.build());
      enqueue();
      if (reload) {
        enqueue();
        builder.withSessionId("s");
      }
      TableBatchWriteSession session = builder.buildBatchWriteSession();
      // A retained builder must not mutate the already negotiated wire/schema contract.
      arrow.withTimestampUnit(ArrowOptions.TimestampUnit.NANO);
      int requestCount = server.getRequestCount();
      try (RootAllocator allocator = new RootAllocator()) {
        BatchWriter<VectorSchemaRoot> writer =
            session.createArrowWriter(
                0,
                WriterAttemptId.of(0),
                WriterOptions.newBuilder()
                    .withSettings(builder.getSettings())
                    .withBufferAllocator(allocator)
                    .build());
        try (VectorSchemaRoot root = writer.newElement()) {
          assertEquals(
              org.apache.arrow.vector.types.TimeUnit.MICROSECOND,
              ((ArrowType.Timestamp) root.getSchema().findField("t").getType()).getUnit());
          assertEquals(
              org.apache.arrow.vector.types.TimeUnit.SECOND,
              ((ArrowType.Timestamp) root.getSchema().findField("d").getType()).getUnit());
        } finally {
          writer.abort();
        }
        assertEquals(requestCount, server.getRequestCount());
      }
    }
  }

  @Test
  public void zeroRetriesStopsThrottlingAndPermanentErrorsAreNotRetried() throws Exception {
    for (int status : new int[] {429, 403}) {
      server.enqueue(
          new MockResponse().setResponseCode(status).setBody("{\"Message\":\"failure\"}"));
      int before = server.getRequestCount();
      try {
        writer(
                settings()
                    .withRestOptions(
                        RestOptions.newBuilder()
                            .withRetryTimes(status == 429 ? 0 : 3)
                            .withRetryWaitTimeInSeconds(0)
                            .build())
                    .build())
            .buildBatchWriteSession();
        fail("expected service error");
      } catch (com.aliyun.odps.storage.ServiceException expected) {
        assertEquals(before + 1, server.getRequestCount());
      }
    }
  }

  @Test
  public void readTimeoutIsEnforced() throws Exception {
    server.enqueue(new MockResponse().setBody(SESSION).setBodyDelay(3, TimeUnit.SECONDS));
    long start = System.nanoTime();
    try {
      writer(
              settings()
                  .withRestOptions(
                      RestOptions.newBuilder().withReadTimeout(1).withRetryTimes(0).build())
                  .build())
          .buildBatchWriteSession();
      fail("expected read timeout");
    } catch (com.aliyun.odps.storage.ClientException expected) {
      assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 2500);
      assertEquals(1, server.getRequestCount());
    }
  }

  @Test
  public void allTimeoutsReachHttpTransport() throws Exception {
    V2ClientHolder holder =
        new V2ClientHolder(
            TableIdentifier.of("proj", "tbl"),
            settings()
                .withRestOptions(
                    RestOptions.newBuilder()
                        .withConnectTimeout(2)
                        .withReadTimeout(3)
                        .withWriteTimeout(4)
                        .build())
                .build());
    com.aliyun.odps.storage.internal.StorageStub stub = holder.stub();
    java.lang.reflect.Field http = stub.getClass().getDeclaredField("httpClient");
    http.setAccessible(true);
    Object transport = http.get(stub);
    java.lang.reflect.Field clientField = transport.getClass().getDeclaredField("client");
    clientField.setAccessible(true);
    okhttp3.OkHttpClient client = (okhttp3.OkHttpClient) clientField.get(transport);
    assertEquals(2000, client.connectTimeoutMillis());
    assertEquals(3000, client.readTimeoutMillis());
    assertEquals(4000, client.writeTimeoutMillis());
  }

  @Test
  public void certificateChecksAreDisabledOnlyByExplicitOption() throws Exception {
    okhttp3.tls.HeldCertificate certificate = new okhttp3.tls.HeldCertificate.Builder()
        .commonName("untrusted.invalid")
        .addSubjectAlternativeName("untrusted.invalid")
        .build();
    okhttp3.tls.HandshakeCertificates certificates = new okhttp3.tls.HandshakeCertificates.Builder()
        .heldCertificate(certificate)
        .build();
    server.useHttps(certificates.sslSocketFactory(), false);
    // Self-signed certificate AND a deliberately mismatched hostname.
    enqueue();
    try {
      writer(settings().withRestOptions(RestOptions.newBuilder().withRetryTimes(0).build()).build())
          .buildBatchWriteSession();
      fail("default TLS must reject the test certificate");
    } catch (com.aliyun.odps.storage.ClientException expected) {
      assertEquals(0, server.getRequestCount());
    }
    writer(
            settings()
                .withRestOptions(
                    RestOptions.newBuilder().withIgnoreCerts(true).withRetryTimes(0).build())
                .build())
        .buildBatchWriteSession();
    assertEquals(1, server.getRequestCount());
  }

  @Test
  public void retryDelayAppliesBeforeSuccessfulRetry() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(503).setBody("{\"Message\":\"try again\"}"));
    enqueue();
    long start = System.nanoTime();
    writer(
            settings()
                .withRestOptions(
                    RestOptions.newBuilder()
                        .withRetryTimes(1)
                        .withRetryWaitTimeInSeconds(1)
                        .build())
                .build())
        .buildBatchWriteSession();
    assertEquals(2, server.getRequestCount());
    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= 1000);
  }
}
