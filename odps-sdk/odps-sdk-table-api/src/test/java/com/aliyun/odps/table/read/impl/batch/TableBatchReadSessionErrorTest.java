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

package com.aliyun.odps.table.read.impl.batch;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplitAssigner;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Failure reporting of the batch read session lifecycle, exercised against a loopback mock
 * server: no cloud credentials and no real MaxCompute endpoint are involved.
 *
 * <p>The contract these cases pin: a read session that cannot be created throws an
 * {@link IOException} whose text names the HTTP status, the {@code x-odps-request-id} and the
 * session id whenever those are known, and never carries an empty message. The reported symptom
 * was a bare "java.io.IOException" line, produced by {@code new IOException(e.getMessage(), e)}
 * around a cause whose message was null.
 */
public class TableBatchReadSessionErrorTest {

    private static final String DATA_SCHEMA = "\"DataSchema\":{\"DataColumns\":["
            + "{\"Name\":\"a\",\"Type\":\"BIGINT\",\"Nullable\":true}],"
            + "\"PartitionColumns\":[],\"SystemColumns\":[]}";

    private static final AtomicInteger SEQUENCE = new AtomicInteger();

    private MockWebServer server;
    private EnvironmentSettings settings;

    @Before
    public void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        String endpoint = server.url("/").toString();
        settings = EnvironmentSettings.newBuilder()
                .withCredentials(Credentials.newBuilder()
                        .withAccount(new AliyunAccount("test-id", "test-key")).build())
                .withServiceEndpoint(endpoint)
                .withTunnelEndpoint(endpoint)
                .build();
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }

    private TableReadSessionBuilder builder() {
        return new TableReadSessionBuilder()
                .identifier(TableIdentifier.of("proj", "default", "tbl"))
                .withSettings(settings);
    }

    private String newRequestId() {
        return "mock-req-" + SEQUENCE.incrementAndGet();
    }

    private void enqueue(String requestId, int status, String body) {
        MockResponse response = new MockResponse()
                .setResponseCode(status)
                .setHeader("x-odps-request-id", requestId);
        if (body == null) {
            response.setBody("");
        } else {
            response.setHeader("Content-Type",
                    body.startsWith("{") ? "application/json" : "text/plain");
            response.setBody(body);
        }
        server.enqueue(response);
    }

    private String odpsErrorBody(String requestId) {
        return "{\"RequestId\":\"" + requestId + "\",\"Code\":\"NoPermission\","
                + "\"Message\":\"Access denied by policy\"}";
    }

    private interface SessionBuild {
        TableBatchReadSession build() throws Exception;
    }

    private IOException captureFailure(SessionBuild build) {
        try {
            build.build();
        } catch (IOException expected) {
            return expected;
        } catch (Exception other) {
            fail("expected an IOException but got " + other.getClass().getName() + ": "
                    + other.getMessage());
            return null;
        }
        fail("expected the session creation to fail");
        return null;
    }

    /** A 4xx from the service must surface HTTP status, request id and the server message. */
    @Test
    public void serverErrorKeepsHttpStatusRequestIdAndServerMessage() {
        String requestId = newRequestId();
        enqueue(requestId, 403, odpsErrorBody(requestId));
        IOException failure = captureFailure(() -> builder().buildBatchReadSession());

        assertTrue("the service exception must stay the cause, was " + failure.getCause(),
                failure.getCause() instanceof OdpsException);
        assertEquals(Integer.valueOf(403), ((OdpsException) failure.getCause()).getStatus());

        String message = failure.getMessage();
        assertNotNull("a read session failure must never carry a null message", message);
        assertTrue("http status missing from: " + message, message.contains("403"));
        assertTrue("request id missing from: " + message, message.contains(requestId));
        assertTrue("server message dropped from: " + message,
                message.contains("Access denied by policy"));
        assertTrue("table identifier missing from: " + message, message.contains("proj"));
    }

    /**
     * A failure body that is not an ODPS error (proxy or gateway text) must still report the
     * request id the transport saw in {@code x-odps-request-id}.
     */
    @Test
    public void gatewayFailureWithoutOdpsErrorBodyKeepsRequestIdAndStatus() {
        String requestId = newRequestId();
        enqueue(requestId, 502, "502 Bad Gateway");
        IOException failure = captureFailure(() -> builder().buildBatchReadSession());

        String message = failure.getMessage();
        assertNotNull(message);
        assertTrue("http status missing from: " + message, message.contains("502"));
        assertTrue("request id missing from: " + message, message.contains(requestId));
    }

    /**
     * The reported symptom: a cause with a null message used to be rethrown as
     * {@code new IOException(null, cause)}, i.e. an exception line with no text at all.
     */
    @Test
    public void nullMessageCauseIsReportedWithSessionContext() {
        String requestId = newRequestId();
        enqueue(requestId, 200, "{\"SessionId\":\"s-bucket\",\"SessionStatus\":\"NORMAL\","
                + DATA_SCHEMA + ",\"SplitMode\":\"Bucket\",\"SplitsCount\":2}");
        IOException failure = captureFailure(() -> builder().buildBatchReadSession());

        String message = failure.getMessage();
        assertNotNull("a failure whose cause has no message must not be silent", message);
        assertFalse("the message must not be empty", message.trim().isEmpty());
        assertTrue("session id missing from: " + message, message.contains("s-bucket"));
        assertTrue("request id missing from: " + message, message.contains(requestId));
        assertTrue("session status missing from: " + message, message.contains("NORMAL"));
        assertTrue("cause type missing from: " + message,
                message.contains(failure.getCause().getClass().getName()));
    }

    /**
     * SplitMode=Bucket without SplitBucketId violates the session response contract; it must fail
     * by naming the missing field instead of dereferencing a null.
     */
    @Test
    public void bucketSplitModeWithoutBucketIdsNamesTheMissingField() {
        enqueue(newRequestId(), 200, "{\"SessionId\":\"s-bucket\",\"SessionStatus\":\"NORMAL\","
                + DATA_SCHEMA + ",\"SplitMode\":\"Bucket\",\"SplitsCount\":2}");
        IOException failure = captureFailure(() -> builder().buildBatchReadSession());

        Throwable cause = failure.getCause();
        assertEquals("expected a field contract error, got " + cause,
                IllegalArgumentException.class, cause.getClass());
        assertTrue("missing field name absent from: " + cause.getMessage(),
                cause.getMessage().contains("SplitBucketId"));
    }

    /** SessionStats without its counters must fail by naming the field, not by a null dereference. */
    @Test
    public void sessionStatsWithoutCountersNamesTheMissingField() {
        enqueue(newRequestId(), 200, "{\"SessionId\":\"s-stats\",\"SessionStatus\":\"NORMAL\","
                + DATA_SCHEMA + ",\"SplitMode\":\"Size\",\"SplitsCount\":1,\"SessionStats\":{}}");
        IOException failure = captureFailure(() -> builder().buildBatchReadSession());

        Throwable cause = failure.getCause();
        assertEquals("expected a field contract error, got " + cause,
                IllegalArgumentException.class, cause.getClass());
        assertTrue("missing field name absent from: " + cause.getMessage(),
                cause.getMessage().contains("EstimatedSize"));
    }

    /**
     * Regression pin for the original report: a session response without {@code SplitMode} is
     * legal for older service versions and must create the session instead of dereferencing null.
     */
    @Test
    public void responseWithoutSplitModeStillCreatesSession() throws Exception {
        enqueue(newRequestId(), 200, "{\"SessionId\":\"s-legacy\",\"SessionStatus\":\"NORMAL\","
                + DATA_SCHEMA + ",\"SplitsCount\":4}");
        TableBatchReadSession session = builder().buildBatchReadSession();

        assertEquals("s-legacy", session.getId());
        assertSame(IndexedInputSplitAssigner.class, session.getInputSplitAssigner().getClass());
        assertEquals(4, session.getInputSplitAssigner().getAllSplits().length);
    }

    /** Restoring an existing session takes the other failure path; the same contract applies. */
    @Test
    public void reloadFailureKeepsEndpointStatusAndRequestId() {
        String requestId = newRequestId();
        enqueue(requestId, 403, odpsErrorBody(requestId));
        IOException failure = captureFailure(
                () -> builder().withSessionId("s-existing").buildBatchReadSession());

        String message = failure.getMessage();
        assertNotNull(message);
        assertTrue("endpoint wording changed: " + message,
                message.startsWith("Failed to reload table read session with endpoint:"));
        assertTrue("http status missing from: " + message, message.contains("403"));
        assertTrue("request id missing from: " + message, message.contains(requestId));
        assertTrue("session id missing from: " + message, message.contains("s-existing"));
    }

    /** A successful create response must not supply context for a later failed poll. */
    @Test(timeout = 10000)
    public void pollFailureDoesNotReportPreviousRequestIdOrStatus() {
        String createRequestId = newRequestId();
        enqueue(createRequestId, 200, "{\"SessionId\":\"s-poll\",\"SessionStatus\":\"INIT\"}");
        server.enqueue(new MockResponse().setResponseCode(403).setBody("Access denied"));
        settings = EnvironmentSettings.newBuilder()
                .withCredentials(Credentials.newBuilder()
                        .withAccount(new AliyunAccount("test-id", "test-key")).build())
                .withServiceEndpoint(server.url("/").toString())
                .withTunnelEndpoint(server.url("/").toString())
                .withRestOptions(RestOptions.newBuilder()
                        .withRetryTimes(0).withAsyncIntervalInMillis(1).build())
                .build();

        IOException failure = captureFailure(() -> builder().buildBatchReadSession());
        String message = failure.getMessage();
        assertTrue(message, message.contains("403"));
        assertFalse(message, message.contains(createRequestId));
        assertFalse(message, message.contains("HTTP status: 200"));
    }

    /** An empty failure body must still say something other than "java.io.IOException". */
    @Test
    public void emptyFailureBodyStillReportsSomething() {
        String requestId = newRequestId();
        enqueue(requestId, 500, null);
        IOException failure = captureFailure(() -> builder().buildBatchReadSession());

        String message = failure.getMessage();
        assertNotNull("an empty error body must not turn into a silent failure", message);
        assertFalse("the message must not be empty", message.trim().isEmpty());
        assertTrue("cause type missing from: " + message,
                message.contains(failure.getCause().getClass().getName()));
    }
}
