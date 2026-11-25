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

import static org.junit.Assert.*;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Test;

/** Production commit-state contracts exercised with fake credentials and local HTTP only. */
public class V2CommitRecoveryTest {
  private static final WriterCommitMessage[] EMPTY = new WriterCommitMessage[0];

  private static final class Scenario extends Dispatcher {
    private final String[] commits;
    private final String[] gets;
    private final AtomicInteger commitCount = new AtomicInteger();
    private final AtomicInteger getCount = new AtomicInteger();
    private final AtomicInteger abortCount = new AtomicInteger();
    private final List<String> commitBodies = new CopyOnWriteArrayList<>();
    private final List<Long> getTimes = new CopyOnWriteArrayList<>();

    private Scenario(String[] commits, String... gets) {
      this.commits = commits;
      this.gets = gets;
    }

    @Override
    public MockResponse dispatch(RecordedRequest request) {
      String action = request.getRequestUrl().queryParameter("Action");
      if ("TableCreateWriteSession".equals(action)) {
        return status("NORMAL");
      }
      if ("TableCommitWriteSession".equals(action)) {
        commitBodies.add(request.getBody().readUtf8());
        return status(at(commits, commitCount.getAndIncrement()));
      }
      if ("TableGetWriteSession".equals(action)) {
        getTimes.add(System.nanoTime());
        return status(at(gets, getCount.getAndIncrement()));
      }
      if ("TableAbortWriteSession".equals(action)) {
        abortCount.incrementAndGet();
        return status("ABORTED");
      }
      return new MockResponse().setResponseCode(400).setBody("unexpected action: " + action);
    }

    private static String at(String[] states, int index) {
      return states.length == 0 ? "NORMAL" : states[Math.min(index, states.length - 1)];
    }

    private static MockResponse status(String state) {
      if ("500".equals(state)) {
        return new MockResponse()
            .setResponseCode(500)
            .setBody("{\"Code\":\"InternalServerError\",\"Message\":\"commit reply lost\"}");
      }
      return new MockResponse()
          .setBody(
              "{\"SessionId\":\"s\",\"SessionStatus\":\""
                  + state
                  + "\",\"DataSchema\":{\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"BIGINT\"}],"
                  + "\"PartitionColumns\":[]}}");
    }
  }

  private TableBatchWriteSession session(
      MockWebServer server, Scenario scenario, long intervalMillis, int timeoutSeconds)
      throws Exception {
    server.setDispatcher(scenario);
    server.start();
    String endpoint = server.url("/").toString();
    EnvironmentSettings settings =
        EnvironmentSettings.newBuilder()
            .withServiceEndpoint(endpoint)
            .withTunnelEndpoint(endpoint)
            .withCredentials(
                Credentials.newBuilder()
                    .withAccount(new AliyunAccount("fake-id", "fake-key"))
                    .build())
            .withRestOptions(
                RestOptions.newBuilder()
                    .withRetryTimes(0)
                    .withReadTimeout(3)
                    .withWriteTimeout(3)
                    .withAsyncIntervalInMillis(intervalMillis)
                    .withAsyncTimeoutInSeconds(timeoutSeconds)
                    .build())
            .build();
    return new TableWriteSessionBuilder()
        .withSessionProvider("v2")
        .identifier(TableIdentifier.of("p", "t"))
        .withSettings(settings)
        .buildBatchWriteSession();
  }

  @Test(timeout = 10000)
  public void lostReplyPollsCommittingWithoutResendingCommit() throws Exception {
    Scenario scenario = new Scenario(new String[] {"500"}, "COMMITTING", "COMMITTED");
    try (MockWebServer server = new MockWebServer()) {
      TableBatchWriteSession session = session(server, scenario, 100, 3);
      session.commit(EMPTY);
      assertEquals(SessionStatus.COMMITTED, session.getStatus());
      assertEquals(1, scenario.commitCount.get());
      assertEquals(2, scenario.getCount.get());
      assertTrue(
          "polls must honor the configured interval",
          TimeUnit.NANOSECONDS.toMillis(scenario.getTimes.get(1) - scenario.getTimes.get(0)) >= 80);
    }
  }

  @Test(timeout = 10000)
  public void committingTimeoutIsUnknownAndCleanupDoesNotAbort() throws Exception {
    Scenario scenario = new Scenario(new String[] {"COMMITTING"}, "COMMITTING");
    try (MockWebServer server = new MockWebServer()) {
      TableBatchWriteSession session = session(server, scenario, 25, 1);
      try {
        session.commit(EMPTY);
        fail("an unresolved commit cannot report success");
      } catch (IOException expected) {
        assertTrue(
            "timeout must identify the unknown commit outcome: " + expected,
            expected.getMessage().toLowerCase(Locale.ROOT).contains("unknown"));
      }
      session.cleanup();
      assertEquals(1, scenario.commitCount.get());
      assertEquals(0, scenario.abortCount.get());
    }
  }

  @Test(timeout = 10000)
  public void interruptedPollingPreservesFlagAndCleanupDoesNotAbort() throws Exception {
    Scenario scenario = new Scenario(new String[] {"COMMITTING"}, "COMMITTING");
    try (MockWebServer server = new MockWebServer()) {
      TableBatchWriteSession session = session(server, scenario, 1000, 5);
      AtomicReference<Throwable> failure = new AtomicReference<>();
      AtomicBoolean interrupted = new AtomicBoolean();
      Thread caller =
          new Thread(
              () -> {
                try {
                  session.commit(EMPTY);
                  failure.set(new AssertionError("interrupted commit cannot report success"));
                } catch (Throwable expected) {
                  failure.set(expected);
                  interrupted.set(Thread.currentThread().isInterrupted());
                }
              },
              "commit-interruption-test");
      caller.setDaemon(true);
      caller.start();
      try {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (caller.isAlive()
            && System.nanoTime() < deadline
            && !(session.getStatus() == SessionStatus.COMMITTING
                && caller.getState() == Thread.State.TIMED_WAITING)) {
          Thread.sleep(5);
        }
        assertTrue("caller should be waiting between commit-status polls", caller.isAlive());
        assertEquals(SessionStatus.COMMITTING, session.getStatus());
        assertEquals(Thread.State.TIMED_WAITING, caller.getState());
        caller.interrupt();
        caller.join(2000);
        assertFalse("interruption must stop polling promptly", caller.isAlive());
        assertTrue(
            "expected IOException but got " + failure.get(), failure.get() instanceof IOException);
        assertTrue("commit must restore the interrupt flag", interrupted.get());
        session.cleanup();
        assertEquals(0, scenario.abortCount.get());
        assertEquals(1, scenario.commitCount.get());
      } finally {
        caller.interrupt();
        caller.join(2000);
      }
    }
  }

  @Test(timeout = 10000)
  public void committedCleanupAndRepeatedCommitAreNoOps() throws Exception {
    Scenario scenario = new Scenario(new String[] {"COMMITTED"});
    try (MockWebServer server = new MockWebServer()) {
      TableBatchWriteSession session = session(server, scenario, 25, 2);
      session.commit(EMPTY);
      int requests = server.getRequestCount();
      session.cleanup();
      session.commit(EMPTY);
      assertEquals(SessionStatus.COMMITTED, session.getStatus());
      assertEquals(requests, server.getRequestCount());
      assertEquals(0, scenario.abortCount.get());
    }
  }

  @Test(timeout = 10000)
  public void waitTimeoutIsValidatedAndTransmitted() throws Exception {
    Scenario scenario = new Scenario(new String[] {"COMMITTED"});
    try (MockWebServer server = new MockWebServer()) {
      TableBatchWriteSession session = session(server, scenario, 25, 2);
      int requests = server.getRequestCount();
      for (int invalid : new int[] {0, 601}) {
        try {
          session.commit(EMPTY, invalid);
          fail("invalid wait timeout must fail before sending a request: " + invalid);
        } catch (IllegalArgumentException expected) {
          assertEquals(requests, server.getRequestCount());
        }
      }
      session.commit(EMPTY, 600);
      assertEquals(1, scenario.commitCount.get());
      assertWaitTimeout(scenario.commitBodies.get(0), 600);
    }
  }

  @Test(timeout = 10000)
  public void normalAfterFailureRetriesOnceAndPreservesWaitTimeout() throws Exception {
    Scenario scenario = new Scenario(new String[] {"500", "COMMITTED"}, "NORMAL");
    try (MockWebServer server = new MockWebServer()) {
      TableBatchWriteSession session = session(server, scenario, 25, 2);
      session.commit(EMPTY, 600);
      assertEquals(SessionStatus.COMMITTED, session.getStatus());
      assertEquals(2, scenario.commitCount.get());
      assertEquals(1, scenario.getCount.get());
      for (String body : scenario.commitBodies) {
        assertWaitTimeout(body, 600);
      }
    }
  }

  @Test(timeout = 10000)
  public void repeatedFailureDoesNotRetryCommitIndefinitely() throws Exception {
    Scenario scenario = new Scenario(new String[] {"500"}, "NORMAL");
    try (MockWebServer server = new MockWebServer()) {
      TableBatchWriteSession session = session(server, scenario, 25, 2);
      try {
        session.commit(EMPTY, 600);
        fail("two failed commit attempts cannot report success");
      } catch (IOException expected) {
        assertEquals(2, scenario.commitCount.get());
      }
      for (String body : scenario.commitBodies) {
        assertWaitTimeout(body, 600);
      }
    }
  }

  @Test(timeout = 10000)
  public void expiredSessionFailsWithoutAnotherCommit() throws Exception {
    Scenario scenario = new Scenario(new String[] {"500"}, "EXPIRED");
    try (MockWebServer server = new MockWebServer()) {
      TableBatchWriteSession session = session(server, scenario, 25, 2);
      try {
        session.commit(EMPTY);
        fail("expired session cannot commit");
      } catch (IOException expected) {
        assertTrue(expected.getMessage().contains("EXPIRED"));
      }
      assertEquals(SessionStatus.EXPIRED, session.getStatus());
      assertEquals(1, scenario.commitCount.get());
    }
  }

  @Test(timeout = 10000)
  public void invalidLocalResultsStillAllowCleanup() throws Exception {
    Scenario scenario = new Scenario(new String[] {"COMMITTED"});
    try (MockWebServer server = new MockWebServer()) {
      TableBatchWriteSession session = session(server, scenario, 25, 2);
      WriterCommitMessage message =
          new com.aliyun.odps.table.write.impl.v2.WriterCommitMessageV2Impl(
              new com.google.gson.Gson()
                  .fromJson(
                      "{\"sessionId\":\"s\",\"blockNumber\":0,\"attemptNumber\":0,\"recordCount\":1,\"commitMessage\":\"block\"}",
                      com.aliyun.odps.storage.write.BlockWriteResult.class));
      try {
        session.commit(new WriterCommitMessage[] {message, message});
        fail("duplicate block must fail locally");
      } catch (IllegalArgumentException expected) {
        assertEquals(0, scenario.commitCount.get());
        assertEquals(0, scenario.getCount.get());
      }
      session.cleanup();
      session.cleanup();
      assertEquals(1, scenario.abortCount.get());
    }
  }

  @Test(timeout = 10000)
  public void failedStatusQueryIsUnknownAndDoesNotAbort() throws Exception {
    Scenario scenario = new Scenario(new String[] {"500"}, "500");
    try (MockWebServer server = new MockWebServer()) {
      TableBatchWriteSession session = session(server, scenario, 25, 1);
      try {
        session.commit(EMPTY);
        fail("cannot determine outcome when status reload fails");
      } catch (IOException expected) {
        assertTrue(expected.getMessage().contains("unknown"));
      }
      session.cleanup();
      assertEquals(0, scenario.abortCount.get());
      assertEquals(1, scenario.commitCount.get());
    }
  }

  @Test(timeout = 10000)
  public void retryAfterUnknownRecoversSameSessionWithoutResendingCommit() throws Exception {
    Scenario scenario = new Scenario(new String[] {"500"}, "500", "COMMITTED");
    try (MockWebServer server = new MockWebServer()) {
      TableBatchWriteSession session = session(server, scenario, 25, 1);
      try {
        session.commit(EMPTY);
        fail("first status query fails");
      } catch (IOException expected) {
        assertTrue(expected.getMessage().contains("unknown"));
      }
      session.cleanup();
      session.commit(EMPTY);
      assertEquals(SessionStatus.COMMITTED, session.getStatus());
      assertEquals(1, scenario.commitCount.get());
      assertEquals(0, scenario.abortCount.get());
    }
  }

  private static void assertWaitTimeout(String body, int timeout) {
    assertEquals(
        timeout,
        JsonParser.parseString(body)
            .getAsJsonObject()
            .get("WaitFlyingWritersTimeoutSeconds")
            .getAsInt());
  }
}
