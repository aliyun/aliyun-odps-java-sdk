package com.aliyun.odps.tunnel.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.retry.RetryHeaders;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.hasher.TypeHasher;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.utils.FixedNettyChannelPool;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

public class UpsertStreamRetryTest {

  @Test(timeout = 15000)
  public void testMixedErrorsAndSuccessiveFlushesHaveIndependentBudgets() throws Exception {
    assertMixedErrorsAndSuccessiveFlushes(null);
  }

  @Test(timeout = 15000)
  public void testAsyncFlushUsesOperationLocalBudgets() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      assertMixedErrorsAndSuccessiveFlushes(executor);
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private void assertMixedErrorsAndSuccessiveFlushes(ExecutorService executor) throws Exception {
    RecordingPolicy policy = new RecordingPolicy(3);
    List<Integer> errors = new ArrayList<>(Collections.nCopies(100, 429));
    errors.addAll(Arrays.asList(-1, -1, -1, 200, -1, 200));
    try (TestSession session = new TestSession(policy, Collections.singletonMap(0, errors));
         UpsertStream stream = session.buildUpsertStream().setAsyncFlushService(executor).build()) {
      writeBucket(session, stream, 0);
      stream.flush();
      assertEquals(Arrays.asList(1, 2, 3), policy.attempts);
      assertEquals(104, session.pool.headers.get(0).size());
      assertRequestSequence(session.pool.headers.get(0), 0, 104);

      writeBucket(session, stream, 0);
      stream.flush();
      assertEquals(Arrays.asList(1, 2, 3, 1), policy.attempts);
      assertEquals(106, session.pool.headers.get(0).size());
      assertRequestSequence(session.pool.headers.get(0), 104, 2);
      assertFalse(session.pool.headers.get(0).get(0).get(RetryHeaders.TRACE_ID).equals(
          session.pool.headers.get(0).get(104).get(RetryHeaders.TRACE_ID)));
    }
  }

  @Test(timeout = 15000)
  public void testAlternatingErrorsExhaustDefaultBudget() throws Exception {
    RecordingPolicy policy = new RecordingPolicy(3);
    try (TestSession session = new TestSession(policy, Collections.singletonMap(0,
        Arrays.asList(429, -1, 429, -1, 429, -1, 429, -1)));
         UpsertStream stream = session.buildUpsertStream().build()) {
      writeBucket(session, stream, 0);
      try {
        stream.flush();
        fail("The fourth local error must exhaust the default retry budget");
      } catch (TunnelException expected) {
        assertEquals(TunnelConstants.LOCAL_ERROR_CODE, expected.getErrorCode());
        assertEquals(8, session.pool.headers.get(0).size());
        assertEquals(Arrays.asList(1, 2, 3, 4), policy.attempts);
      }
    }
  }

  @Test(timeout = 15000)
  public void testBucketsInSameFlushHaveIndependentBudgets() throws Exception {
    RecordingPolicy policy = new RecordingPolicy(1);
    List<Integer> errors = new ArrayList<>(Collections.nCopies(100, 429));
    errors.addAll(Arrays.asList(-1, 200));
    Map<Integer, List<Integer>> script = new HashMap<>();
    script.put(0, errors);
    script.put(1, errors);
    try (TestSession session = new TestSession(policy, script);
         UpsertStream stream = session.buildUpsertStream().build()) {
      writeBucket(session, stream, 0);
      writeBucket(session, stream, 1);
      stream.flush();
      assertEquals(Arrays.asList(1, 1), policy.attempts);
      assertEquals(102, session.pool.headers.get(0).size());
      assertEquals(102, session.pool.headers.get(1).size());
      assertRequestSequence(session.pool.headers.get(0), 0, 102);
      assertRequestSequence(session.pool.headers.get(1), 0, 102);
      assertFalse(session.pool.headers.get(0).get(0).get(RetryHeaders.TRACE_ID).equals(
          session.pool.headers.get(1).get(0).get(RetryHeaders.TRACE_ID)));
    }
  }

  @Test(timeout = 15000)
  public void testZeroDefaultRetriesStopsAtFirstLocalError() throws Exception {
    RecordingPolicy policy = new RecordingPolicy(0);
    try (TestSession session = new TestSession(policy,
        Collections.singletonMap(0, Arrays.asList(-1, 200)));
         UpsertStream stream = session.buildUpsertStream().build()) {
      writeBucket(session, stream, 0);
      try {
        stream.flush();
        fail("A zero retry limit must reject the first local error");
      } catch (TunnelException expected) {
        assertEquals(Arrays.asList(1), policy.attempts);
        assertEquals(1, session.pool.headers.get(0).size());
      }
    }
  }

  @Test(timeout = 15000)
  public void testLocalErrorsAllowExactlyThreeRetries() throws Exception {
    RecordingPolicy policy = new RecordingPolicy(3);
    try (TestSession session = new TestSession(policy,
        Collections.singletonMap(0, Arrays.asList(-1, -1, -1, -1, 200)));
         UpsertStream stream = session.buildUpsertStream().build()) {
      writeBucket(session, stream, 0);
      try {
        stream.flush();
        fail("The fourth failed request must stop after three retries");
      } catch (TunnelException expected) {
        assertEquals(Arrays.asList(1, 2, 3, 4), policy.attempts);
        assertEquals(4, session.pool.headers.get(0).size());
      }
    }
  }

  @Test(timeout = 15000)
  public void testLegacyExceptionListenerReceivesOriginalRetryCount() throws Exception {
    List<Integer> retries = new ArrayList<>();
    assertLegacyListener(new UpsertStream.Listener() {
      @Override
      public void onFlush(UpsertStream.FlushResult result) {
      }

      @Override
      public boolean onFlushFail(Exception error, int retry) {
        retries.add(retry);
        return retry < 2;
      }
    });
    assertEquals(Arrays.asList(0, 1), retries);
  }

  @Test(timeout = 15000)
  public void testLegacyStringListenerReceivesOriginalRetryCount() throws Exception {
    List<Integer> retries = new ArrayList<>();
    assertLegacyListener(new UpsertStream.Listener() {
      @Override
      public void onFlush(UpsertStream.FlushResult result) {
      }

      @Override
      public boolean onFlushFail(String error, int retry) {
        assertNotNull(error);
        retries.add(retry);
        return retry < 2;
      }
    });
    assertEquals(Arrays.asList(0, 1), retries);
  }

  private void assertLegacyListener(UpsertStream.Listener listener) throws Exception {
    RecordingPolicy policy = new RecordingPolicy(0);
    try (TestSession session = new TestSession(policy,
        Collections.singletonMap(0, Arrays.asList(429, -1, 200)));
         UpsertStream stream = session.buildUpsertStream().setListener(listener).build()) {
      writeBucket(session, stream, 0);
      stream.flush();
      assertTrue(policy.attempts.isEmpty());
      assertEquals(3, session.pool.headers.get(0).size());
    }
  }

  @Test(timeout = 15000)
  public void testLegacyDefaultListenerSubclassKeepsItsOverride() throws Exception {
    RecordingPolicy policy = new RecordingPolicy(0);
    List<Integer> retries = new ArrayList<>();
    try (TestSession session = new TestSession(policy,
        Collections.singletonMap(0, Arrays.asList(429, -1, 200)));
         UpsertStream stream = session.buildUpsertStream().setListener(
             new UpsertSessionImpl.DefaultUpsertSteamListener(session) {
               @Override
               public boolean onFlushFail(Exception error, int retry) {
                 retries.add(retry);
                 return retry < 2;
               }
             }).build()) {
      writeBucket(session, stream, 0);
      stream.flush();
      assertEquals(Arrays.asList(0, 1), retries);
      assertTrue(policy.attempts.isEmpty());
    }
  }

  @Test(timeout = 15000)
  public void testLegacyHandlerOverrideCanStopDefaultUpsertRetries() throws Exception {
    RecordingPolicy policy = new RecordingPolicy(3);
    List<Integer> retries = new ArrayList<>();
    try (TestSession session = new TestSession(policy,
        Collections.singletonMap(0, Arrays.asList(-1, 200)));
         UpsertStream stream = session.buildUpsertStream().build()) {
      session.tunnelRetryHandler = new TunnelRetryHandler(policy, null) {
        @Override
        public boolean onFailure(Exception error, int retry) {
          retries.add(retry);
          return false;
        }
      };
      writeBucket(session, stream, 0);
      try {
        stream.flush();
        fail("The legacy handler must be able to stop the default Upsert listener");
      } catch (TunnelException expected) {
        assertEquals(TunnelConstants.LOCAL_ERROR_CODE, expected.getErrorCode());
      }
      assertEquals(Arrays.asList(0), retries);
      assertTrue(policy.attempts.isEmpty());
      assertEquals(1, session.pool.headers.get(0).size());
    }
  }

  @Test(timeout = 15000)
  public void testInheritedLegacyHandlerOverrideCanCallSuper() throws Exception {
    RecordingPolicy policy = new RecordingPolicy(1);
    List<Integer> retries = new ArrayList<>();
    class LegacyHandler extends TunnelRetryHandler {
      LegacyHandler() {
        super(policy, null);
      }

      @Override
      public boolean onFailure(Exception error, int retry) {
        retries.add(retry);
        return super.onFailure(error, retry);
      }
    }
    try (TestSession session = new TestSession(policy,
        Collections.singletonMap(0, Arrays.asList(-1, -1, -1, 200)));
         UpsertStream stream = session.buildUpsertStream().build()) {
      session.tunnelRetryHandler = new LegacyHandler() { };
      writeBucket(session, stream, 0);
      try {
        stream.flush();
        fail("The inherited override must retain its zero-based retry budget");
      } catch (TunnelException expected) {
        assertEquals(TunnelConstants.LOCAL_ERROR_CODE, expected.getErrorCode());
      }
      assertEquals(Arrays.asList(0, 1, 2), retries);
      assertEquals(Arrays.asList(0, 1, 2), policy.attempts);
      assertEquals(3, session.pool.headers.get(0).size());
    }
  }

  @Test(timeout = 15000)
  public void testSlotReassignmentAndServerReloadStillWork() throws Exception {
    RecordingPolicy policy = new RecordingPolicy(0);
    try (TestSession session = new TestSession(policy,
        Collections.singletonMap(0, Arrays.asList(308, 502, 504, 200)));
         UpsertStream stream = session.buildUpsertStream().build()) {
      writeBucket(session, stream, 0);
      stream.flush();
      assertEquals(2, session.reloads);
      assertEquals("127.0.0.1:9001", session.pool.headers.get(0).get(1).get(
          HttpHeaders.HEADER_ODPS_ROUTED_SERVER));
      assertTrue(policy.attempts.isEmpty());
      assertRequestSequence(session.pool.headers.get(0), 0, 4);
    }
  }

  private static void assertRequestSequence(List<Map<String, String>> headers, int start,
                                            int size) {
    String trace = headers.get(start).get(RetryHeaders.TRACE_ID);
    assertNotNull(trace);
    for (int i = 0; i < size; i++) {
      assertEquals(trace, headers.get(start + i).get(RetryHeaders.TRACE_ID));
      assertEquals(String.valueOf(i), headers.get(start + i).get(RetryHeaders.RETRY_INDEX));
    }
  }

  private static void writeBucket(TestSession session, UpsertStream stream, int bucket)
      throws Exception {
    for (long key = 0; key < 100; key++) {
      int hash = TypeHasher.CombineHashVal(Collections.singletonList(
          TypeHasher.hash(OdpsType.BIGINT, key, "default")));
      if (hash % session.getBuckets().size() == bucket) {
        Record record = session.newRecord();
        record.setBigint(0, key);
        stream.upsert(record);
        return;
      }
    }
    throw new AssertionError("No test key found for bucket " + bucket);
  }

  private static final class RecordingPolicy implements TunnelRetryHandler.RetryPolicy {
    private final int limit;
    private final List<Integer> attempts = new ArrayList<>();

    private RecordingPolicy(int limit) {
      this.limit = limit;
    }

    @Override
    public boolean shouldRetry(Exception e, int attempt) {
      attempts.add(attempt);
      return attempt <= limit;
    }

    @Override
    public long getRetryWaitTime(int attempt) {
      return 0;
    }
  }

  private static final class TestSession extends UpsertSessionImpl {
    private final ScriptedChannelPool pool;
    private int reloads;

    private TestSession(RecordingPolicy policy, Map<Integer, List<Integer>> script)
        throws Exception {
      super(newSessionBuilder());
      pool = new ScriptedChannelPool(script);
      tunnelRetryHandler = new TunnelRetryHandler(policy, null) {
        private final Map<RetryPolicy, RetryPolicy> noWaitPolicies = new IdentityHashMap<>();

        @Override
        protected RetryPolicy getRetryPolicy(Integer status) {
          RetryPolicy delegate = super.getRetryPolicy(status);
          return noWaitPolicies.computeIfAbsent(delegate, ignored -> new RetryPolicy() {
            @Override
            public boolean shouldRetry(Exception e, int attempt) {
              return delegate.shouldRetry(e, attempt);
            }

            @Override
            public long getRetryWaitTime(int attempt) {
              return 0;
            }
          });
        }
      };
    }

    @Override
    protected HttpResult httpRequestWithNoRetry(HashMap<String, String> headers,
                                               Map<String, String> params,
                                               String method, String action) {
      if ("GET".equals(method)) {
        reloads++;
      }
      return new HttpResult("session-request", 200,
          "{\"id\":\"upsert-id\",\"schema\":{\"columns\":[{\"name\":\"key\",\"type\":\"bigint\"}]},"
          + "\"hash_key\":[\"key\"],\"hasher\":\"default\",\"status\":\"normal\","
          + "\"slots\":[{\"slot_id\":\"0\",\"buckets\":[0,1],"
          + "\"worker_addr\":\"127.0.0.1:9000\"}]}");
    }

    @Override
    public FixedNettyChannelPool getChannelPool() {
      return pool;
    }
  }

  private static UpsertSessionImpl.Builder newSessionBuilder() {
    Odps odps = new Odps(new AliyunAccount("test-id", "test-key"));
    odps.setEndpoint("http://127.0.0.1");
    odps.setTunnelEndpoint("http://127.0.0.1");
    return new UpsertSessionImpl.Builder().setProjectName("project").setTableName("table")
        .setConfig(new Configuration(odps)).setNettyBootStrap(new Bootstrap());
  }

  private static final class ScriptedChannelPool extends FixedNettyChannelPool {
    private final Map<Integer, Deque<Integer>> script = new HashMap<>();
    private final Map<Integer, List<Map<String, String>>> headers = new HashMap<>();

    private ScriptedChannelPool(Map<Integer, List<Integer>> script) {
      super(1, () -> null);
      script.forEach((bucket, statuses) -> this.script.put(bucket, new ArrayDeque<>(statuses)));
    }

    @Override
    public Channel acquire() {
      return new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
        @Override
        public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
          try {
            FullHttpRequest request = (FullHttpRequest) message;
            int bucket = Integer.parseInt(new QueryStringDecoder(request.uri()).parameters()
                .get(TunnelConstants.BUCKET_ID).get(0));
            Map<String, String> requestHeaders = new HashMap<>();
            request.headers().forEach(
                header -> requestHeaders.put(header.getKey(), header.getValue()));
            headers.computeIfAbsent(bucket, ignored -> new ArrayList<>()).add(requestHeaders);
            Deque<Integer> statuses = script.get(bucket);
            int status = statuses == null || statuses.isEmpty() ? 200 : statuses.removeFirst();
            if (status == -1) {
              promise.setFailure(new IOException("simulated connection failure"));
              return;
            }
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.valueOf(status), Unpooled.copiedBuffer(
                    "{\"Code\":\"TestError\",\"Message\":\"simulated response\"}",
                    CharsetUtil.UTF_8));
            response.headers().set(HttpHeaders.HEADER_ODPS_REQUEST_ID, "flush-request");
            if (status == 308) {
              response.headers().set(HttpHeaders.HEADER_ODPS_ROUTED_SERVER, "127.0.0.1:9001");
            }
            promise.setSuccess();
            ctx.pipeline().fireChannelRead(response);
          } finally {
            ReferenceCountUtil.release(message);
          }
        }
      });
    }
  }
}
