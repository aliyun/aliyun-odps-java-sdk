package com.aliyun.odps.tunnel.impl;

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;
import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_ROUTED_SERVER;
import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_TUNNEL_METRICS;
import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_TUNNEL_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.transport.Transport;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.retry.RetryContext;
import com.aliyun.odps.retry.RetryHeaders;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;

public class StreamUploadSessionImplTest {

  @Test
  public void shouldReloadSlotsAndUseNewRoutedServerAfter502() throws Exception {
    TestRestClient restClient = new TestRestClient();
    TestStreamUploadSession.prepare(
        new QueuedHttpResult("create-session", 200,
            "{\"session_name\":\"upload-id\",\"schema\":{},\"slots\":[[\"0\",\"127.0.0.1:9000\"]],\"status\":\"normal\"}"),
        new QueuedHttpResult("reload-session", 200,
            "{\"slots\":[[\"0\",\"127.0.0.1:9001\"]],\"status\":\"normal\"}"));

    TestStreamUploadSession session = new TestStreamUploadSession(newConfiguration(restClient));
    session.httpClient = restClient;

    ProtobufRecordPack pack = new ProtobufRecordPack(new TableSchema());

    StreamUploadSessionImpl.WriteResult result = session.writeBlock(pack, 0L, null);

    assertEquals("req-200", result.requestId);
    assertEquals(1, session.getReloadCount());
    assertEquals("upload-id", session.getReloadParams().get(TunnelConstants.UPLOADID));
    assertEquals("schema-version", session.getReloadParams().get(TunnelConstants.SCHEMA_VERSION));
    assertTrue(!session.getReloadParams().containsKey(TunnelConstants.PARAM_READ_ONLY));
    assertEquals(2, restClient.requestHeaders.size());
    assertEquals("127.0.0.1:9000", restClient.requestHeaders.get(0).get(HEADER_ODPS_ROUTED_SERVER));
    assertEquals("127.0.0.1:9001", restClient.requestHeaders.get(1).get(HEADER_ODPS_ROUTED_SERVER));
    assertTrue(restClient.requestHeaders.get(0).containsKey(HEADER_ODPS_TUNNEL_VERSION));
    String retryTraceId = restClient.requestHeaders.get(0).get(RetryHeaders.TRACE_ID);
    assertNotNull(retryTraceId);
    assertEquals(retryTraceId, restClient.requestHeaders.get(1).get(RetryHeaders.TRACE_ID));
    assertEquals("0", restClient.requestHeaders.get(0).get(RetryHeaders.RETRY_INDEX));
    assertEquals("1", restClient.requestHeaders.get(1).get(RetryHeaders.RETRY_INDEX));
  }

  private static Configuration newConfiguration(RestClient restClient) {
    return new TestConfiguration(restClient);
  }

  private static class TestOdps extends Odps {
    private final RestClient restClient;

    TestOdps(RestClient restClient) {
      super(new AliyunAccount("ak", "sk"));
      this.restClient = restClient;
      setTunnelEndpoint("http://127.0.0.1");
    }

    @Override
    public Odps clone() {
      return this;
    }

    @Override
    public RestClient getRestClient() {
      return restClient;
    }
  }

  private static class TestConfiguration extends Configuration {
    TestConfiguration(RestClient restClient) {
      super(new TestOdps(restClient));
    }

    @Override
    public TunnelRetryHandler.RetryPolicy getRetryPolicy() {
      return new TunnelRetryHandler.RetryPolicy() {
        @Override
        public boolean shouldRetry(Exception e, int attempt) {
          return attempt <= 7;
        }

        @Override
        public long getRetryWaitTime(int attempt) {
          return 0;
        }
      };
    }
  }

  private static class TestRestClient extends RestClient {
    private int attempt;
    private final List<Map<String, String>> requestHeaders = new ArrayList<Map<String, String>>();

    TestRestClient() {
      super((Transport) null);
    }

    @Override
    public Connection connect(String resource, String method, Map<String, String> params,
                              Map<String, String> headers) {
      requestHeaders.add(new HashMap<String, String>(headers));
      attempt++;
      if (attempt == 1) {
        return new TestConnection(new TestResponse(502, "req-502", null, null),
            new ByteArrayInputStream(
                "{\"Code\":\"BadGateway\",\"Message\":\"bad gateway\"}".getBytes()));
      }
      return new TestConnection(new TestResponse(200, "req-200", "127.0.0.1:9001", "1"),
          new ByteArrayInputStream(new byte[0]));
    }
  }

  private static class TestConnection implements Connection {
    private final Response response;
    private final InputStream inputStream;
    private final OutputStream outputStream = new ByteArrayOutputStream();

    TestConnection(Response response, InputStream inputStream) {
      this.response = response;
      this.inputStream = inputStream;
    }

    @Override
    public void connect(Request req) {
    }

    @Override
    public OutputStream getOutputStream() {
      return outputStream;
    }

    @Override
    public Response getResponse() {
      return response;
    }

    @Override
    public InputStream getInputStream() {
      return inputStream;
    }

    @Override
    public void disconnect() {
    }
  }

  private static class TestResponse extends Response {
    TestResponse(int status, String requestId, String routedServer, String slotNum) {
      this.status = status;
      headers.put(HEADER_ODPS_REQUEST_ID, requestId);
      if (routedServer != null) {
        headers.put(HEADER_ODPS_ROUTED_SERVER, routedServer);
      }
      if (slotNum != null) {
        headers.put(HttpHeaders.HEADER_ODPS_SLOT_NUM, slotNum);
      }
      headers.put(HEADER_ODPS_TUNNEL_METRICS, "{}");
    }
  }

  private static class TestStreamUploadSession extends StreamUploadSessionImpl {
    private static final ThreadLocal<Deque<QueuedHttpResult>> RESULTS =
        new ThreadLocal<Deque<QueuedHttpResult>>();

    private int reloadCount;
    private Map<String, String> reloadParams;

    static void prepare(QueuedHttpResult... results) {
      Deque<QueuedHttpResult> queued = new ArrayDeque<QueuedHttpResult>();
      for (QueuedHttpResult result : results) {
        queued.addLast(result);
      }
      RESULTS.set(queued);
    }

    TestStreamUploadSession(Configuration conf) throws TunnelException {
      super(conf, "project", "schema", "table", null, false, 1L, null, "schema-version", true,
          false);
    }

    @Override
    protected HttpResult httpRequest(HashMap<String, String> headers, Map<String, String> params,
                                     String method, String action) throws TunnelException {
      return nextHttpResult(params, method);
    }

    @Override
    protected HttpResult httpRequest(HashMap<String, String> headers, Map<String, String> params,
                                     String method, String action, RetryContext initialContext)
        throws TunnelException {
      return nextHttpResult(params, method);
    }

    private HttpResult nextHttpResult(Map<String, String> params, String method) {
      if ("GET".equals(method)) {
        reloadCount++;
        reloadParams = new HashMap<String, String>(params);
      }
      Deque<QueuedHttpResult> queued = RESULTS.get();
      QueuedHttpResult result = queued.removeFirst();
      return new HttpResult(result.requestId, result.status, result.body);
    }

    int getReloadCount() {
      return reloadCount;
    }

    Map<String, String> getReloadParams() {
      return reloadParams;
    }
  }

  private static class QueuedHttpResult {
    private final String requestId;
    private final int status;
    private final String body;

    QueuedHttpResult(String requestId, int status, String body) {
      this.requestId = requestId;
      this.status = status;
      this.body = body;
    }
  }
}
