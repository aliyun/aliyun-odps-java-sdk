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

package com.aliyun.odps.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.ProxyConfig;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitAssigner;

/**
 * The table-api (OkHttp) egress against a real MaxCompute service, with and without an
 * authenticating proxy.
 *
 * <p>Why this class exists: the credential plumbing in {@code OkHttpConnection} answers a
 * {@code 407} from whatever is on the {@link com.aliyun.odps.rest.RestClient} it is handed, but
 * until this change the clients built by {@code ExecutionEnvironment} never had a proxy set at all
 * ({@code EnvironmentSettings.withHttpProxy/withHttpsProxy} were stored and never read). So the
 * case below is not a restatement of the storage-api one: it drives the read session through this
 * module's own client and asserts that data actually came back.
 *
 * <p><b>Opt-in.</b> The whole class is skipped unless {@code -Dodps.e2e=true} is given, so an
 * ordinary {@code mvn test} without a cloud identity neither runs nor fails it.
 *
 * <pre>
 *   # direct
 *   mvn -pl odps-sdk/odps-sdk-table-api -am test -Dtest=ProxyEgressE2ETest -Dodps.e2e=true
 *   # through a proxy that requires Proxy-Authorization (credential via the environment, so it
 *   # never appears in the process arguments)
 *   ... -Dodps.e2e=true -Dodps.e2e.proxy.address=http://127.0.0.1:18911
 * </pre>
 *
 * <p>OkHttp answers a challenge (407 then retry with the header), so seeing
 * {@code auth=none} once next to {@code auth=ok} on the same URI is the expected shape here. That
 * is the opposite of the netty Upsert shape, which attaches the credential to the CONNECT and must
 * never show an unauthenticated attempt; each egress is judged by its own shape and neither cell
 * borrows the other's conclusion.
 */
public class ProxyEgressE2ETest {

  private static final String E2E_ENABLED = "odps.e2e";
  private static final String PROXY_ADDRESS = "odps.e2e.proxy.address";
  private static final String PROXY_REFUSAL_ADDRESS = "odps.e2e.proxy.refusal.address";
  /** Seeded by this class; two rows, read back through the session under test. */
  private static final String READ_TABLE = "proxy_e2e_read_src";

  private static String endpoint;
  private static String project;
  private static String accessId;
  private static String accessKey;
  private static String proxyAddress;
  private static String refusalAddress;
  private static String proxyUser;
  private static String proxyPassword;
  private static boolean proxyMode;

  @BeforeClass
  public static void setUp() {
    Assume.assumeTrue("opt in with -D" + E2E_ENABLED + "=true", Boolean.getBoolean(E2E_ENABLED));
    accessId = System.getenv("ODPS_ACCESS_ID");
    accessKey = System.getenv("ODPS_ACCESS_KEY");
    endpoint = System.getenv("ODPS_ENDPOINT");
    project = System.getenv("ODPS_PROJECT");
    Assume.assumeTrue("ODPS_ACCESS_ID/ODPS_ACCESS_KEY/ODPS_ENDPOINT/ODPS_PROJECT must be injected",
        accessId != null && accessKey != null && endpoint != null && project != null);
    proxyAddress = System.getProperty(PROXY_ADDRESS);
    refusalAddress = System.getProperty(PROXY_REFUSAL_ADDRESS, proxyAddress);
    proxyUser = System.getenv("ODPS_E2E_PROXY_USER");
    proxyPassword = System.getenv("ODPS_E2E_PROXY_PASS");
    proxyMode = proxyAddress != null && !proxyAddress.isEmpty();
    Authenticator.setDefault(null);
    System.out.println("E2E-CELL cell=setup mode=" + (proxyMode ? "proxy" : "direct")
        + " proxy=" + (proxyMode ? hostAndPort(proxyAddress) : "none"));
  }

  @After
  public void clearGlobalAuthenticator() {
    Authenticator.setDefault(null);
  }

  /**
   * Real read session against a real table: the session must be created by the service and the two
   * seeded rows must come back through the OkHttp egress. In proxy mode the very same call must
   * succeed through the authenticating proxy, which is the point of the change.
   */
  @Test
  public void realReadSessionReturnsSeededRows() throws Exception {
    seedTableThroughControlPlane();

    EnvironmentSettings settings = settingsBuilder(proxyAddress, false).build();
    reportWiring(settings, proxyMode);
    TableBatchReadSession session = new TableReadSessionBuilder()
        .identifier(TableIdentifier.of(project, "default", READ_TABLE))
        .withSettings(settings)
        .buildBatchReadSession();
    assertNotNull("the service must hand back a session", session);
    InputSplitAssigner assigner = session.getInputSplitAssigner();
    int splits = assigner.getSplitsCount();
    assertTrue("a read session over a seeded table must expose at least one split", splits >= 1);

    long rows = 0;
    long idSum = 0;
    for (int i = 0; i < splits; i++) {
      InputSplit split = assigner.getSplit(i);
      try (SplitReader<ArrayRecord> reader = session.createRecordReader(split,
          ReaderOptions.newBuilder().withMaxBatchRowCount(4096).withSettings(settings).build())) {
        while (reader.hasNext()) {
          Record record = reader.get();
          rows++;
          idSum += (Long) record.get(0);
        }
      }
    }
    assertEquals("both seeded rows must be read", 2L, rows);
    assertEquals("the values must survive the trip", 3L, idSum);
    System.out.println("E2E-CELL cell=tableapi-read-session mode=" + mode()
        + " outcome=OK splits=" + splits + " rows=" + rows);
  }

  /**
   * Counter-case: an address without a credential must not get an answer out of an authenticating
   * proxy. The proxy refuses it, so the service never sees the request.
   */
  @Test
  public void credentiallessProxyIsRefusedBeforeTheService() {
    Assume.assumeTrue("only meaningful with a proxy", proxyMode);
    EnvironmentSettings settings = settingsBuilder(refusalAddress, true).build();
    try {
      new TableReadSessionBuilder()
          .identifier(TableIdentifier.of(project, "default", READ_TABLE))
          .withSettings(settings)
          .buildBatchReadSession();
      fail("expected the authenticating proxy to refuse a credentialless client");
    } catch (Exception e) {
      String text = describe(e);
      assertTrue("the failure must point at proxy authentication, was: " + trim(text),
          text.contains("407") || text.toLowerCase().contains("proxy"));
      System.out.println("E2E-CELL cell=tableapi-no-credential mode=proxy outcome=refused "
          + trim(text));
    }
  }

  /**
   * Prints what the settings actually did to the client this module builds, before the session
   * call is attempted. The read session is served by a client of {@code ExecutionEnvironment}'s
   * own, so a proxy that never reached that client would make the egress cells meaningless: this
   * line is what distinguishes "the proxy refused us" from "the proxy was never configured".
   */
  private static void reportWiring(EnvironmentSettings settings, boolean expectProxy) {
    RestClient client = ExecutionEnvironment.create(settings).createHttpClient(project);
    boolean hasProxy = client.getProxy() != null;
    boolean hasCredential = client.getProxyCredential() != null;
    assertEquals("the configured proxy must reach the client this module builds", expectProxy, hasProxy);
    assertEquals("a credential must reach it exactly when the endpoint carried one",
        expectProxy && !withoutCredential(settings), hasCredential);
    System.out.println("E2E-CELL cell=tableapi-wiring mode=" + mode()
        + " proxy=" + hasProxy + " credential=" + hasCredential);
  }

  private static boolean withoutCredential(EnvironmentSettings settings) {
    // The refusal cell builds its settings with an address that carries no userinfo; the success
    // cell always carries one. Nothing is compared against the secret itself.
    String proxy = settings.getHttpProxy().orElse("");
    String authority = proxy.substring(proxy.indexOf("//") + 2);
    return !authority.contains("@");
  }

  private static EnvironmentSettings.Builder settingsBuilder(String address, boolean withoutCredential) {
    EnvironmentSettings.Builder builder = EnvironmentSettings.newBuilder()
        .withCredentials(new Credentials.Builder()
            .withAccount(new AliyunAccount(accessId, accessKey))
            .build())
        .withDefaultProject(project)
        .withServiceEndpoint(endpoint)
        // A write timeout is what makes this module build an OkHttp backed connection at all;
        // without it the same request would ride the JDK transport and test a different egress.
        .withRestOptions(RestOptions.newBuilder().withWriteTimeout(60).withRetryTimes(0).build());
    if (address != null && !address.isEmpty()) {
      String withCredential = withoutCredential ? address : withUserinfo(address);
      builder.withHttpProxy(withCredential).withHttpsProxy(withCredential);
    }
    return builder;
  }

  /**
   * Puts the credential into the proxy URI as percent-encoded userinfo. Encoding is deliberate:
   * the user and password used by the driver contain {@code /}, {@code @}, {@code :} and a bare
   * {@code %}, i.e. exactly the characters that break a naive userinfo parse.
   */
  private static String withUserinfo(String address) {
    assertNotNull("proxy credential must be injected through the environment", proxyUser);
    assertNotNull("proxy credential must be injected through the environment", proxyPassword);
    String scheme = address.substring(0, address.indexOf("//") + 2);
    String rest = address.substring(scheme.length());
    return scheme + escape(proxyUser) + ":" + escape(proxyPassword) + "@" + rest;
  }

  private static String escape(String raw) {
    StringBuilder sb = new StringBuilder();
    for (byte b : raw.getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
      char c = (char) (b & 0xFF);
      if (c == '%' || c == '@' || c == ':' || c == '/' || c == '?' || c == '#' || c == ' ') {
        sb.append(String.format("%%%02X", b));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /**
   * Creates and fills the table through the control plane, so the read session has deterministic
   * content to serve. {@code insert overwrite} keeps reruns idempotent.
   *
   * <p>The control plane here is the JDK transport, which answers a proxy challenge only through a
   * host {@link Authenticator}; that contract has its own cell in the core module's
   * {@code ProxyEgressE2ETest} and is not what this class is about, so it is installed here merely
   * to let the fixture be built.
   */
  private static void seedTableThroughControlPlane() throws Exception {
    Odps odps = new Odps(new AliyunAccount(accessId, accessKey));
    odps.setEndpoint(endpoint);
    odps.setDefaultProject(project);
    odps.getRestClient().setRetryTimes(0);
    if (proxyMode) {
      ProxyConfig.Builder config = ProxyConfig.builder();
      if (proxyUser != null) {
        config.withHttpProxy(proxyAddress, proxyUser, proxyPassword)
            .withHttpsProxy(proxyAddress, proxyUser, proxyPassword);
      } else {
        config.withHttpProxy(proxyAddress).withHttpsProxy(proxyAddress);
      }
      ProxyConfig built = config.build();
      odps.options().setProxyConfig(built);
      built.getProxy(ProxyConfig.Type.HTTP).ifPresent(p -> odps.getRestClient().setProxy(p));
      built.getProxyCredential(ProxyConfig.Type.HTTP)
          .ifPresent(odps.getRestClient()::setProxyCredential);
      Authenticator.setDefault(new ScopedAuthenticator(proxyAddress));
    }
    // Warm the JDK's per-address proxy credential cache with a GET before any POST.
    //
    // Why this is here at all: in the run that first produced this case, the proxy answered the
    // fixture's first request (a POST that creates the table) with a 407 and nothing authenticated
    // followed it (proxy-proxy.log line "PROXY-REJECT kind=post ... auth=none", no auth=ok POST
    // after it), while the same JVM's GETs did get answered after their challenge. Whatever the
    // reason on the JDK side, this class is not the place to litigate it: the egress under test is
    // the OkHttp read session, so the fixture takes the shape that works. The missing status /
    // request id in that failure message is the separate defect tracked as "REST 传输层在非 2xx 且
    // 无错误流时丢弃 status" (see the Clients backlog), not something this cell asserts.
    odps.projects().get(project).reload();
    Map<String, String> hints = new HashMap<>();
    hints.put("odps.sql.type.system.odps2", "true");
    runSql(odps, "create table if not exists " + READ_TABLE + " (id bigint, name string);");
    runSql(odps, "insert overwrite table " + READ_TABLE + " values (1L, 'a'), (2L, 'b');");
    Authenticator.setDefault(null);
  }

  private static void runSql(Odps odps, String sql) throws Exception {
    Instance instance = SQLTask.run(odps, project, sql, null, null);
    instance.waitForSuccess();
    List<Record> ignored = SQLTask.getResult(instance);
  }

  private static String mode() {
    return proxyMode ? "proxy" : "direct";
  }

  private static String hostAndPort(String url) {
    String s = url.substring(url.indexOf("//") + 2);
    int slash = s.indexOf('/');
    return slash >= 0 ? s.substring(0, slash) : s;
  }

  private static String describe(Throwable e) {
    StringBuilder sb = new StringBuilder();
    for (Throwable t = e; t != null && sb.length() < 2000; t = t.getCause()) {
      sb.append(t.getClass().getName()).append(": ").append(t.getMessage()).append(' ');
    }
    return sb.toString();
  }

  private static String trim(String s) {
    String one = s.replace('\n', ' ');
    return one.length() > 180 ? one.substring(0, 180) : one;
  }

  /** Answers the challenge of the configured proxy only; see the core module for the full contract. */
  private static final class ScopedAuthenticator extends Authenticator {
    private final String host;
    private final int port;

    ScopedAuthenticator(String address) {
      String authority = address.substring(address.indexOf("//") + 2);
      int slash = authority.indexOf('/');
      if (slash >= 0) {
        authority = authority.substring(0, slash);
      }
      int colon = authority.lastIndexOf(':');
      if (colon >= 0) {
        host = authority.substring(0, colon);
        port = Integer.parseInt(authority.substring(colon + 1));
      } else {
        host = authority;
        port = 80;
      }
    }

    @Override
    protected PasswordAuthentication getPasswordAuthentication() {
      if (!host.equals(getRequestingHost()) || port != getRequestingPort()) {
        return null;
      }
      return new PasswordAuthentication(proxyUser, proxyPassword.toCharArray());
    }
  }
}
