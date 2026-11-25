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

package com.aliyun.odps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.streams.UpsertStream;

/**
 * End-to-end check of the proxy path against a real MaxCompute service, with the proxy switched on
 * and off. Nothing here is mocked: the client really opens the socket, and the assertions are about
 * what the real service answers.
 *
 * <p><b>Opt-in.</b> The whole class is skipped unless {@code -Dodps.e2e=true} is given, so an
 * ordinary {@code mvn test} (no cloud identity, no network) neither runs nor fails it. Two runs are
 * expected to be green:
 *
 * <pre>
 *   # direct: no proxy at all
 *   mvn -pl odps-sdk/odps-sdk-core -am test -Dtest=ProxyEgressE2ETest -Dodps.e2e=true
 *   # through a proxy that requires Proxy-Authorization
 *   ... -Dodps.e2e=true -Dodps.e2e.proxy.address=http://127.0.0.1:18911 \
 *       (proxy user/password come from the ODPS_E2E_PROXY_USER / ODPS_E2E_PROXY_PASS environment,
 *        never from the command line, so they stay out of the process arguments)
 * </pre>
 *
 * <p>Proxy-side observation (did {@code Proxy-Authorization} actually reach the proxy?) is done by
 * the driver that reads the proxy's own log, not from inside the JVM: see
 * {@code work/items/2026-09-29-java-sdk-proxy-auth/evidence/e2e} in the Clients workspace.
 *
 * <p><b>Known shape this documents on purpose:</b> {@link ProxyConfig} carries the credential, and
 * the OkHttp based transports (storage-api, table-api) send it themselves. The default control
 * plane transport here is the JDK one ({@code HttpURLConnection}), which answers a {@code 407} only
 * through a {@link java.net.Authenticator}. {@link #controlPlaneNeedsAnAuthenticatorForProxyAuth()}
 * asserts the failure without it and {@link #authenticatorAnswerIsScopedToTheProxy()} installs a
 * scoped one and asserts success, so both halves of that contract are on the record.
 */
public class ProxyEgressE2ETest {

  private static final String E2E_ENABLED = "odps.e2e";
  private static final String PROXY_ADDRESS = "odps.e2e.proxy.address";
  /**
   * A second proxy address, used only by the cells that must be refused. The JDK caches proxy
   * credentials per address for the life of the JVM ({@code sun.net.www.protocol.http.AuthCache}),
   * so the refusal cells may not share an address with a cell that authenticates successfully,
   * or their outcome would depend on the order the methods happen to run in.
   */
  private static final String PROXY_REFUSAL_ADDRESS = "odps.e2e.proxy.refusal.address";
  private static final String PROXY_USER_ENV = "ODPS_E2E_PROXY_USER";
  private static final String PROXY_PASS_ENV = "ODPS_E2E_PROXY_PASS";
  private static final String PUBLIC_ENDPOINT_ENV = "ODPS_E2E_PUBLIC_ENDPOINT";
  private static final String DEFAULT_PUBLIC_ENDPOINT =
      "https://service.cn-shanghai.maxcompute.aliyun.com/api";

  /** The primary-key table the Upsert cell writes through. Created by the owning work item. */
  private static final String UPSERT_TABLE = "proxy_e2e_upsert_tx";
  /** Fixed keys: an Upsert of the same ids twice leaves the same three rows. */
  private static final long[] UPSERT_IDS = {1L, 2L, 3L};

  /** Server-side request ids look like this; their presence proves the real service answered. */
  private static final Pattern REQUEST_ID = Pattern.compile("[0-9]{8}[0-9a-zA-Z]{10,}");

  private static boolean proxyMode;
  private static String proxyAddress;
  private static String proxyUser;
  private static String proxyPassword;
  private static String refusalAddress;
  private static String project;

  @BeforeClass
  public static void setUp() {
    Assume.assumeTrue(
        "opt in with -D" + E2E_ENABLED + "=true; requires a real identity in test.conf",
        Boolean.getBoolean(E2E_ENABLED));
    proxyAddress = System.getProperty(PROXY_ADDRESS);
    proxyUser = System.getenv(PROXY_USER_ENV);
    proxyPassword = System.getenv(PROXY_PASS_ENV);
    refusalAddress = System.getProperty(PROXY_REFUSAL_ADDRESS, proxyAddress);
    proxyMode = proxyAddress != null && !proxyAddress.isEmpty();
    if (proxyMode) {
      assertTrue("proxy address must be a URL", proxyAddress.startsWith("http://"));
    }
    project = OdpsTestUtils.getProperty("default.project");
    assertNotNull("test.conf must provide default.project for the E2E", project);
    Authenticator.setDefault(null);
    System.out.println("E2E-CELL cell=setup mode=" + (proxyMode ? "proxy" : "direct")
        + " proxy=" + (proxyMode ? hostAndPort(proxyAddress) : "none"));
  }

  @AfterClass
  public static void restoreGlobalState() {
    // Never leave a global Authenticator behind: it would change the behaviour of other tests.
    // JDK 8 cannot read the previous default, so the contract of this class is that it owns
    // the Authenticator only while a cell runs and clears it afterwards.
    Authenticator.setDefault(null);
  }

  /**
   * The business half: a real SQL job submitted to the real service must succeed in both modes.
   * This is what "the proxy does not break the product" means for the control plane.
   */
  @Test
  public void realSqlSelectOneSucceeds() throws Exception {
    Odps odps = newOdpsWithProxy();
    if (proxyMode && proxyUser != null) {
      installScopedProxyAuthenticator();
    }
    try {
      Instance instance = SQLTask.run(odps, project, "select 1;", null, null);
      instance.waitForSuccess();
      List<Record> records = SQLTask.getResult(instance);
      assertEquals("one row expected", 1, records.size());
      assertEquals("1", String.valueOf(records.get(0).get(0)));
      System.out.println("E2E-CELL cell=sql-select-1 mode=" + mode()
          + " outcome=OK instance=" + instance.getId());
    } finally {
      Authenticator.setDefault(null);
    }
  }

  /**
   * Wiring check on the object graph the caller actually builds: without a proxy nothing must be
   * installed; with one, the address and (when supplied) the credential must be reachable by
   * transports that read them.
   */
  @Test
  public void proxyWiringIsVisibleOnRestClient() {
    Odps odps = newOdpsWithProxy();
    if (!proxyMode) {
      assertNull("direct mode must not install a proxy", odps.getRestClient().getProxy());
      assertNull("direct mode must not install a credential",
          odps.getRestClient().getProxyCredential());
      System.out.println("E2E-CELL cell=rest-wiring mode=direct proxy=null credential=null");
      return;
    }
    assertNotNull("proxy must be installed", odps.getRestClient().getProxy());
    if (proxyUser != null) {
      assertNotNull("credential must be handed to the RestClient",
          odps.getRestClient().getProxyCredential());
      assertEquals(proxyUser, odps.getRestClient().getProxyCredential().getUsername());
      // toString must not leak the password.
      assertTrue("toString must mask the credential",
          !String.valueOf(odps.getRestClient().getProxyCredential()).contains(proxyPassword));
    }
    System.out.println("E2E-CELL cell=rest-wiring mode=proxy proxy="
        + odps.getRestClient().getProxy() + " credential="
        + (odps.getRestClient().getProxyCredential() != null));
  }

  /**
   * Documents the JDK transport contract instead of pretending it away: {@code ProxyConfig}
   * credentials alone do not answer a {@code 407} on the JDK control plane. Callers that need the
   * control plane behind an authenticating proxy must provide an {@link Authenticator} (which is
   * exactly what the Studio product does).
   */
  @Test
  public void controlPlaneNeedsAnAuthenticatorForProxyAuth() {
    Assume.assumeTrue("only meaningful with an authenticating proxy",
        proxyMode && proxyUser != null);
    Authenticator.setDefault(null);
    Odps odps = newOdpsWithProxy(refusalAddress);
    try {
      odps.projects().get(project).reload();
      fail("expected the proxy to refuse the request without an Authenticator");
    } catch (OdpsException e) {
      String text = String.valueOf(e.getMessage()) + " " + e.getClass().getName();
      assertTrue("failure must point at proxy authentication, was: " + firstLine(text),
          text.contains("407") || text.toLowerCase().contains("proxy"));
      System.out.println("E2E-CELL cell=jdk-without-authenticator mode=proxy outcome=refused "
          + firstLine(text));
    }
  }

  /**
   * With the scoped Authenticator the same control-plane call must succeed through the proxy, and
   * the authenticator must only ever answer for the configured proxy host.
   */
  @Test
  public void authenticatorAnswerIsScopedToTheProxy() throws Exception {
    Assume.assumeTrue("only meaningful with an authenticating proxy",
        proxyMode && proxyUser != null);
    installScopedProxyAuthenticator();
    try {
      Odps odps = newOdpsWithProxy();
      Project p = odps.projects().get(project);
      p.reload();
      assertNotNull(p.getName());
      ScopingAuthenticator a = new ScopingAuthenticator(hostOf(proxyAddress), portOf(proxyAddress),
          proxyUser, proxyPassword);
      Authenticator.setDefault(a);
      PasswordAuthentication answer = a.answerFor(hostOf(proxyAddress), portOf(proxyAddress));
      assertNotNull("must answer for its own proxy", answer);
      assertEquals(proxyUser, answer.getUserName());
      assertNull("must stay silent for another proxy address",
          a.answerFor("not-the-proxy.invalid", 3128));
      assertNull("must stay silent for the service host itself (a 401, not a 407)",
          a.answerFor("service.cn-shanghai.maxcompute.aliyun.com", 443));
      System.out.println("E2E-CELL cell=jdk-with-authenticator mode=proxy outcome=OK project="
          + p.getName());
    } finally {
      Authenticator.setDefault(null);
    }
  }

  /**
   * Public-internet half: the very same code path must be able to reach a real public MaxCompute
   * endpoint. With the registered identity this is expected to be refused by the service itself
   * (the account does not exist on the public cloud), which still proves the request left the
   * machine and the tunnel carried it: the answer must be the service's own, carrying a server
   * request id. Business success on the public endpoint needs a public-cloud AK and is therefore
   * asserted as "reached the service", not as "200".
   */
  @Test
  public void publicEndpointIsReachedThroughTheSamePath() {
    String endpoint = System.getenv(PUBLIC_ENDPOINT_ENV);
    if (endpoint == null || endpoint.isEmpty()) {
      endpoint = DEFAULT_PUBLIC_ENDPOINT;
    }
    Odps odps = new Odps(new AliyunAccount("e2e-no-such-access-id", "e2e-no-such-access-key"));
    odps.setEndpoint(endpoint);
    odps.setDefaultProject(project);
    odps.getRestClient().setRetryTimes(0);
    applyProxy(odps, proxyAddress);
    if (proxyMode && proxyUser != null) {
      installScopedProxyAuthenticator();
    }
    try {
      odps.projects().get(project).reload();
      // Not a failure: a public endpoint that answers 200 to this identity is out of scope here.
      System.out.println("E2E-CELL cell=public-egress mode=" + mode() + " outcome=unexpected-200");
    } catch (OdpsException e) {
      String text = String.valueOf(e.getMessage()) + " " + e.getClass().getName();
      boolean reached = REQUEST_ID.matcher(text).find()
          || text.contains("ODPS-") || text.contains("AccessKeyId");
      assertTrue("the public service must have answered (request id / ODPS error code), was: "
          + firstLine(text), reached);
      System.out.println("E2E-CELL cell=public-egress mode=" + mode() + " outcome=answered "
          + firstLine(text));
    } finally {
      Authenticator.setDefault(null);
    }
  }

  /**
   * The Upsert (netty) egress against the real service. This is the cell the parent change left
   * open: the netty {@code CONNECT} credential was only ever measured against a local origin
   * ({@code UpsertProxyAuthTest}), never against the service a customer talks to, because that
   * needs a writable primary-key table. The table below is created once in the test project by the
   * work item that owns this case; nothing here drops or alters it, and the rows are written to
   * fixed primary keys so a rerun is an idempotent update instead of an accumulation.
   *
   * <p>Two shapes must not be confused (they are documented in {@code UpsertProxyAuthTest} too):
   * the netty data plane attaches {@code Proxy-Authorization} to the {@code CONNECT} itself, so the
   * proxy must see {@code auth=ok} on the very first attempt and never {@code auth=none}. The
   * session create / commit calls around it ride the JDK control plane, which answers a challenge
   * only through a host {@link Authenticator} &mdash; installed here for that reason, and asserted
   * separately by {@link #authenticatorAnswerIsScopedToTheProxy()}. The proxy-side counts are
   * judged per request kind by the driver that reads the proxy's own log.
   */
  @Test
  public void upsertWriteThroughRealServiceSucceeds() throws Exception {
    Odps odps = newOdpsWithProxy();
    if (proxyMode && proxyUser != null) {
      installScopedProxyAuthenticator();
    }
    try {
      TableTunnel tunnel = new TableTunnel(odps);
      TableTunnel.UpsertSession session =
          tunnel.buildUpsertSession(project, UPSERT_TABLE).build();
      UpsertStream stream = session.buildUpsertStream().build();
      for (int i = 0; i < UPSERT_IDS.length; i++) {
        Record record = session.newRecord();
        record.setBigint(0, UPSERT_IDS[i]);
        record.setString(1, "proxy-e2e-row-" + i);
        record.setBigint(2, 100L * (i + 1));
        stream.upsert(record);
      }
      stream.flush();
      stream.close();
      session.commit(false);

      // Read it back through the very same client: only the service can answer this.
      Instance readback = SQLTask.run(odps, project,
          "select count(*) as c, sum(amount) as s from " + UPSERT_TABLE + ";", null, null);
      readback.waitForSuccess();
      List<Record> rows = SQLTask.getResult(readback);
      assertEquals("one aggregate row expected", 1, rows.size());
      long count = Long.parseLong(String.valueOf(rows.get(0).get(0)));
      long sum = Long.parseLong(String.valueOf(rows.get(0).get(1)));
      assertEquals("the rows written through this egress must be visible", UPSERT_IDS.length, count);
      assertEquals("every value must have landed", 600L, sum);
      // The host the netty CONNECT must be authenticated to. The proxy-side judgement is scoped to
      // it so that a 407 round trip made by the JDK control plane (PLAIN requests to the same
      // host) can never be mistaken for the netty shape, and vice versa.
      String tunnelHost = new java.net.URI(
          odps.projects().get(project).getTunnelEndpoint(null)).getHost();
      System.out.println("E2E-CELL cell=upsert-write mode=" + mode() + " outcome=OK"
          + " upsertId=" + session.getId() + " rows=" + count + " sum=" + sum
          + " readback=" + readback.getId() + " tunnelHost=" + tunnelHost);
    } finally {
      Authenticator.setDefault(null);
    }
  }

  /**
   * Counter-case for the netty egress on the real service: with an address but no credential, the
   * proxy must turn the Upsert away. It runs against a second proxy address on purpose (see
   * {@link #PROXY_REFUSAL_ADDRESS}) so it cannot inherit a credential an earlier cell put in the
   * JVM's per-address cache, and it takes no {@link Authenticator} &mdash; netty does not use one,
   * which is exactly why the credential has to be in the {@link ProxyConfig}.
   *
   * <p>The shape is the proof, not just the failure: netty attaches the credential to the CONNECT,
   * so without one there is nothing to answer a {@code 407} with. The Upsert write above must show
   * {@code auth=ok} with no unauthenticated CONNECT at all; this one must show the opposite, and
   * neither may be used to argue about the other egress.
   */
  @Test
  public void upsertWithoutCredentialIsRefusedByTheRealProxy() {
    Assume.assumeTrue("only meaningful with an authenticating proxy", proxyMode && proxyUser != null);
    Authenticator.setDefault(null);
    Odps odps = OdpsTestUtils.newDefaultOdps();
    odps.getRestClient().setRetryTimes(0);
    ProxyConfig addressOnly = ProxyConfig.builder()
        .withHttpProxy(refusalAddress).withHttpsProxy(refusalAddress).build();
    odps.options().setProxyConfig(addressOnly);
    addressOnly.getProxy(ProxyConfig.Type.HTTP).ifPresent(odps.getRestClient()::setProxy);
    try {
      TableTunnel tunnel = new TableTunnel(odps);
      tunnel.buildUpsertSession(project, UPSERT_TABLE).build();
      fail("expected the proxy to refuse an Upsert session with no credential");
    } catch (Exception e) {
      String text = e.getClass().getName() + ": " + e.getMessage();
      Throwable cause = e.getCause();
      while (cause != null) {
        text = text + " / " + cause.getClass().getSimpleName() + ": " + cause.getMessage();
        cause = cause.getCause();
      }
      assertTrue("the failure must point at proxy authentication, was: " + firstLine(text),
          text.contains("407") || text.toLowerCase().contains("proxy"));
      System.out.println("E2E-CELL cell=upsert-no-credential mode=proxy outcome=refused "
          + firstLine(text));
    }
  }

  private static Odps newOdpsWithProxy() {
    return newOdpsWithProxy(proxyAddress);
  }

  private static Odps newOdpsWithProxy(String address) {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    odps.getRestClient().setRetryTimes(0);
    applyProxy(odps, address);
    return odps;
  }

  private static void applyProxy(Odps odps, String address) {
    ProxyConfig.Builder builder = ProxyConfig.builder();
    if (proxyMode) {
      if (proxyUser != null) {
        builder.withHttpProxy(address, proxyUser, proxyPassword)
            .withHttpsProxy(address, proxyUser, proxyPassword);
      } else {
        builder.withHttpProxy(address).withHttpsProxy(address);
      }
    }
    ProxyConfig config = builder.build();
    odps.options().setProxyConfig(config);
    // The options path installs the proxy on the RestClient; setProxy/setProxyCredential keep the
    // two entry points equivalent for this test.
    config.getProxy(ProxyConfig.Type.HTTP).ifPresent(odps.getRestClient()::setProxy);
    config.getProxyCredential(ProxyConfig.Type.HTTP)
        .ifPresent(odps.getRestClient()::setProxyCredential);
    config.getProxy(ProxyConfig.Type.HTTPS).ifPresent(odps.getRestClient()::setProxy);
    config.getProxyCredential(ProxyConfig.Type.HTTPS)
        .ifPresent(odps.getRestClient()::setProxyCredential);
  }

  private static void installScopedProxyAuthenticator() {
    Authenticator.setDefault(new ScopingAuthenticator(hostOf(proxyAddress),
        portOf(proxyAddress), proxyUser, proxyPassword));
  }

  private static String mode() {
    return proxyMode ? "proxy" : "direct";
  }

  private static String hostOf(String url) {
    String s = url.substring(url.indexOf("//") + 2);
    int slash = s.indexOf('/');
    if (slash >= 0) {
      s = s.substring(0, slash);
    }
    int colon = s.indexOf(':');
    return colon >= 0 ? s.substring(0, colon) : s;
  }

  private static int portOf(String url) {
    String s = url.substring(url.indexOf("//") + 2);
    int slash = s.indexOf('/');
    if (slash >= 0) {
      s = s.substring(0, slash);
    }
    int colon = s.indexOf(':');
    return colon >= 0 ? Integer.parseInt(s.substring(colon + 1)) : 80;
  }

  private static String hostAndPort(String url) {
    return hostOf(url) + ":" + portOf(url);
  }

  private static String firstLine(String s) {
    String line = s.replace('\n', ' ');
    return line.length() > 160 ? line.substring(0, 160) : line;
  }

  /**
   * Answers proxy challenges for one address only.
   *
   * <p>The protocol string is deliberately not used as the discriminator: for a plain HTTP request
   * sent through an HTTP proxy JDK 8 reports {@code "http"}, not {@code "Proxy"} (measured here on
   * 8/11/17/21/25). Matching the proxy address is both simpler and safe: a {@code 401} from the
   * MaxCompute service itself arrives with the service host as the requesting host, which is never
   * the loopback proxy, so this authenticator stays silent for it.
   */
  private static final class ScopingAuthenticator extends Authenticator {
    private final String host;
    private final int port;
    private final String user;
    private final String password;

    ScopingAuthenticator(String host, int port, String user, String password) {
      this.host = host;
      this.port = port;
      this.user = user;
      this.password = password;
    }

    @Override
    protected PasswordAuthentication getPasswordAuthentication() {
      return answerFor(getRequestingHost(), getRequestingPort());
    }

    PasswordAuthentication answerFor(String requestingHost, int requestingPort) {
      if (!host.equals(requestingHost) || port != requestingPort) {
        return null;
      }
      return new PasswordAuthentication(user, password.toCharArray());
    }
  }
}
