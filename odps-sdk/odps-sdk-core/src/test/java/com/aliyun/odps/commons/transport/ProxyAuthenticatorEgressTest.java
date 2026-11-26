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

package com.aliyun.odps.commons.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.List;

import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ProxyAuthenticator;
import com.aliyun.odps.ProxyConfig;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.rest.RestClient;

/**
 * What a request from the SDK's own control-plane stack looks like on a proxy that requires
 * authentication, and what changes when a caller opts in.
 *
 * <p>The stack under test is the real one: {@code Odps} → {@link RestClient} →
 * {@link DefaultTransport} → {@code java.net.HttpURLConnection}. The peer is
 * {@link LoopbackProxyStub}, a proxy on loopback that answers a request only when it carries the
 * fixture credential. Nothing here reaches a service, and no real credential appears anywhere in
 * the file or in the proxy's log.
 *
 * <p>The cells fall into two groups. The first is the promise that {@code ProxyAuthenticator} had to
 * be written for: with nothing installed the SDK behaves exactly as it did before the class existed
 * — same bytes on the wire, same refusal — and installing it changes the outcome without changing
 * anything else. The second group is where the JDK, not the SDK, sets the limit: on a build whose
 * tunneling gate exists and is at its default the JVM will not authenticate a {@code CONNECT} from a
 * cold cache, which is the difference between an {@code http} endpoint working through an
 * authenticating proxy and an {@code https} one not, and the reason the SDK does not install a
 * JVM-wide authenticator on somebody else's behalf. That gate is a JDK build property, not a
 * constant: the cells below measure what the running JVM does on its own and hold the SDK to that,
 * so a release never depends on which side of the boundary the test box happens to sit on
 * (the original regression).
 */
public class ProxyAuthenticatorEgressTest {

  private static final String USER = "loopback-user";
  private static final String PASSWORD = "test-proxy-password";
  private static final String RESOURCE = "/projects/wire_shapes";

  /** Java 8 can set the JVM-wide authenticator but cannot read who owns it; see the cells below. */
  private static final boolean CAN_READ_DEFAULT = canReadDefaultAuthenticator();

  /** Cached reading of the probe below: the JVM decides the tunneling gate once per JVM. */
  private static volatile Boolean jvmColdTunnelAuthenticated;

  private final Authenticator defaultBefore = CAN_READ_DEFAULT ? readDefault() : null;

  private static boolean canReadDefaultAuthenticator() {
    try {
      Authenticator.class.getMethod("getDefault");
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  private static Authenticator readDefault() {
    try {
      return (Authenticator) Authenticator.class.getMethod("getDefault").invoke(null);
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  /**
   * Installs the way a caller who owns the hook does it. {@code ProxyAuthenticator.install(config)}
   * is the convenience that additionally refuses to clobber a stranger, and on Java 8 it cannot
   * tell who owns the hook, so the wire cells go through the primitive they share.
   */
  private static void installScopedAuthenticator(ProxyConfig config) {
    Authenticator.setDefault(null);
    ProxyAuthenticator built = ProxyAuthenticator.forProxyConfig(config);
    assertNotNull("the fixture config carries a credential", built);
    Authenticator.setDefault(built);
  }

  @After
  public void restoreJvmWideHook() {
    Authenticator.setDefault(CAN_READ_DEFAULT ? defaultBefore : null);
  }

  // ---- the default path, which must not move ----------------------------------------------------

  /**
   * Configuring a proxy credential through the public option must not touch
   * {@code Authenticator.setDefault}: that hook belongs to the whole JVM, and taking it over from a
   * constructor or an option setter is exactly how a library breaks an unrelated component. A
   * studio, an agent or a container that already owns the hook keeps owning it.
   */
  @Test
  public void settingProxyCredentialsInstallsNoJvmWideAuthenticator() throws Exception {
    Assume.assumeTrue("only a JDK that can report the hook can prove nothing was installed",
                      CAN_READ_DEFAULT);
    try (LoopbackProxyStub proxy = new LoopbackProxyStub(USER, PASSWORD)) {
      assertNull("the JVM must start these cells with no authenticator installed, found "
                     + readDefault(), readDefault());

      newClient(proxy, true);

      assertNull("the SDK must not install one by itself", readDefault());
    }
  }

  /**
   * Today's behaviour, pinned so that a later change cannot quietly become the new default: the
   * credential is carried by the client but never reaches the JDK's tunnel-less request, the proxy
   * refuses, and the caller gets the 407 the proxy answered with (the original regression put that status
   * back into the exception; before it, this cell was a bare "Invalid connection").
   */
  @Test
  public void withoutAnAuthenticatorTheControlPlaneRequestIsRefusedByTheProxy() throws Exception {
    try (LoopbackProxyStub proxy = new LoopbackProxyStub(USER, PASSWORD)) {
      RestClient client = newClient(proxy, true);

      OdpsException e = expectFailure(client, "credential configured, nothing installed");

      assertEquals("the refusal is the proxy's 407", Integer.valueOf(407), e.getStatus());
      assertEquals("the proxy must not have seen a credential", 0, proxy.countAuth("ok"));
      assertEquals(1, proxy.countAuth("none"));
    }
  }

  /**
   * The bytes on the wire with a credential configured are the bytes without one. This is the
   * "default unchanged" claim in the form that can actually be checked: not "nothing should happen",
   * but "the request the proxy receives must be identical apart from the values every signed
   * request varies by".
   */
  @Test
  public void theRefusedRequestIsByteForByteTheOneWithoutCredentials() throws Exception {
    try (LoopbackProxyStub withCredentials = new LoopbackProxyStub(USER, PASSWORD);
         LoopbackProxyStub withoutCredentials = new LoopbackProxyStub(USER, PASSWORD)) {

      expectFailure(newClient(withCredentials, true), "with credentials");
      expectFailure(newClient(withoutCredentials, false), "without credentials");

      String a = shapeOf(withCredentials.headerBlocks());
      String b = shapeOf(withoutCredentials.headerBlocks());
      assertEquals("a ProxyConfig that carries a credential must not change the request shape",
                   a, b);
      assertFalse("no credential may leak onto a request nobody installed",
                  a.toLowerCase().contains("proxy-authorization"));
    }
  }

  // ---- the opt-in -----------------------------------------------------------------------------

  /**
   * Installing the authenticator is the whole opt-in: the same client, the same endpoint and the
   * same proxy now reach the origin. The refusal still goes first — that is the JVM answering a
   * challenge rather than pre-sending anything — and the retry carries the credential.
   */
  @Test
  public void installedAuthenticatorAnswersTheProxyChallengeAndTheRequestSucceeds() throws Exception {
    try (LoopbackProxyStub proxy = new LoopbackProxyStub(USER, PASSWORD)) {
      RestClient client = newClient(proxy, true);
      installScopedAuthenticator(config(proxy, true));

      try {
        Response resp = client.request(RESOURCE, "GET", null, null, (byte[]) null);
        assertEquals(200, resp.getStatus());
      } catch (OdpsException e) {
        fail("expected the request to get through the proxy, got " + describe(e)
             + "; proxy log=" + proxy.log());
      }

      assertEquals("407 first, then the retry with the credential", 1, proxy.countAuth("none"));
      assertEquals(1, proxy.countAuth("ok"));
      assertEquals("the credential must be the one from the ProxyConfig, log=" + proxy.log(),
                   0, proxy.countAuth("bad"));
    }
  }

  /**
   * The scope is the proxy address, not "whatever the JVM asked about". A second proxy in the path
   * must not be handed the first one's credential: that would turn a configuration aid into a way to
   * leak a password to the wrong host.
   */
  @Test
  public void anInstalledAuthenticatorIgnoresAProxyItWasNotBuiltFrom() throws Exception {
    try (LoopbackProxyStub configured = new LoopbackProxyStub(USER, PASSWORD);
         LoopbackProxyStub other = new LoopbackProxyStub(USER, PASSWORD)) {

      RestClient client = newClient(configured, true);
      installScopedAuthenticator(config(configured, true));

      try {
        assertEquals("the configured proxy must be answered", 200,
                     client.request(RESOURCE, "GET", null, null, (byte[]) null).getStatus());
      } catch (OdpsException e) {
        fail("the configured proxy should have been passed, got " + describe(e)
             + " log=" + configured.log());
      }
      assertEquals("one refusal, then the retry with the credential",
                   1, configured.countAuth("ok"));

      RestClient foreign = newClient(other, true);
      OdpsException e = expectFailure(foreign, "through a proxy nobody configured");

      assertEquals(Integer.valueOf(407), e.getStatus());
      assertEquals("the other proxy must never see this credential", 0, other.countAuth("ok"));
      assertEquals(1, other.countAuth("none"));
    }
  }

  // ---- where the JDK, not the SDK, draws the line ---------------------------------------------

  /**
   * Whether the running JVM authenticates a cold {@code CONNECT} through an authenticating proxy all
   * by itself, with {@code java.net.HttpURLConnection} and nothing from the SDK involved. Measured
   * once per JVM on a throwaway proxy address, because the JVM decides the tunneling gate the first
   * time it writes a {@code CONNECT} and never revisits it.
   *
   * <p>Printed as a reading rather than assumed, because the answer is a property of the JDK build:
   * a build that carries {@code jdk.http.auth.tunneling.disabledSchemes} and leaves it alone keeps
   * {@code Basic} out of tunnel authentication and answers {@code false}, while a build without that
   * gate authenticates the cold tunnel itself. The release regression box is the second kind — the original regression read Java {@code 1.8.0_45-AliJVM} taking one authenticated {@code CONNECT} on a cold
   * tunnel, which is what turned this case red. That build is not available here, so the gate-open
   * shape was measured on builds that do have the gate by opening it at startup
   * ({@code -Djdk.http.auth.tunneling.disabledSchemes=}), which is what the cells below compare
   * against; the probe reads the outcome on the wire, not the property.
   */
  private static boolean jvmAuthenticatesColdTunnel() throws Exception {
    Boolean measured = jvmColdTunnelAuthenticated;
    if (measured != null) {
      return measured;
    }
    synchronized (ProxyAuthenticatorEgressTest.class) {
      if (jvmColdTunnelAuthenticated == null) {
        try (LoopbackProxyStub probe = new LoopbackProxyStub(USER, PASSWORD)) {
          installScopedAuthenticator(config(probe, true));
          HttpURLConnection conn = (HttpURLConnection) new URL("https://origin.invalid/api")
              .openConnection(new Proxy(Proxy.Type.HTTP,
                                         new InetSocketAddress("127.0.0.1", probe.port())));
          conn.setConnectTimeout(5000);
          conn.setReadTimeout(5000);
          try {
            conn.getResponseCode();
          } catch (IOException expected) {
            // The stub never carries a tunnel, so the answer this cell needs is in the stub's log.
          } finally {
            conn.disconnect();
          }
          int ok = probe.countConnect("ok");
          int none = probe.countConnect("none");
          System.out.println("CELL jvm-cold-tunnel-probe jdk=" + System.getProperty("java.version")
                             + " gate=" + (ok == 0 ? "closed" : "open")
                             + " connect_none=" + none + " connect_ok=" + ok
                             + " log=" + probe.log());
          assertTrue("the probe must have put a CONNECT on the wire, log=" + probe.log(), none >= 1);
          jvmColdTunnelAuthenticated = ok > 0;
        }
      }
      return jvmColdTunnelAuthenticated;
    }
  }

  /**
   * An https endpoint reaches the proxy as a {@code CONNECT} that the JDK writes by itself. On a JVM
   * whose tunneling gate is at its default the cold cache neither sends a credential nor asks an
   * installed {@code Authenticator} for one, and that is pinned here so a future reader does not
   * "discover" that installing an authenticator was supposed to fix this. What the caller sees is the
   * proxy's refusal, and the honest description of that state is in
   * {@code docs/docs/operations/proxy-egress.md}.
   *
   * <p>The pin is about the default, so it only holds where the default is the one being tested: on a
   * JVM that measured itself as authenticating cold tunnels — an older 8u build without the gate, or
   * somebody else's startup flag that opened it — the cell is skipped with the reading above, and
   * {@link #theSdkSendsOnAColdTunnelOnlyWhatThisJvmSendsItself()} keeps asserting there.
   */
  @Test
  public void aColdHttpsTunnelIsNotAuthenticatedEvenWithAnAuthenticatorInstalled() throws Exception {
    Assume.assumeFalse("this JVM authenticates a cold CONNECT without being asked (see the "
                       + "jvm-cold-tunnel-probe line above); this cell pins the default gate, and "
                       + "the SDK-side invariant is pinned next to it",
                       jvmAuthenticatesColdTunnel());
    try (LoopbackProxyStub proxy = new LoopbackProxyStub(USER, PASSWORD)) {
      RestClient client = newHttpsClient(proxy);
      installScopedAuthenticator(config(proxy, true));

      try {
        Response resp = client.request(RESOURCE, "GET", null, null, (byte[]) null);
        // Answering 502 to an authorised CONNECT is the stub's lot in life, so reaching this line
        // at all would mean the JVM authenticated a cold tunnel; that is a change worth knowing.
        fail("the cold CONNECT was authenticated after all, status=" + resp.getStatus()
             + " log=" + proxy.log());
      } catch (OdpsException e) {
        assertTrue("expected a refused tunnel, got " + describe(e),
                   proxy.countConnect("none") >= 1);
        assertEquals("nothing may be sent on a cold tunnel", 0, proxy.countConnect("ok"));
      }
    }
  }

  /**
   * The invariant that holds on every build: on a cold {@code CONNECT} the SDK's control-plane stack
   * sends exactly what this JVM's own {@code HttpURLConnection} sends, and never a credential nobody
   * configured for this proxy address. Refusal first, at most one authenticated retry, no third
   * shape. This is what a release may rely on whichever side of the gate the box sits on.
   */
  @Test
  public void theSdkSendsOnAColdTunnelOnlyWhatThisJvmSendsItself() throws Exception {
    boolean jvmSendsItself = jvmAuthenticatesColdTunnel();
    try (LoopbackProxyStub proxy = new LoopbackProxyStub(USER, PASSWORD)) {
      RestClient client = newHttpsClient(proxy);
      installScopedAuthenticator(config(proxy, true));

      try {
        Response resp = client.request(RESOURCE, "GET", null, null, (byte[]) null);
        fail("the stub refuses to carry a tunnel, so this call must not succeed, status="
             + resp.getStatus() + " log=" + proxy.log());
      } catch (OdpsException e) {
        // Expected either way: a refused tunnel (407) or an authorised one the stub will not carry.
        System.out.println("CELL sdk-cold-tunnel jvm_authenticates=" + jvmSendsItself
                           + " status=" + e.getStatus() + " log=" + proxy.log());
      }

      assertEquals("the first CONNECT must go out unanswered, log=" + proxy.log(),
                   1, proxy.countConnect("none"));
      assertEquals("the SDK may send at most what this JVM sends by itself, log=" + proxy.log(),
                   jvmSendsItself ? 1 : 0, proxy.countConnect("ok"));
      assertEquals("a credential that was not configured must never reach the wire, log="
                   + proxy.log(), 0, proxy.countConnect("bad"));
    }
  }

  /**
   * The same https request after a plaintext request to the same proxy address does carry the
   * credential on the CONNECT: the JVM keeps the credential it obtained, keyed by the proxy address,
   * and reuses it without asking anyone again. Worth pinning because it is how a "proxy credentials
   * work" report can be true in one build of an application and false in another — the difference is
   * only whether something spoke plain HTTP through that proxy first.
   */
  @Test
  public void aWarmTunnelCarriesTheCredentialWithoutAskingAgain() throws Exception {
    try (LoopbackProxyStub proxy = new LoopbackProxyStub(USER, PASSWORD)) {
      RestClient plain = newClient(proxy, true);
      RestClient https = newHttpsClient(proxy);
      installScopedAuthenticator(config(proxy, true));

      assertEquals(200, plain.request(RESOURCE, "GET", null, null, (byte[]) null).getStatus());
      int askedForPlain = proxy.log().size();

      try {
        https.request(RESOURCE, "GET", null, null, (byte[]) null);
        fail("the stub refuses to carry a tunnel, so this call must not succeed");
      } catch (OdpsException e) {
        // expected: the tunnel itself is answered 502
      }

      List<String> after = proxy.log().subList(askedForPlain, proxy.log().size());
      assertEquals("the CONNECT must be authenticated from the cache: " + after,
                   1, countIn(after, "kind=CONNECT target=origin.invalid:443 auth=ok"));
      assertEquals("the JVM must not ask the Authenticator a second time: " + after,
                   0, countIn(after, " auth=none"));
    }
  }

  // ---- helpers ---------------------------------------------------------------------------------

  private static int countIn(List<String> lines, String fragment) {
    int n = 0;
    for (String line : lines) {
      if (line.contains(fragment)) {
        n++;
      }
    }
    return n;
  }

  /**
   * Both entries name the same proxy, which is what a caller with one corporate proxy writes.
   * {@code OdpsOptions} picks the entry by the scheme of the endpoint, so the tunnel cells need the
   * HTTPS one to exist at all.
   */
  private static ProxyConfig config(LoopbackProxyStub proxy, boolean withCredentials) {
    ProxyConfig.Builder builder = ProxyConfig.builder();
    if (withCredentials) {
      builder.withHttpProxy(proxy.endpoint(), USER, PASSWORD)
          .withHttpsProxy(proxy.endpoint(), USER, PASSWORD);
    } else {
      builder.withHttpProxy(proxy.endpoint()).withHttpsProxy(proxy.endpoint());
    }
    return builder.build();
  }

  /** A client wired the way {@code Odps.options().setProxyConfig(...)} wires one. */
  private RestClient newClient(LoopbackProxyStub proxy, boolean withCredentials) {
    return wiredClient(proxy, withCredentials, "http://origin.invalid/api");
  }

  /** Same, but with an https endpoint, which the JDK puts through a CONNECT. */
  private RestClient newHttpsClient(LoopbackProxyStub proxy) {
    return wiredClient(proxy, true, "https://origin.invalid/api");
  }

  private RestClient wiredClient(LoopbackProxyStub proxy, boolean withCredentials, String endpoint) {
    Odps odps = new Odps(new AliyunAccount("test-ak", "test-sk"));
    odps.setEndpoint(endpoint);
    odps.options().setProxyConfig(config(proxy, withCredentials));
    RestClient client = odps.getRestClient();
    client.setConnectTimeout(5);
    client.setReadTimeout(10);
    client.setRetryTimes(0);
    assertNotNull("the option must still hand the proxy to the REST client", client.getProxy());
    return client;
  }

  private OdpsException expectFailure(RestClient client, String shape) {
    try {
      Response resp = client.request(RESOURCE, "GET", null, null, (byte[]) null);
      throw new AssertionError("expected a refusal for " + shape + ", got status "
                               + resp.getStatus());
    } catch (OdpsException e) {
      System.out.println("CELL shape=" + shape + " status=" + e.getStatus()
                         + " message=" + firstLine(describe(e)));
      return e;
    }
  }

  private static String shapeOf(List<String> blocks) {
    assertFalse("the proxy must have seen the request", blocks.isEmpty());
    return LoopbackProxyStub.withoutVolatileHeaders(blocks.get(blocks.size() - 1));
  }

  private static String describe(Throwable t) {
    return t.getClass().getName() + ": " + t.getMessage()
           + (t.getCause() == null ? "" : " cause=" + t.getCause());
  }

  private static String firstLine(String s) {
    String line = s.replace('\n', ' ');
    return line.length() > 200 ? line.substring(0, 200) : line;
  }
}
