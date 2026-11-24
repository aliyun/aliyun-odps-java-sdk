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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.Authenticator;
import java.net.InetAddress;
import java.net.PasswordAuthentication;
import java.net.URL;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Scope and ownership rules of {@link ProxyAuthenticator}, checked without a socket: the challenge
 * is delivered through {@link Authenticator#requestPasswordAuthentication}, which is the same entry
 * point the JDK's HTTP stack uses, so what these cells assert is what a real 407 gets answered with.
 *
 * <p>The wire side — that an installed authenticator actually turns a refusing proxy into a 200,
 * and that installing nothing leaves the request byte-for-byte as it was — is
 * {@code ProxyAuthenticatorEgressTest} in odps-sdk-core.
 *
 * <p>Every credential here is a loopback fixture, never a real one.
 */
public class ProxyAuthenticatorTest {

  private static final String USER = "loopback-user";
  private static final String PASSWORD = "test-proxy-password";

  /**
   * Java 8 has no {@code readDefault()}, so on that lane the cells that assert who
   * owns the JVM-wide hook are skipped rather than silently dropped: the behaviour they pin is real
   * on every JDK the SDK supports, the *observation* of it is not available on the oldest one.
   */
  private static final boolean CAN_READ_DEFAULT = canReadDefaultAuthenticator();

  private Authenticator authenticatorBefore;
  private String tunnelGateBefore;

  private static boolean canReadDefaultAuthenticator() {
    try {
      Authenticator.class.getMethod("getDefault");
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  /**
   * The JVM's default authenticator, read reflectively.
   *
   * <p>{@code readDefault()} only exists from Java 9 on, so a direct call would not
   * even compile on the Java 8 lane this SDK still ships against. Null on that lane means "cannot
   * tell", which is what the guarded cells assume.
   */
  private static Authenticator readDefault() {
    try {
      return (Authenticator) Authenticator.class.getMethod("getDefault").invoke(null);
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  @Before
  public void captureJvmState() {
    authenticatorBefore = CAN_READ_DEFAULT ? readDefault() : null;
    tunnelGateBefore = System.getProperty(ProxyAuthenticator.TUNNELING_DISABLED_SCHEMES);
  }

  @After
  public void restoreJvmState() {
    // On Java 8 nothing can read the previous owner, and a test JVM starts with none installed.
    Authenticator.setDefault(CAN_READ_DEFAULT ? authenticatorBefore : null);
    if (tunnelGateBefore == null) {
      System.clearProperty(ProxyAuthenticator.TUNNELING_DISABLED_SCHEMES);
    } else {
      System.setProperty(ProxyAuthenticator.TUNNELING_DISABLED_SCHEMES, tunnelGateBefore);
    }
  }

  // ---- building it -----------------------------------------------------------------------------

  /** A proxy config without credentials has nothing to answer with, and must produce nothing. */
  @Test
  public void configWithoutCredentialsBuildsNoAuthenticator() {
    ProxyConfig addressOnly = ProxyConfig.builder()
        .withHttpProxy("http://127.0.0.1:18999")
        .build();
    assertFalse(addressOnly.hasProxyCredentials());
    assertNull(ProxyAuthenticator.forProxyConfig(addressOnly));
    assertNull(ProxyAuthenticator.forProxyConfig(null));
  }

  /** {@code install} on such a config is a no-op: the default path cannot be disturbed by it. */
  @Test
  public void installWithoutCredentialsTouchesNothing() {
    Assume.assumeTrue(CAN_READ_DEFAULT);
    ProxyConfig addressOnly = ProxyConfig.builder()
        .withHttpProxy("http://127.0.0.1:18999")
        .build();

    assertFalse(ProxyAuthenticator.install(addressOnly));
    assertSameAsBefore();
  }

  /** The masked representation is the only thing a log or an exception can pick up. */
  @Test
  public void stringFormNeverCarriesThePassword() {
    ProxyAuthenticator a = ProxyAuthenticator.forProxyConfig(
        ProxyConfig.builder().withHttpProxy("http://127.0.0.1:18999", USER, PASSWORD).build());
    assertNotNull(a);
    assertFalse(a.toString(), a.toString().contains(PASSWORD));
    assertTrue(a.toString(), a.toString().contains("127.0.0.1:18999"));
  }

  // ---- scope -----------------------------------------------------------------------------------

  /** The proxy this config names gets the credential. */
  @Test
  public void answersForTheConfiguredProxyAddress() {
    installFor("http://127.0.0.1:18999");

    PasswordAuthentication answer = askProxy("127.0.0.1", 18999);
    assertNotNull("a challenge from the configured proxy must be answered", answer);
    assertEquals(USER, answer.getUserName());
    assertEquals(PASSWORD, new String(answer.getPassword()));
  }

  /**
   * Another proxy on another port gets nothing. The alternative — answering whatever host the JVM
   * happens to ask about — would hand this proxy's password to any other proxy in the path, which
   * is the failure mode a scoped authenticator exists to avoid.
   */
  @Test
  public void staysSilentForADifferentProxyAddress() {
    installFor("http://127.0.0.1:18999");

    assertNull("a different proxy must not be handed this credential",
               askProxy("127.0.0.1", 19000));
    assertNull(askProxy("192.0.2.10", 18999));
  }

  /**
   * A server challenge (401 from the origin, not 407 from the proxy) is not ours to answer: it is
   * answered by nothing at all when no authenticator is installed, and staying silent keeps that
   * true for every other protocol in the JVM.
   */
  @Test
  public void staysSilentForAServerChallenge() {
    installFor("http://127.0.0.1:18999");

    assertNull(askServer("127.0.0.1", 18999));
  }

  /**
   * The discriminator is the prompting address, not {@code getRequestingProtocol()}: measured on
   * JDK 8, 11, 17, 21 and 25, a plaintext request through an HTTP proxy reports
   * {@code protocol=http}, the same value an origin server challenge carries. Pinned here so the
   * next reader does not rediscover it the way the original regression did.
   */
  @Test
  public void protocolIsNotPartOfTheDecision() {
    installFor("http://127.0.0.1:18999");

    assertNotNull(ask("127.0.0.1", 18999, "http", Authenticator.RequestorType.PROXY));
    assertNotNull("the tunnel challenge reports the tunnelled protocol, not the proxy's",
                  ask("127.0.0.1", 18999, "https", Authenticator.RequestorType.PROXY));
  }

  /** Host spelling is normalised: an IPv6 proxy configured with brackets still matches a challenge. */
  @Test
  public void ipv6ProxyAddressIsMatchedByEitherSpelling() {
    installFor("http://[::1]:18999");

    assertNotNull(askProxy("::1", 18999));
    assertNotNull(askProxy("[::1]", 18999));
  }

  // ---- ownership of the JVM-wide hook ----------------------------------------------------------

  /** Installing is allowed when the hook is free, and reports that it happened. */
  @Test
  public void installTakesAnUnownedHook() {
    Assume.assumeTrue(CAN_READ_DEFAULT);
    Authenticator.setDefault(null);

    assertTrue(ProxyAuthenticator.install(
        ProxyConfig.builder().withHttpProxy("http://127.0.0.1:18999", USER, PASSWORD).build()));
    assertTrue(readDefault() instanceof ProxyAuthenticator);
  }

  /**
   * Installing again replaces what this class installed, so a rotated password takes effect without
   * a restart. The JVM's own credential cache still replays the previous value first (see the
   * javadoc), but the value that is eventually sent is the new one.
   */
  @Test
  public void reinstallPicksUpTheNewCredential() {
    Assume.assumeTrue(CAN_READ_DEFAULT);
    Authenticator.setDefault(null);
    assertTrue(ProxyAuthenticator.install(
        ProxyConfig.builder().withHttpProxy("http://127.0.0.1:18999", USER, "old-password").build()));

    assertTrue(ProxyAuthenticator.install(
        ProxyConfig.builder().withHttpProxy("http://127.0.0.1:18999", USER, PASSWORD).build()));
    PasswordAuthentication answer = askProxy("127.0.0.1", 18999);
    assertNotNull(answer);
    assertEquals(PASSWORD, new String(answer.getPassword()));
  }

  /**
   * Someone else's authenticator is never replaced. A host application that owns
   * {@code Authenticator.setDefault} — a studio, an agent, a container — would otherwise lose its
   * own authentication because a MaxCompute client was constructed in the same JVM.
   */
  @Test
  public void refusesToClobberAnotherAuthenticator() {
    Assume.assumeTrue(CAN_READ_DEFAULT);
    Authenticator foreign = new Authenticator() {
      @Override
      protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication("someone-else", "whatever".toCharArray());
      }
    };
    Authenticator.setDefault(foreign);

    assertFalse(ProxyAuthenticator.install(
        ProxyConfig.builder().withHttpProxy("http://127.0.0.1:18999", USER, PASSWORD).build()));
    assertSame("install must leave a foreign authenticator in place",
               foreign, readDefault());
    PasswordAuthentication answer = askProxy("127.0.0.1", 18999);
    assertNotNull(answer);
    assertEquals("the existing owner still answers", "someone-else", answer.getUserName());
  }

  /**
   * Where the JDK cannot report the owner of the hook, {@code install()} must leave it alone rather
   * than overwrite a stranger. Pinned on the Java 8 lane; skipped where the owner can be read.
   */
  @Test
  public void installRefusesWhereTheOwnerOfTheHookCannotBeRead() {
    Assume.assumeFalse(CAN_READ_DEFAULT);
    assertFalse(ProxyAuthenticator.install(
        ProxyConfig.builder().withHttpProxy("http://127.0.0.1:18999", USER, PASSWORD).build()));
    assertFalse(ProxyAuthenticator.uninstall());
  }

  /** {@code uninstall} only ever removes what {@code install} put there. */
  @Test
  public void uninstallRemovesOnlyItsOwn() {
    Assume.assumeTrue(CAN_READ_DEFAULT);
    Authenticator.setDefault(null);
    assertFalse(ProxyAuthenticator.uninstall());

    assertTrue(ProxyAuthenticator.install(
        ProxyConfig.builder().withHttpProxy("http://127.0.0.1:18999", USER, PASSWORD).build()));
    assertTrue(ProxyAuthenticator.uninstall());
    assertNull(readDefault());

    Authenticator foreign = new Authenticator() {
      @Override
      protected PasswordAuthentication getPasswordAuthentication() {
        return null;
      }
    };
    Authenticator.setDefault(foreign);
    assertFalse(ProxyAuthenticator.uninstall());
    assertSame(foreign, readDefault());
  }

  // ---- the CONNECT tunnel gate -----------------------------------------------------------------

  /**
   * The JDK reads {@code jdk.http.auth.tunneling.disabledSchemes} once per JVM and, with nothing
   * set, keeps Basic out of CONNECT authentication — which is why an installed authenticator on its
   * own still leaves an https endpoint at 407 (pinned on the wire in
   * {@code ProxyAuthenticatorEgressTest}).
   */
  @Test
  public void blockedTunnelAuthenticationIsReportedWhenTheGateIsAtItsDefault() {
    System.clearProperty(ProxyAuthenticator.TUNNELING_DISABLED_SCHEMES);

    assertTrue("the JDK default excludes Basic from tunnel authentication",
               ProxyAuthenticator.isProxyAuthForHttpsTunnelBlocked());
  }

  /** Opening the gate removes Basic and leaves any other scheme the operator listed alone. */
  @Test
  public void openingTheTunnelGateDropsOnlyBasic() {
    System.setProperty(ProxyAuthenticator.TUNNELING_DISABLED_SCHEMES, "Basic,Digest");

    assertTrue(ProxyAuthenticator.enableProxyAuthForHttpsTunnel());
    assertEquals("Digest", System.getProperty(ProxyAuthenticator.TUNNELING_DISABLED_SCHEMES));
    assertFalse(ProxyAuthenticator.isProxyAuthForHttpsTunnelBlocked());
  }

  /** An already open gate is left alone, and its value is not rewritten. */
  @Test
  public void openingTheTunnelGateIsIdempotent() {
    System.setProperty(ProxyAuthenticator.TUNNELING_DISABLED_SCHEMES, "");
    assertFalse(ProxyAuthenticator.isProxyAuthForHttpsTunnelBlocked());
    assertFalse("already open", ProxyAuthenticator.enableProxyAuthForHttpsTunnel());

    System.setProperty(ProxyAuthenticator.TUNNELING_DISABLED_SCHEMES, "Digest");
    assertFalse("a gate that never excluded Basic is not our business",
                ProxyAuthenticator.enableProxyAuthForHttpsTunnel());
    assertEquals("Digest", System.getProperty(ProxyAuthenticator.TUNNELING_DISABLED_SCHEMES));
  }

  /**
   * The unset case is the one users actually meet, and it must still work: leaving the property
   * alone would leave the JDK's own default (Basic excluded) in place, so "call it and nothing
   * happens" would be the answer to the exact question the method exists for.
   */
  @Test
  public void openingTheTunnelGateFromTheDefaultWritesAnEmptyList() {
    System.clearProperty(ProxyAuthenticator.TUNNELING_DISABLED_SCHEMES);
    assertTrue(ProxyAuthenticator.isProxyAuthForHttpsTunnelBlocked());

    assertTrue(ProxyAuthenticator.enableProxyAuthForHttpsTunnel());
    assertEquals("", System.getProperty(ProxyAuthenticator.TUNNELING_DISABLED_SCHEMES));
    assertFalse(ProxyAuthenticator.isProxyAuthForHttpsTunnelBlocked());
  }

  // ---- helpers ---------------------------------------------------------------------------------

  /**
   * Sets the hook the way a caller does when they own it: build the scoped authenticator, install
   * it. Used by the scoping cells so they run on Java 8 as well, where {@code install()} refuses to
   * touch a hook it cannot read the owner of.
   */
  private void installFor(String endpoint) {
    Authenticator.setDefault(null);
    ProxyAuthenticator built = ProxyAuthenticator.forProxyConfig(
        ProxyConfig.builder().withHttpProxy(endpoint, USER, PASSWORD).build());
    assertNotNull("the fixture config carries a credential", built);
    Authenticator.setDefault(built);
  }

  private PasswordAuthentication askProxy(String host, int port) {
    return ask(host, port, "http", Authenticator.RequestorType.PROXY);
  }

  private PasswordAuthentication askServer(String host, int port) {
    return ask(host, port, "http", Authenticator.RequestorType.SERVER);
  }

  /** The JDK's own entry into the default authenticator, with the challenge fields it would set. */
  private static PasswordAuthentication ask(String host, int port, String protocol,
                                            Authenticator.RequestorType type) {
    try {
      String literal = loopbackFor(host);
      // An IPv6 host needs brackets to be a URL authority at all.
      String urlHost = literal.indexOf(':') >= 0 ? "[" + literal + "]" : literal;
      return Authenticator.requestPasswordAuthentication(
          host, InetAddress.getByName(literal), port, protocol, "mcc-test-realm",
          "basic", new URL("http://" + urlHost + ":" + port + "/probe"), type);
    } catch (Exception e) {
      throw new AssertionError("cannot deliver the synthetic challenge for " + host + ":" + port
                               + " type=" + type + ": " + e, e);
    }
  }

  /** {@code [::1]} is not a hostname; the bracketed spelling still means loopback. */
  private static String loopbackFor(String host) {
    String normalized = host.replace("[", "").replace("]", "");
    return "::1".equals(normalized) || "0:0:0:0:0:0:1".equals(normalized) ? "::1" : normalized;
  }

  private void assertSameAsBefore() {
    if (authenticatorBefore == null) {
      assertNull(readDefault());
    } else {
      assertSame(authenticatorBefore, readDefault());
    }
  }

}
