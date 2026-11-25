package com.aliyun.odps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.junit.Test;

/**
 * Proxy credential parsing (A1) and the guarantee that a credential-less configuration parses to
 * exactly what previous SDK versions produced.
 *
 * <p>Every expected value here is the shape that a proxy must receive on the wire, cross-checked
 * against {@link ProxyCredential#proxyAuthorizationHeader()} in {@link #headerMatchesOkHttpShape()}.
 */
public class ProxyConfigCredentialTest {

  private static InetSocketAddress address(ProxyConfig config, ProxyConfig.Type type) {
    return (InetSocketAddress) config.getProxy(type).orElseThrow(
        () -> new AssertionError("proxy missing for " + type)).address();
  }

  private static String user(ProxyConfig config, ProxyConfig.Type type) {
    return config.getProxyCredential(type).orElseThrow(
        () -> new AssertionError("credential missing for " + type)).getUsername();
  }

  private static String password(ProxyConfig config, ProxyConfig.Type type) {
    return config.getProxyCredential(type).orElseThrow(
        () -> new AssertionError("credential missing for " + type)).getPassword();
  }

  @Test
  public void testUserInfoInEndpointIsParsed() {
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://proxyuser:proxypass@proxy.example.com:8080")
        .build();

    InetSocketAddress addr = address(config, ProxyConfig.Type.HTTP);
    assertEquals("proxy.example.com", addr.getHostName());
    assertEquals(8080, addr.getPort());
    assertEquals("proxyuser", user(config, ProxyConfig.Type.HTTP));
    assertEquals("proxypass", password(config, ProxyConfig.Type.HTTP));
    // java.net.Proxy itself still carries host and port only.
    assertEquals(Proxy.Type.HTTP, config.getProxy(ProxyConfig.Type.HTTP).get().type());
  }

  @Test
  public void testColonInsidePasswordIsNotTruncated() {
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://alice:pa:ss:word@proxy.example.com:8080")
        .build();

    assertEquals("alice", user(config, ProxyConfig.Type.HTTP));
    assertEquals("pa:ss:word", password(config, ProxyConfig.Type.HTTP));
  }

  @Test
  public void testRawPercentInPasswordIsKeptAndUsedToBeRejected() {
    // A literal '%' is not valid percent-encoding, so new URI(...) refuses this endpoint and the
    // previous SDK version raised "Invalid proxy endpoint format". The password shape reported by
    // enterprise identity providers is exactly this, so it must now parse and stay intact.
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://alice:100%off@proxy.example.com:8080")
        .build();

    assertEquals("alice", user(config, ProxyConfig.Type.HTTP));
    assertEquals("100%off", password(config, ProxyConfig.Type.HTTP));
    assertEquals("proxy.example.com", address(config, ProxyConfig.Type.HTTP).getHostName());
    assertEquals(8080, address(config, ProxyConfig.Type.HTTP).getPort());
  }

  @Test
  public void testPercentEscapesAreDecodedExactlyOnce() {
    ProxyConfig plain = ProxyConfig.builder()
        .withHttpProxy("http://alice:p%40ss%3Aword@proxy.example.com:8080")
        .build();
    assertEquals("alice", user(plain, ProxyConfig.Type.HTTP));
    assertEquals("p@ss:word", password(plain, ProxyConfig.Type.HTTP));

    // "%2525" is an encoded '%25': decoding once must yield "%25", not "%".
    ProxyConfig doubleEncoded = ProxyConfig.builder()
        .withHttpProxy("http://alice:%2525@proxy.example.com:8080")
        .build();
    assertEquals("%25", password(doubleEncoded, ProxyConfig.Type.HTTP));

    // "%25" alone decodes to a single literal '%'.
    ProxyConfig encodedPercent = ProxyConfig.builder()
        .withHttpProxy("http://alice:a%25b@proxy.example.com:8080")
        .build();
    assertEquals("a%b", password(encodedPercent, ProxyConfig.Type.HTTP));
  }

  @Test
  public void testAtSignInPasswordIsSplitAtTheLastDelimiter() {
    ProxyConfig encoded = ProxyConfig.builder()
        .withHttpProxy("http://alice:p%40ss@proxy.example.com:8080")
        .build();
    assertEquals("alice", user(encoded, ProxyConfig.Type.HTTP));
    assertEquals("p@ss", password(encoded, ProxyConfig.Type.HTTP));

    // Unencoded '@' in the password: the last '@' separates userinfo from host, so the password
    // keeps the earlier one. This is what a user writes when they forget to encode.
    // The last '@' separates userinfo from the host (a host can never contain '@'), so a
    // password written without encoding still lands whole in the password field.
    ProxyConfig literal = ProxyConfig.builder()
        .withHttpProxy("http://alice:pa@ss@proxy.example.com:8080")
        .build();
    assertEquals("alice", user(literal, ProxyConfig.Type.HTTP));
    assertEquals("pa@ss", password(literal, ProxyConfig.Type.HTTP));
  }

  @Test
  public void testExplicitCredentialWinsAndIsNeverRewritten() {
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://ignored:ignored@proxy.example.com:8080", "alice", "100%off:a:b")
        .build();

    assertEquals("alice", user(config, ProxyConfig.Type.HTTP));
    assertEquals("100%off:a:b", password(config, ProxyConfig.Type.HTTP));
    assertEquals("proxy.example.com", address(config, ProxyConfig.Type.HTTP).getHostName());
  }

  @Test
  public void testHttpsAndSocks5Credentials() {
    ProxyConfig config = ProxyConfig.builder()
        .withHttpsProxy("https://tunneluser:tunnelpass@proxy.example.com:8443")
        .withSocks5Proxy("socks5://socksuser:sockspass@socks.example.com:1080")
        .build();

    assertEquals("tunneluser", user(config, ProxyConfig.Type.HTTPS));
    assertEquals("tunnelpass", password(config, ProxyConfig.Type.HTTPS));
    assertEquals(Proxy.Type.HTTP, config.getProxy(ProxyConfig.Type.HTTPS).get().type());
    assertEquals("socksuser", user(config, ProxyConfig.Type.SOCKS5));
    assertEquals("sockspass", password(config, ProxyConfig.Type.SOCKS5));
    assertEquals(Proxy.Type.SOCKS, config.getProxy(ProxyConfig.Type.SOCKS5).get().type());
  }

  @Test
  public void testNoCredentialKeepsTheOldShape() {
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://proxy.example.com:8080")
        .withHttpsProxy("http://proxy.example.com:8443")
        .build();

    assertFalse(config.hasProxyCredentials());
    assertFalse(config.getProxyCredential(ProxyConfig.Type.HTTP).isPresent());
    assertEquals(8080, address(config, ProxyConfig.Type.HTTP).getPort());
    assertEquals(8443, address(config, ProxyConfig.Type.HTTPS).getPort());
  }

  @Test
  public void testBlankUserInfoDoesNotInventACredential() {
    // Endpoints like "http://@host:8080" were accepted before (userinfo dropped) and connected
    // without proxy auth. They must keep doing that instead of failing or sending "Basic Og==".
    ProxyConfig config = ProxyConfig.builder().withHttpProxy("http://@proxy.example.com:8080").build();

    assertFalse(config.hasProxyCredentials());
    assertEquals("proxy.example.com", address(config, ProxyConfig.Type.HTTP).getHostName());

    ProxyConfig userOnly = ProxyConfig.builder()
        .withHttpProxy("http://onlyuser@proxy.example.com:8080")
        .build();
    assertEquals("onlyuser", user(userOnly, ProxyConfig.Type.HTTP));
    assertEquals("", password(userOnly, ProxyConfig.Type.HTTP));
  }

  @Test
  public void testSchemelessEndpointIsStillRejected() {
    try {
      ProxyConfig.builder().withHttpProxy("proxy.example.com:8080").build();
      fail("expected IllegalArgumentException for an endpoint without scheme");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Invalid proxy endpoint format"));
    }
  }

  @Test
  public void testBadPortIsRejectedInBothParsePaths() {
    for (String endpoint : new String[] {"http://proxy.example.com:notaport",
        "http://proxy.example.com:70000", "http://alice:100%off@proxy.example.com:70000",
        "http://proxy.example.com:"}) {
      try {
        ProxyConfig.builder().withHttpProxy(endpoint).build();
        fail("expected IllegalArgumentException for " + endpoint);
      } catch (IllegalArgumentException expected) {
        assertTrue(expected.getMessage().contains("Invalid proxy endpoint format"));
      }
    }
  }

  @Test
  public void testIpv6LiteralWithCredential() {
    // Raw '%' in the password forces the text-level parse, which has to keep the bracketed IPv6
    // literal working.
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://alice:100%off@[::1]:8080")
        .build();

    assertEquals("alice", user(config, ProxyConfig.Type.HTTP));
    assertEquals("100%off", password(config, ProxyConfig.Type.HTTP));
    InetSocketAddress addr = address(config, ProxyConfig.Type.HTTP);
    assertEquals(8080, addr.getPort());
    assertTrue(addr.getAddress() instanceof java.net.Inet6Address);
  }

  @Test
  public void testToStringNeverContainsThePassword() {
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://alice:sup3rs3cret%off@proxy.example.com:8080")
        .build();

    String rendered = config.toString();
    assertFalse(rendered, rendered.contains("sup3rs3cret%off"));
    assertTrue(rendered, rendered.contains("alice"));

    ProxyCredential credential = config.getProxyCredential(ProxyConfig.Type.HTTP).get();
    assertFalse(credential.toString(), credential.toString().contains("sup3rs3cret%off"));
  }

  @Test
  public void testHeaderMatchesOkHttpShape() {
    // "Basic " + base64(UTF-8 "user:pass") is what the probe that produced the 26-cell matrix sent,
    // via okhttp3.Credentials.basic(user, pass). Pinned here without an okhttp dependency so the
    // netty path is covered by the same value.
    ProxyCredential credential = new ProxyCredential("alice", "p@ss:中");
    String expected = "Basic " + Base64.getEncoder().encodeToString(
        "alice:p@ss:中".getBytes(StandardCharsets.UTF_8));
    assertEquals(expected, credential.proxyAuthorizationHeader());

    assertEquals("Basic " + Base64.getEncoder().encodeToString("alice:".getBytes(
        StandardCharsets.UTF_8)), new ProxyCredential("alice", null).proxyAuthorizationHeader());

    // Never wrap the base64 across lines: a folded header value is not what the proxy expects.
    StringBuilder longPassword = new StringBuilder();
    for (int i = 0; i < 40; i++) {
      longPassword.append("0123456789");
    }
    String header = new ProxyCredential("alice", longPassword.toString())
        .proxyAuthorizationHeader();
    assertFalse(header, header.contains("\n"));
    assertFalse(header, header.contains("\r"));
  }

  @Test
  public void testCredentialIsPlumbedToRestClient() {
    Odps odps = new Odps(new com.aliyun.odps.account.AliyunAccount("access", "secret"));
    odps.setEndpoint("http://service.example.com/api");

    ProxyConfig withCredential = ProxyConfig.builder()
        .withHttpProxy("http://alice:secret@proxy.example.com:8080")
        .build();
    odps.options().setProxyConfig(withCredential);
    assertEquals("alice", odps.getRestClient().getProxyCredential().getUsername());
    assertEquals("secret", odps.getRestClient().getProxyCredential().getPassword());

    ProxyConfig withoutCredential = ProxyConfig.builder()
        .withHttpProxy("http://proxy.example.com:8080")
        .build();
    odps.options().setProxyConfig(withoutCredential);
    assertNull(odps.getRestClient().getProxyCredential());
    assertEquals(8080, ((InetSocketAddress) odps.getRestClient().getProxy().address()).getPort());

    odps.options().setProxyConfig(null);
    assertNull(odps.getRestClient().getProxy());
    assertNull(odps.getRestClient().getProxyCredential());
  }

  @Test
  public void testParserSplitsBeforeDecoding() {
    // An encoded ':' in the username must not become the username/password delimiter.
    ProxyCredential credential = ProxyCredentialParser.fromRawUserInfo("a%3Ab:pw");
    assertEquals("a:b", credential.getUsername());
    assertEquals("pw", credential.getPassword());

    assertNull(ProxyCredentialParser.fromRawUserInfo(null));
    assertNull(ProxyCredentialParser.fromRawUserInfo(""));
  }
}
