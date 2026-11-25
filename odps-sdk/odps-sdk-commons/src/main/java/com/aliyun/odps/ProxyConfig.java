package com.aliyun.odps;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import com.aliyun.odps.utils.StringUtils;

/**
 * Proxy configuration for the SDK egress paths.
 *
 * <p>Each entry is a {@link Proxy} (host and port, {@link java.net.Proxy} cannot carry
 * credentials) plus an optional {@link ProxyCredential}. Endpoints may be written with userinfo
 * ({@code http://user:pass@proxy.example.com:8080}) or the credential may be passed explicitly
 * ({@code withHttpProxy("http://proxy.example.com:8080", user, pass)}); explicit fields win when
 * both are present.
 *
 * <p>Behaviour without credentials is unchanged: an endpoint that carries no credential parses to
 * exactly the same {@link Proxy} as before, and every consumer that cannot send
 * {@code Proxy-Authorization} keeps ignoring credentials rather than failing.
 */
public class ProxyConfig {

  public enum Type {
    DIRECT,
    HTTP,
    HTTPS,
    SOCKS4,
    SOCKS5
  }

  private final Map<Type, Proxy> proxyMap;

  private final Map<Type, ProxyCredential> credentialMap;

  // whether to disable netty dns resolver in local, if set true, the proxy server should be able to resolve it
  private final boolean disableNettyLocalResolver;

  private ProxyConfig(Map<Type, Proxy> proxyMap,
                      Map<Type, ProxyCredential> credentialMap,
                      boolean disableNettyLocalResolver) {
    this.proxyMap = Collections.unmodifiableMap(new EnumMap<>(proxyMap));
    this.credentialMap = Collections.unmodifiableMap(new EnumMap<>(credentialMap));
    this.disableNettyLocalResolver = disableNettyLocalResolver;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Optional<Proxy> getProxy(Type type) {
    return Optional.ofNullable(proxyMap.get(type));
  }

  /**
   * The credential for {@code type}, if the endpoint carried one.
   *
   * <p>Only meaningful for HTTP/HTTPS (sent as {@code Proxy-Authorization} on a {@code 407} or on
   * the netty {@code CONNECT}) and SOCKS5 (username/password authentication). SOCKS4 has no
   * authentication, so a credential there is dropped by the consumers, never silently invented.
   */
  public Optional<ProxyCredential> getProxyCredential(Type type) {
    return Optional.ofNullable(credentialMap.get(type));
  }

  /**
   * @return true when at least one entry carries credentials; used to keep the no-credential path
   *         byte-identical to previous SDK versions (no authenticator installed at all).
   */
  public boolean hasProxyCredentials() {
    return !credentialMap.isEmpty();
  }

  public boolean isDisableNettyLocalResolver() {
    return disableNettyLocalResolver;
  }

  /**
   * Masked on purpose: options are logged by several consumers and a proxy password must never be
   * one of the values that shows up there.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ProxyConfig{");
    boolean first = true;
    for (Map.Entry<Type, Proxy> entry : proxyMap.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(entry.getKey()).append('=').append(entry.getValue());
      ProxyCredential credential = credentialMap.get(entry.getKey());
      if (credential != null) {
        sb.append(" (user=").append(credential.getUsername()).append(", password=****)");
      }
    }
    return sb.append(", disableNettyLocalResolver=").append(disableNettyLocalResolver)
        .append('}').toString();
  }

  public static class Builder {

    private final Map<Type, Proxy> internalMap = new EnumMap<>(Type.class);
    private final Map<Type, ProxyCredential> credentialMap = new EnumMap<>(Type.class);
    private boolean loadEnv = false;
    private boolean disableNettyLocalResolver = false;

    public Builder withHttpProxy(String endpoint) {
      parseAndAdd(endpoint, Type.HTTP, Proxy.Type.HTTP, null, null);
      return this;
    }

    /**
     * HTTP proxy with credentials passed explicitly, bypassing any URL encoding rules: the
     * password is used exactly as given, including {@code :} and {@code %}.
     */
    public Builder withHttpProxy(String endpoint, String username, String password) {
      parseAndAdd(endpoint, Type.HTTP, Proxy.Type.HTTP, username, password);
      return this;
    }

    public Builder withHttpsProxy(String endpoint) {
      parseAndAdd(endpoint, Type.HTTPS, Proxy.Type.HTTP, null, null);
      return this;
    }

    /**
     * Proxy used for {@code https://} endpoints, with credentials passed explicitly.
     */
    public Builder withHttpsProxy(String endpoint, String username, String password) {
      parseAndAdd(endpoint, Type.HTTPS, Proxy.Type.HTTP, username, password);
      return this;
    }

    public Builder withSocks4Proxy(String endpoint) {
      parseAndAdd(endpoint, Type.SOCKS4, Proxy.Type.SOCKS, null, null);
      return this;
    }

    public Builder withSocks5Proxy(String endpoint) {
      parseAndAdd(endpoint, Type.SOCKS5, Proxy.Type.SOCKS, null, null);
      return this;
    }

    /**
     * SOCKS5 proxy with credentials passed explicitly (SOCKS5 username/password authentication).
     */
    public Builder withSocks5Proxy(String endpoint, String username, String password) {
      parseAndAdd(endpoint, Type.SOCKS5, Proxy.Type.SOCKS, username, password);
      return this;
    }

    public Builder loadFromEnvironment() {
      this.loadEnv = true;
      return this;
    }

    public Builder withDisableNettyLocalResolver(boolean disableNettyLocalResolver) {
      this.disableNettyLocalResolver = disableNettyLocalResolver;
      return this;
    }

    public ProxyConfig build() {
      if (loadEnv) {
        tryLoadFromEnv("HTTP_PROXY", Type.HTTP, Proxy.Type.HTTP);
        tryLoadFromEnv("HTTPS_PROXY", Type.HTTPS, Proxy.Type.HTTP);
        tryLoadFromEnv("SOCKS_PROXY", Type.SOCKS4, Proxy.Type.SOCKS);
        tryLoadFromEnv("SOCKS_PROXY", Type.SOCKS4, Proxy.Type.SOCKS);
      }
      return new ProxyConfig(internalMap, credentialMap, disableNettyLocalResolver);
    }

    private void tryLoadFromEnv(String envVar, Type type, Proxy.Type proxyType) {
      if (internalMap.containsKey(type)) {
        return;
      }
      String endpoint = System.getenv(envVar);
      if (StringUtils.isNotBlank(endpoint)) {
        parseAndAdd(endpoint, type, proxyType, null, null);
      }
    }

    private void parseAndAdd(String endpoint,
                             Type type,
                             Proxy.Type proxyType,
                             String username,
                             String password) {
      if (StringUtils.isBlank(endpoint)) {
        return;
      }
      ParsedProxy parsed = parse(endpoint, proxyType);
      internalMap.put(type, parsed.proxy);
      ProxyCredential credential = username == null || username.isEmpty()
          ? parsed.credential
          : new ProxyCredential(username, password);
      if (credential != null) {
        credentialMap.put(type, credential);
      }
    }

    /**
     * Parses an endpoint into host/port plus an optional credential.
     *
     * <p>{@link URI} stays the authority for host and port so that every endpoint accepted (or
     * rejected) by previous versions keeps behaving the same. The credential is taken from the raw
     * text of the endpoint instead: {@code URI.getUserInfo()} cannot be used as the single source,
     * because a password containing a literal {@code %} (very common) makes {@code new URI(...)}
     * fail before any component is read.
     */
    private static ParsedProxy parse(String endpoint, Proxy.Type proxyType) {
      Proxy proxy = strictProxy(endpoint, proxyType);
      // Read once, straight from the endpoint text (see the javadoc above on why getUserInfo() is
      // not usable as the single source).
      ProxyCredential credential = ProxyCredentialParser.fromRawUserInfo(rawUserInfo(endpoint));
      if (proxy == null) {
        // Text-level fallback for "scheme://[userinfo@]host:port", used only for endpoints that
        // URI itself refuses; genuinely malformed ones still fail the same way as before.
        proxy = looseProxy(endpoint, proxyType);
        if (proxy == null) {
          throw new IllegalArgumentException("Invalid proxy endpoint format: " + endpoint);
        }
      }
      return new ParsedProxy(proxy, credential);
    }

    private static Proxy strictProxy(String endpoint, Proxy.Type proxyType) {
      try {
        URI uri = new URI(endpoint);
        return new Proxy(proxyType, new InetSocketAddress(uri.getHost(), uri.getPort()));
      } catch (Exception e) {
        return null;
      }
    }

    /**
     * The userinfo text of the authority, still percent-encoded, or null when absent.
     */
    private static String rawUserInfo(String endpoint) {
      String authority = authorityOf(endpoint);
      if (authority == null) {
        return null;
      }
      int at = authority.lastIndexOf('@');
      return at < 0 ? null : authority.substring(0, at);
    }

    private static Proxy looseProxy(String endpoint, Proxy.Type proxyType) {
      String authority = authorityOf(endpoint);
      if (authority == null) {
        return null;
      }
      int at = authority.lastIndexOf('@');
      String hostPort = at < 0 ? authority : authority.substring(at + 1);
      String host;
      int port;
      if (hostPort.startsWith("[")) {
        int close = hostPort.indexOf(']');
        // A bracketed literal (IPv6) must be followed by ':' and a port, as in "[::1]:8080".
        if (close < 0 || close + 1 >= hostPort.length() || hostPort.charAt(close + 1) != ':') {
          return null;
        }
        host = hostPort.substring(1, close);
        hostPort = hostPort.substring(close + 2);
      } else {
        int colon = hostPort.indexOf(':');
        host = colon < 0 ? hostPort : hostPort.substring(0, colon);
        hostPort = colon < 0 ? "" : hostPort.substring(colon + 1);
      }
      if (host.isEmpty() || hostPort.isEmpty()) {
        return null;
      }
      int parsedPort;
      try {
        parsedPort = Integer.parseInt(hostPort.trim());
      } catch (NumberFormatException e) {
        return null;
      }
      // Same range java.net.InetSocketAddress accepts, so the fallback path cannot be stricter or
      // laxer than the URI path for an endpoint both of them read.
      if (parsedPort < 0 || parsedPort > 65535) {
        return null;
      }
      try {
        return new Proxy(proxyType, new InetSocketAddress(host, parsedPort));
      } catch (Exception e) {
        return null;
      }
    }

    /**
     * Text between {@code "://"} and the first {@code '/'} or {@code '?'}; null when the endpoint
     * has no scheme, which is the same shape previous versions rejected.
     */
    private static String authorityOf(String endpoint) {
      String value = endpoint.trim();
      int schemeEnd = value.indexOf("://");
      if (schemeEnd < 0) {
        return null;
      }
      String rest = value.substring(schemeEnd + 3);
      int end = rest.length();
      for (int i = 0; i < rest.length(); i++) {
        char c = rest.charAt(i);
        if (c == '/' || c == '?' || c == '#') {
          end = i;
          break;
        }
      }
      return rest.substring(0, end);
    }

    private static final class ParsedProxy {
      private final Proxy proxy;
      private final ProxyCredential credential;

      private ParsedProxy(Proxy proxy, ProxyCredential credential) {
        this.proxy = proxy;
        this.credential = credential;
      }
    }
  }
}
