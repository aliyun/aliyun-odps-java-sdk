package com.aliyun.odps;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * Credentials for a proxy that requires {@code Proxy-Authorization}.
 *
 * <p>A credential always belongs to one {@link ProxyConfig.Type} entry and is stored separately
 * from the proxy address, because {@link java.net.Proxy} cannot carry credentials. Keeping them
 * apart also means an HTTP stack that cannot send them (for example the JDK transport used by
 * {@code RestClient}) simply ignores the credential instead of silently mangling the endpoint.
 *
 * <p>Username and password are kept exactly as the caller provided them: no encoding, no
 * trimming, no lower-casing. {@link #proxyAuthorizationHeader()} is the only place where they are
 * serialized.
 */
public final class ProxyCredential {

  private static final String BASIC_PREFIX = "Basic ";

  private final String username;
  private final String password;

  /**
   * @param username proxy username, must not be null or empty
   * @param password proxy password, {@code null} is treated as an empty password
   */
  public ProxyCredential(String username, String password) {
    if (username == null || username.isEmpty()) {
      throw new IllegalArgumentException("Proxy username must not be null or empty");
    }
    this.username = username;
    this.password = password == null ? "" : password;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  /**
   * Builds the {@code Proxy-Authorization} header value for this credential.
   *
   * <p>Same shape as {@code okhttp3.Credentials.basic(user, pass)}: {@code Basic } plus
   * base64 (no line wrapping) of {@code "user:pass"} encoded as UTF-8, so an OkHttp based egress
   * and a netty based egress send byte-identical values for the same credential.
   */
  public String proxyAuthorizationHeader() {
    String raw = username + ":" + password;
    return BASIC_PREFIX
        + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ProxyCredential)) {
      return false;
    }
    ProxyCredential that = (ProxyCredential) other;
    return username.equals(that.username) && password.equals(that.password);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, password);
  }

  /** Masked representation: the password is never part of {@code toString()}. */
  @Override
  public String toString() {
    return "ProxyCredential{username='" + username + "', password='" + mask() + "'}";
  }

  private String mask() {
    return password.isEmpty() ? "" : "****";
  }
}
