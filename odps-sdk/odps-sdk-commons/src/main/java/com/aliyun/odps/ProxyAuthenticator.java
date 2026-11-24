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

import java.lang.reflect.Method;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@code java.net.Authenticator} for proxies that require {@code Proxy-Authorization}, built from
 * the credentials carried by a {@link ProxyConfig}.
 *
 * <h2>Why this class exists at all</h2>
 *
 * The SDK's control plane (and everything that rides on it, including the Tunnel REST calls) talks
 * through {@code java.net.HttpURLConnection}
 * ({@code com.aliyun.odps.commons.transport.DefaultTransport}). That stack takes a
 * {@code java.net.Proxy}, which cannot carry a credential, and it does not let the caller put
 * {@code Proxy-Authorization} on the request it actually sends to the proxy for an {@code https}
 * endpoint — so an SDK-side credential alone cannot authenticate it. The only mechanism the JDK
 * offers on this stack is a JVM-wide {@code Authenticator}, which the SDK deliberately does not
 * install: doing so would take over authentication for every other component in the same JVM. This
 * class is the piece that a caller who <i>chooses</i> to own that hook does not have to write,
 * because getting its scope wrong breaks unrelated authentication.
 *
 * <h2>Scope: only the proxies in the {@code ProxyConfig} it was built from</h2>
 *
 * It answers a challenge only when all of the following hold, and returns {@code null} otherwise,
 * which is exactly what "no {@code Authenticator} installed" means to the JDK:
 *
 * <ul>
 *   <li>the challenge is for a proxy ({@code getRequestorType() == PROXY}), not for a server;</li>
 *   <li>the prompting host and port are one of the proxy addresses of that {@link ProxyConfig}.</li>
 * </ul>
 *
 * <p>The address, not the protocol, is the discriminator. Measured on JDK 8, 11, 17, 21 and 25, a
 * plaintext request to an {@code http} origin through an HTTP proxy hands the Authenticator
 * {@code protocol=http} — the same value a real {@code http} server challenge would carry — so
 * judging "is this a proxy challenge" on the protocol is not possible, and judging on the prompting
 * address is. See {@code ProxyAuthenticatorEgressTest} for the wire-level cells behind this.
 *
 * <h2>HTTPS endpoints need one more step, and it has to happen early</h2>
 *
 * A request to an {@code https} endpoint authenticates to the proxy on the {@code CONNECT}, which
 * the JDK itself writes. Since 8u121 the JVM refuses to answer a proxy challenge with the
 * {@code Basic} scheme on a tunneled connection unless {@code jdk.http.auth.tunneling.disabledSchemes}
 * says otherwise, and it reads that gate <i>once per JVM, when the first HTTP request goes out</i>.
 * Measured on JDK 8 (8u121 and later), 11, 17, 21 and 25: with the default value an installed
 * {@code Authenticator} is never consulted for a cold {@code CONNECT} and the caller just sees
 * {@code 407}. An 8u build older than that gate has no gate to open and authenticates the cold
 * {@code CONNECT} itself: the release regression box, on Java {@code 1.8.0_45-AliJVM}, is measured
 * sending one authenticated {@code CONNECT} (the original regression), so neither this method nor the
 * command-line flag changes anything there. Opening the gate
 * is therefore part of the same opt-in: {@link #enableProxyAuthForHttpsTunnel()} (equivalently
 * {@code -Djdk.http.auth.tunneling.disabledSchemes=} on the command line, which is the only form
 * that is guaranteed to be early enough in a JVM that already made requests).
 *
 * <h2>Credential changes inside one process</h2>
 *
 * The JVM caches the credential it used for a proxy address. After the password changes, the next
 * request is still sent with the old value first and is refused, and only then is this class asked
 * again — measured: two refused attempts, then the new credential, then success, identically on
 * JDK 8, 11, 17, 21 and 25. So calling {@link #install(ProxyConfig)} again after changing the
 * password does take effect; the cost is the extra round trips, not a permanently stale credential.
 * A proxy that locks an account after a couple of failed attempts will see those refusals, which is
 * the reason to change credentials at rest rather than in a running JVM where that is an option.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * ProxyConfig proxy = ProxyConfig.builder()
 *     .withHttpProxy("http://proxy.example.com:8080", user, password)
 *     .build();
 * odps.options().setProxyConfig(proxy);
 *
 * ProxyAuthenticator.enableProxyAuthForHttpsTunnel();   // before the first request, if https
 * if (!ProxyAuthenticator.install(proxy)) {
 *   // something else already owns Authenticator.setDefault: install this one yourself, or let
 *   // that owner answer the proxy challenge as well
 * }
 * }</pre>
 *
 * <p>The password is never part of {@code toString()}, of any log line, or of the returned
 * {@code PasswordAuthentication} until a matching challenge arrives.
 *
 * @see ProxyConfig
 * @see ProxyCredential
 */
public final class ProxyAuthenticator extends Authenticator {

  /**
   * JVM gate that keeps {@code Basic} out of proxy authentication on a {@code CONNECT} tunnel.
   * Read once per JVM, at the first HTTP request — which is why opening it from running code only
   * works if nothing has made a request yet.
   */
  static final String TUNNELING_DISABLED_SCHEMES = "jdk.http.auth.tunneling.disabledSchemes";

  private static final Logger LOG = Logger.getLogger(ProxyAuthenticator.class.getName());

  /**
   * {@code Authenticator.getDefault()}, or null when this JDK does not have it.
   *
   * <p>That getter was added in Java 9. On Java 8 — which this SDK still supports and still ships
   * against — a caller can set the JVM's authenticator but nobody can read who set it. So
   * "install unless somebody else already owns the hook" cannot be checked there, and guessing by
   * overwriting is exactly the failure this class was written to avoid.
   */
  private static final Method GET_DEFAULT = resolveGetDefault();

  private static Method resolveGetDefault() {
    try {
      return Authenticator.class.getMethod("getDefault");
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  /** Whether this JVM can report who owns {@code Authenticator.setDefault}. False on Java 8. */
  static boolean canReportInstalledAuthenticator() {
    return GET_DEFAULT != null;
  }

  /** The current default authenticator, or null when there is none or this JDK cannot tell. */
  private static Authenticator currentDefault() {
    if (GET_DEFAULT == null) {
      return null;
    }
    try {
      return (Authenticator) GET_DEFAULT.invoke(null);
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  /** One entry per configured proxy that carries a credential. */
  private final List<ProxyEntry> entries;

  private ProxyAuthenticator(List<ProxyEntry> entries) {
    this.entries = Collections.unmodifiableList(entries);
  }

  /**
   * Builds an authenticator for every entry of {@code config} that has both an address and a
   * credential. Which {@link ProxyConfig.Type} an entry was filed under does not matter here: the
   * JVM asks about a proxy, and the only thing that identifies it is the address it is prompting
   * from. Nothing is invented for an entry without a credential.
   *
   * @param config proxy configuration; may be null
   * @return the authenticator, or null when there is no credential to answer a challenge with
   */
  public static ProxyAuthenticator forProxyConfig(ProxyConfig config) {
    if (config == null || !config.hasProxyCredentials()) {
      return null;
    }
    List<ProxyEntry> entries = new ArrayList<>();
    for (ProxyConfig.Type type : ProxyConfig.Type.values()) {
      Proxy proxy = config.getProxy(type).orElse(null);
      ProxyCredential credential = config.getProxyCredential(type).orElse(null);
      if (proxy == null || credential == null || !(proxy.address() instanceof InetSocketAddress)) {
        continue;
      }
      entries.add(new ProxyEntry((InetSocketAddress) proxy.address(), credential));
    }
    return entries.isEmpty() ? null : new ProxyAuthenticator(entries);
  }

  /**
   * A proxy address plus the credential that belongs to it.
   *
   * <p>Two spellings of the same proxy are kept because the JDK does not have to hand back what was
   * configured: an {@code InetSocketAddress} built from {@code "::1"} reports
   * {@code 0:0:0:0:0:0:0:1} as its host string, and a reverse lookup can turn a numeric proxy into a
   * name. Both the textual forms and the resolved address are therefore matched.
   */
  private static final class ProxyEntry {
    private final Set<String> hostNames;
    private final InetAddress address; // may be null when the configured host never resolved
    private final int port;
    private final ProxyCredential credential;

    ProxyEntry(InetSocketAddress socketAddress, ProxyCredential credential) {
      this.hostNames = new HashSet<>();
      addHostName(this.hostNames, socketAddress.getHostString());
      this.address = socketAddress.getAddress();
      if (this.address != null) {
        addHostName(this.hostNames, this.address.getHostAddress());
      }
      this.port = socketAddress.getPort();
      this.credential = credential;
    }

    boolean matches(String promptingHost, int promptingPort, InetAddress promptingSite) {
      if (port != promptingPort) {
        return false;
      }
      String normalized = normalizeHost(promptingHost);
      if (normalized != null && hostNames.contains(normalized)) {
        return true;
      }
      // A reverse lookup on either side changes the spelling but not the machine. The numeric text
      // is compared instead of InetAddress.equals, which can reach for the name service.
      if (address != null && promptingSite != null) {
        String configured = normalizeHost(address.getHostAddress());
        String prompting = normalizeHost(promptingSite.getHostAddress());
        return configured != null && configured.equals(prompting);
      }
      return false;
    }

    private static void addHostName(Set<String> target, String host) {
      String normalized = normalizeHost(host);
      if (normalized != null) {
        target.add(normalized);
      }
    }
  }

  /**
   * Lowercase, no IPv6 brackets, no zone id, and IPv6 in its expanded form so both spellings agree.
   *
   * <p>Expanding is not decoration. {@code InetSocketAddress("::1", p).getHostString()} reports
   * {@code 0:0:0:0:0:0:0:1} while a challenge for that same proxy carries {@code ::1}, so matching
   * text alone fails open for exactly one spelling — which for an IPv6 proxy means a 407 with
   * nothing in the message to explain it.
   */
  private static String normalizeHost(String host) {
    if (host == null) {
      return null;
    }
    String value = host.trim().toLowerCase(Locale.ROOT);
    if (value.startsWith("[") && value.endsWith("]")) {
      value = value.substring(1, value.length() - 1);
    }
    int zone = value.indexOf('%');
    if (zone > 0) {
      value = value.substring(0, zone); // link-local scope id, e.g. fe80::1%eth0
    }
    if (value.isEmpty()) {
      return null;
    }
    return value.indexOf(':') >= 0 && value.indexOf('.') < 0 ? expandIpv6(value) : value;
  }

  /**
   * Expands the {@code ::} shorthand, so that {@code ::1} and {@code 0:0:0:0:0:0:0:1} — the form
   * {@link InetSocketAddress} reports — describe the same host. Anything that is not a parsable
   * IPv6 literal is returned as given: a proxy called {@code my:proxy} is unusual, and mis-expanding
   * it would be worse than not normalising it.
   */
  private static String expandIpv6(String value) {
    int shorthand = value.indexOf("::");
    if (shorthand < 0) {
      return value;
    }
    String head = value.substring(0, shorthand);
    String tail = value.substring(shorthand + 2);
    String[] headGroups = head.isEmpty() ? new String[0] : head.split(":");
    String[] tailGroups = tail.isEmpty() ? new String[0] : tail.split(":");
    List<String> groups = new ArrayList<>();
    Collections.addAll(groups, headGroups);
    for (int i = 8 - headGroups.length - tailGroups.length; i > 0; i--) {
      groups.add("0");
    }
    Collections.addAll(groups, tailGroups);
    if (groups.size() != 8) {
      return value;
    }
    StringBuilder expanded = new StringBuilder();
    for (int i = 0; i < groups.size(); i++) {
      if (i > 0) {
        expanded.append(':');
      }
      String group = groups.get(i);
      if (group.isEmpty() || group.length() > 4) {
        return value; // not a parsable IPv6 literal; leave the spelling as the caller wrote it
      }
      for (int j = 0; j < group.length(); j++) {
        if (Character.digit(group.charAt(j), 16) < 0) {
          return value;
        }
      }
      // Leading zeros off, as InetAddress prints them: 0000 -> 0, 0db8 -> db8.
      expanded.append(group.replaceFirst("^0+(?!$)", ""));
    }
    return expanded.toString();
  }

  /**
   * Makes an authenticator for {@code config} the JVM's default, unless some other component owns
   * that hook already.
   *
   * <ul>
   *   <li>nothing installed — installs this one, returns {@code true};</li>
   *   <li>a {@code ProxyAuthenticator} installed — replaces it, so a changed credential takes
   *       effect, returns {@code true};</li>
   *   <li>someone else's {@code Authenticator} installed — leaves it untouched, logs a warning and
   *       returns {@code false}. Overwriting it would take over another component's authentication,
   *       which is worse than not helping; build the authenticator with
   *       {@link #forProxyConfig(ProxyConfig)} and chain it from whoever owns the hook;</li>
   *   <li>no credentials in {@code config} — installs nothing, returns {@code false}. This is the
   *       case that keeps the default path untouched: a {@code ProxyConfig} without a credential
   *       never reaches this class.</li>
   * </ul>
   *
   * @param config the proxy configuration whose credentials should answer proxy challenges
   * @return true when this JVM's default authenticator is now an authenticator built here
   */
  public static synchronized boolean install(ProxyConfig config) {
    ProxyAuthenticator built = forProxyConfig(config);
    if (built == null) {
      return false;
    }
    if (!canReportInstalledAuthenticator()) {
      // Java 8: the owner of the hook cannot be read, so it cannot be protected. Refuse to guess
      // and hand the caller the one-liner that is always theirs to decide.
      LOG.log(Level.WARNING,
          "This JDK (" + System.getProperty("java.version") + ") cannot report who owns"
          + " java.net.Authenticator.setDefault, so install() will not touch it. If nothing else in"
          + " this process owns the hook, install the scoped authenticator directly:"
          + " Authenticator.setDefault(ProxyAuthenticator.forProxyConfig(config));");
      return false;
    }
    Authenticator current = currentDefault();
    if (current != null && !(current instanceof ProxyAuthenticator)) {
      LOG.log(Level.WARNING,
          "Not installing the MaxCompute proxy authenticator: java.net.Authenticator.getDefault() "
          + "is {0}, which this SDK will not replace. Proxy challenges will keep going to that "
          + "authenticator; chain com.aliyun.odps.ProxyAuthenticator from it if this process also "
          + "needs to authenticate to {1}",
          new Object[] {current.getClass().getName(), built.proxyAddresses()});
      return false;
    }
    Authenticator.setDefault(built);
    if (isProxyAuthForHttpsTunnelBlocked()) {
      LOG.log(Level.INFO,
          "Installed a proxy authenticator for {0}, but {1} currently keeps the Basic scheme out of "
          + "CONNECT tunnel authentication: requests to https endpoints will still be refused with "
          + "407. Call ProxyAuthenticator.enableProxyAuthForHttpsTunnel() before the first request, "
          + "or start the JVM with -D{1}=.",
          new Object[] {built.proxyAddresses(), TUNNELING_DISABLED_SCHEMES});
    }
    return true;
  }

  /**
   * Removes an authenticator installed through {@link #install(ProxyConfig)}. A no-op when the JVM's
   * default authenticator is not one built here, so a caller can never tear down somebody else's.
   *
   * @return true if an authenticator built here was removed
   */
  public static synchronized boolean uninstall() {
    if (!canReportInstalledAuthenticator()) {
      // Same reason as in install(): on Java 8 a blind setDefault(null) could clear a hook that
      // another component owns.
      LOG.log(Level.WARNING,
          "This JDK (" + System.getProperty("java.version") + ") cannot report who owns"
          + " java.net.Authenticator.setDefault, so uninstall() will not touch it.");
      return false;
    }
    if (currentDefault() instanceof ProxyAuthenticator) {
      Authenticator.setDefault(null);
      return true;
    }
    return false;
  }

  /**
   * Opens the JDK gate that keeps {@code Basic} out of proxy authentication on a CONNECT tunnel, so
   * that an {@code https} endpoint can authenticate to the proxy at all.
   *
   * <p>Returns whether anything changed. A {@code Basic} entry is removed from the list and other
   * schemes are left exactly as the caller set them. If no SecurityManager allows the write, the
   * {@code SecurityException} is logged and {@code false} is returned: the caller still gets a
   * {@code 407} on an {@code https} endpoint, which is the honest outcome rather than a claim that
   * authentication was enabled.
   *
   * <p>Must be called before this JVM makes its first HTTP request; the JDK reads the gate once.
   * In a JVM that is already past that point, use
   * {@code -Djdk.http.auth.tunneling.disabledSchemes=} at startup instead.
   *
   * @return true if the property was rewritten by this call
   */
  public static synchronized boolean enableProxyAuthForHttpsTunnel() {
    String current;
    try {
      current = System.getProperty(TUNNELING_DISABLED_SCHEMES);
    } catch (SecurityException e) {
      LOG.log(Level.WARNING, "Cannot read " + TUNNELING_DISABLED_SCHEMES
          + "; leaving it untouched. Use -D" + TUNNELING_DISABLED_SCHEMES + "= at startup.", e);
      return false;
    }
    if (current != null && !excludesBasic(current)) {
      // Already open: rewriting it would only risk racing another component.
      return false;
    }
    // An unset property means the JDK's own default, which excludes Basic, so the value written
    // back has to keep whatever else was listed and drop only Basic.
    List<String> kept = new ArrayList<>();
    if (current != null) {
      for (String scheme : current.split(",")) {
        String trimmed = scheme.trim();
        if (!trimmed.isEmpty() && !"basic".equalsIgnoreCase(trimmed)) {
          kept.add(trimmed);
        }
      }
    }
    try {
      System.setProperty(TUNNELING_DISABLED_SCHEMES, String.join(",", kept));
      return true;
    } catch (SecurityException e) {
      LOG.log(Level.WARNING, "Not allowed to rewrite " + TUNNELING_DISABLED_SCHEMES
          + "; an https endpoint will keep getting 407. Use -D"
          + TUNNELING_DISABLED_SCHEMES + "= at startup instead.", e);
      return false;
    }
  }

  /** Whether an https request through a proxy would be refused without anyone being asked. */
  static boolean isProxyAuthForHttpsTunnelBlocked() {
    String current;
    try {
      current = System.getProperty(TUNNELING_DISABLED_SCHEMES);
    } catch (SecurityException e) {
      // Cannot read it, so cannot claim anything about it.
      return false;
    }
    // Unset means the JDK default, and that default excludes Basic.
    return current == null || excludesBasic(current);
  }

  private static boolean excludesBasic(String schemes) {
    for (String scheme : schemes.split(",")) {
      if ("basic".equalsIgnoreCase(scheme.trim())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Answers a proxy challenge for one of the configured proxy addresses, and nothing else.
   */
  @Override
  protected PasswordAuthentication getPasswordAuthentication() {
    if (getRequestorType() != RequestorType.PROXY) {
      return null;
    }
    for (ProxyEntry entry : entries) {
      if (entry.matches(getRequestingHost(), getRequestingPort(), getRequestingSite())) {
        ProxyCredential credential = entry.credential;
        return new PasswordAuthentication(
            credential.getUsername(), credential.getPassword().toCharArray());
      }
    }
    return null;
  }

  /** The proxy addresses this authenticator will answer for; never a credential. */
  Set<String> proxyAddresses() {
    Set<String> addresses = new LinkedHashSet<>();
    for (ProxyEntry entry : entries) {
      addresses.add(entry.hostNames.isEmpty() ? String.valueOf(entry.port)
          : Collections.min(entry.hostNames) + ":" + entry.port);
    }
    return addresses;
  }

  /** Masked, like {@link ProxyCredential}: the password is never part of a representation. */
  @Override
  public String toString() {
    return "ProxyAuthenticator{proxies=" + proxyAddresses() + "}";
  }
}
