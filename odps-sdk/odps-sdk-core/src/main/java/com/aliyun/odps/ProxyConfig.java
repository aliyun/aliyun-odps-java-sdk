package com.aliyun.odps;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import com.aliyun.odps.utils.StringUtils;

public class ProxyConfig {

  public enum Type {
    DIRECT,
    HTTP,
    HTTPS,
    SOCKS4,
    SOCKS5
  }

  private final Map<Type, Proxy> proxyMap;

  private ProxyConfig(Map<Type, Proxy> proxyMap) {
    this.proxyMap = Collections.unmodifiableMap(new EnumMap<>(proxyMap));
  }

  public static Builder builder() {
    return new Builder();
  }

  public Optional<Proxy> getProxy(Type type) {
    return Optional.ofNullable(proxyMap.get(type));
  }

  public static class Builder {

    private final Map<Type, Proxy> internalMap = new EnumMap<>(Type.class);
    private boolean loadEnv = false;

    public Builder withHttpProxy(String endpoint) {
      parseAndAdd(endpoint, Type.HTTP, Proxy.Type.HTTP);
      return this;
    }

    public Builder withHttpsProxy(String endpoint) {
      parseAndAdd(endpoint, Type.HTTPS, Proxy.Type.HTTP);
      return this;
    }

    public Builder withSocks4Proxy(String endpoint) {
      parseAndAdd(endpoint, Type.SOCKS4, Proxy.Type.SOCKS);
      return this;
    }

    public Builder withSocks5Proxy(String endpoint) {
      parseAndAdd(endpoint, Type.SOCKS5, Proxy.Type.SOCKS);
      return this;
    }

    public Builder loadFromEnvironment() {
      this.loadEnv = true;
      return this;
    }

    public ProxyConfig build() {
      if (loadEnv) {
        tryLoadFromEnv("HTTP_PROXY", Type.HTTP, Proxy.Type.HTTP);
        tryLoadFromEnv("HTTPS_PROXY", Type.HTTPS, Proxy.Type.HTTP);
        tryLoadFromEnv("SOCKS_PROXY", Type.SOCKS4, Proxy.Type.SOCKS);
        tryLoadFromEnv("SOCKS_PROXY", Type.SOCKS4, Proxy.Type.SOCKS);
      }
      return new ProxyConfig(internalMap);
    }

    private void tryLoadFromEnv(String envVar, Type type, Proxy.Type proxyType) {
      internalMap.computeIfAbsent(type, k -> {
        String endpoint = System.getenv(envVar);
        return StringUtils.isBlank(endpoint) ? null : parse(endpoint, proxyType);
      });
    }

    private void parseAndAdd(String endpoint, Type type, Proxy.Type proxyType) {
      if (StringUtils.isNotBlank(endpoint)) {
        internalMap.put(type, parse(endpoint, proxyType));
      }
    }

    private Proxy parse(String endpoint, Proxy.Type proxyType) {
      try {
        URI uri = new URI(endpoint);
        return new Proxy(proxyType, new InetSocketAddress(uri.getHost(), uri.getPort()));
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid proxy endpoint format: " + endpoint, e);
      }
    }
  }
}
