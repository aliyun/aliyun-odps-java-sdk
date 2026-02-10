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

package com.aliyun.odps.storage.settings;

import java.net.Proxy;
import java.time.Duration;

import com.aliyun.odps.ProxyConfig;

/**
 * Configuration class for HTTP connection settings used by the MaxCompute Storage API client.
 *
 * <p>This class allows configuration of various HTTP connection parameters including
 * timeouts, connection pooling, and proxy settings. It follows the builder pattern
 * to create immutable instances with default values that can be customized as needed.
 *
 * <p>Example usage:
 * <pre>{@code
 * HttpSettings httpSettings = HttpSettings.newBuilder()
 *     .withConnectTimeout(Duration.ofSeconds(30))
 *     .withReadTimeout(Duration.ofSeconds(300))
 *     .withMaxIdleConnections(10)
 *     .withKeepAliveDuration(Duration.ofMinutes(10))
 *     .build();
 * }</pre>
 */
public class HttpSettings {

  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(120);
  private static final Duration DEFAULT_KEEP_ALIVE_DURATION = Duration.ofMinutes(5);
  private static final int DEFAULT_MAX_IDLE_CONNECTIONS = 5;

  private Duration connectTimeout;

  private Duration readTimeout;

  private int maxIdleConnections;

  private Duration keepAliveDuration;

  private ProxyConfig proxyConfig;

  private HttpSettings() {
  }

  /**
   * Gets the connection timeout duration.
   *
   * <p>The connection timeout is the maximum time to wait when establishing
   * a connection to the server before giving up.
   *
   * @return The connection timeout duration
   */
  public Duration getConnectTimeout() {
    return connectTimeout;
  }

  /**
   * Gets the read timeout duration.
   *
   * <p>The read timeout is the maximum time to wait for data to be available
   * for reading from an established connection before giving up.
   *
   * @return The read timeout duration
   */
  public Duration getReadTimeout() {
    return readTimeout;
  }

  /**
   * Gets the maximum number of idle connections to keep in the connection pool.
   *
   * @return The maximum number of idle connections
   */
  public int getMaxIdleConnections() {
    return maxIdleConnections;
  }

  /**
   * Gets the keep-alive duration for idle connections.
   *
   * <p>Connections that have been idle for longer than this duration will be closed.
   *
   * @return The keep-alive duration
   */
  public Duration getKeepAliveDuration() {
    return keepAliveDuration;
  }

  /**
   * Gets the proxy configuration, if any.
   *
   * @return The proxy configuration or null if no proxy is configured
   */
  public ProxyConfig getProxyConfig() {
    return proxyConfig;
  }

  public static HttpSettingsBuilder newBuilder() {
    return new HttpSettingsBuilder();
  }

  public static final class HttpSettingsBuilder {

    private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private Duration readTimeout = DEFAULT_READ_TIMEOUT;
    private int maxIdleConnections = DEFAULT_MAX_IDLE_CONNECTIONS;
    private Duration keepAliveDuration = DEFAULT_KEEP_ALIVE_DURATION;
    private ProxyConfig proxyConfig;

    private HttpSettingsBuilder() {
    }

    /**
     * Sets the connection timeout duration.
     *
     * <p>The connection timeout is the maximum time to wait when establishing
     * a connection to the server before giving up.
     *
     * @param connectTimeout The connection timeout duration
     * @return This builder instance for method chaining
     */
    public HttpSettingsBuilder withConnectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /**
     * Sets the read timeout duration.
     *
     * <p>The read timeout is the maximum time to wait for data to be available
     * for reading from an established connection before giving up.
     *
     * @param readTimeout The read timeout duration
     * @return This builder instance for method chaining
     */
    public HttpSettingsBuilder withReadTimeout(Duration readTimeout) {
      this.readTimeout = readTimeout;
      return this;
    }

    /**
     * Sets the maximum number of idle connections to keep in the connection pool.
     *
     * @param maxIdleConnections The maximum number of idle connections
     * @return This builder instance for method chaining
     */
    public HttpSettingsBuilder withMaxIdleConnections(int maxIdleConnections) {
      this.maxIdleConnections = maxIdleConnections;
      return this;
    }

    /**
     * Sets the keep-alive duration for idle connections.
     *
     * <p>Connections that have been idle for longer than this duration will be closed.
     *
     * @param keepAliveDuration The keep-alive duration
     * @return This builder instance for method chaining
     */
    public HttpSettingsBuilder withKeepAliveDuration(Duration keepAliveDuration) {
      this.keepAliveDuration = keepAliveDuration;
      return this;
    }

    /**
     * Sets the proxy configuration.
     *
     * @param proxyConfig The proxy configuration or null for no proxy
     * @return This builder instance for method chaining
     */
    public HttpSettingsBuilder withProxyConfig(ProxyConfig proxyConfig) {
      this.proxyConfig = proxyConfig;
      return this;
    }

    /**
     * Builds and returns a new HttpSettings instance with the configured settings.
     *
     * @return A new HttpSettings instance
     */
    public HttpSettings build() {
      HttpSettings httpSettings = new HttpSettings();
      httpSettings.proxyConfig = this.proxyConfig;
      httpSettings.connectTimeout = this.connectTimeout;
      httpSettings.readTimeout = this.readTimeout;
      httpSettings.maxIdleConnections = this.maxIdleConnections;
      httpSettings.keepAliveDuration = this.keepAliveDuration;
      return httpSettings;
    }
  }
}
