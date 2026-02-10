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

package com.aliyun.odps.storage.internal;

import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.storage.internal.retry.RetryHandler;
import com.aliyun.odps.storage.settings.HttpSettings;

/**
 * Configuration class for StubSettings used by the MaxCompute Storage API client.
 *
 * <p>This class allows configuration of various settings for the storage stub including
 * credentials, endpoint, region, quota, retry settings, and HTTP settings. It follows
 * the builder pattern to create immutable instances that can be customized as needed.
 *
 * <p>Example usage:
 * <pre>{@code
 * StubSettings stubSettings = StubSettings.newBuilder()
 *     .withEndpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
 *     .withCredentialProvider(credentialsProvider)
 *     .withRegion("cn-hangzhou")
 *     .withQuota("payed")
 *     .withRetrySettings(retrySettings)
 *     .withHttpSettings(httpSettings)
 *     .build();
 * }</pre>
 */
public class StubSettings {

  private ICredentialsProvider credentialsProvider;

  private String endpoint;

  private String tunnelEndpoint;

  private String region;

  private String quota;

  private String project;

  private HttpSettings httpSettings;

  private String userAgent;

  private RetryHandler retryHandler;

  /**
   * Creates a new builder for constructing StubSettings instances.
   *
   * @return A new StubSettingsBuilder instance
   */
  public static StubSettingsBuilder newBuilder() {
    return new StubSettingsBuilder();
  }

  /**
   * Gets the credential provider.
   *
   * @return The credential provider
   */
  public ICredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  /**
   * Gets the endpoint.
   *
   * @return The endpoint
   */
  public String getEndpoint() {
    return endpoint;
  }


  /**
   * Gets the tunnel endpoint.
   *
   * @return The endpoint
   */
  public String getTunnelEndpoint() {
    return tunnelEndpoint;
  }

  /**
   * Gets the region.
   *
   * @return The region
   */
  public String getRegion() {
    return region;
  }

  /**
   * Gets the quota.
   *
   * @return The quota
   */
  public String getQuota() {
    return quota;
  }

  /**
   * Gets the project.
   *
   * @return The project
   */
  public String getProject() {
    return project;
  }

  

  /**
   * Gets the HTTP settings.
   *
   * @return The HTTP settings
   */
  public HttpSettings getHttpSettings() {
    return httpSettings;
  }


  /**
   * Gets the user agent.
   *
   * @return The user agent
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * Gets the retry handler.
   *
   * @return The retry handler
   */
  public RetryHandler getRetryHandler() {
    return retryHandler;
  }

  public static final class StubSettingsBuilder {

    private ICredentialsProvider credentialProvider;
    private String endpoint;
    private String tunnelEndpoint;
    private String region;
    private String quota;
    private String project;
    private HttpSettings httpSettings;
    private String userAgent;
    private RetryHandler retryHandler;

    private StubSettingsBuilder() {
    }

    /**
     * Sets the credential provider.
     *
     * @param credentialsProvider The credential provider
     * @return This builder instance for method chaining
     */
    public StubSettingsBuilder withCredentialsProvider(ICredentialsProvider credentialsProvider) {
      this.credentialProvider = credentialsProvider;
      return this;
    }

    /**
     * Sets the endpoint.
     *
     * @param endpoint The endpoint
     * @return This builder instance for method chaining
     */
    public StubSettingsBuilder withEndpoint(String endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    /**
     * Sets the tunnelEndpoint.
     *
     * @param tunnelEndpoint The endpoint
     * @return This builder instance for method chaining
     */
    public StubSettingsBuilder withTunnelEndpoint(String tunnelEndpoint) {
      this.tunnelEndpoint = tunnelEndpoint;
      return this;
    }

    /**
     * Sets the region.
     *
     * @param region The region
     * @return This builder instance for method chaining
     */
    public StubSettingsBuilder withRegion(String region) {
      this.region = region;
      return this;
    }

    /**
     * Sets the current project.
     *
     * @param project The project use to fetch tunnel endpoint
     * @return This builder instance for method chaining
     */
    public StubSettingsBuilder withProject(String project) {
      this.project = project;
      return this;
    }


    /**
     * Sets the quota.
     *
     * @param quota The quota
     * @return This builder instance for method chaining
     */
    public StubSettingsBuilder withQuota(String quota) {
      this.quota = quota;
      return this;
    }

    /**
     * Sets the HTTP settings.
     *
     * @param httpSettings The HTTP settings
     * @return This builder instance for method chaining
     */
    public StubSettingsBuilder withHttpSettings(HttpSettings httpSettings) {
      this.httpSettings = httpSettings;
      return this;
    }

    /**
     * Sets the request user agent
     *
     * @param userAgent The request UserAgent
     * @return This builder instance for method chaining
     */
    public StubSettingsBuilder withUserAgent(String userAgent) {
      this.userAgent = userAgent;
      return this;
    }

    /**
     * Sets the retry handler.
     *
     * @param retryHandler The retry handler
     * @return This builder instance for method chaining
     */
    public StubSettingsBuilder withRetryHandler(RetryHandler retryHandler) {
      this.retryHandler = retryHandler;
      return this;
    }

    /**
     * Builds and returns a new StubSettings instance with the configured settings.
     *
     * @return A new StubSettings instance
     */
    public StubSettings build() {
      StubSettings stubSettings = new StubSettings();
      stubSettings.httpSettings = this.httpSettings;
      stubSettings.endpoint = this.endpoint;
      stubSettings.tunnelEndpoint = this.tunnelEndpoint;
      stubSettings.quota = this.quota;
      stubSettings.project = this.project;
      stubSettings.credentialsProvider = this.credentialProvider;
      stubSettings.region = this.region;
      stubSettings.userAgent = this.userAgent;
      stubSettings.retryHandler = this.retryHandler;
      return stubSettings;
    }
  }
}
