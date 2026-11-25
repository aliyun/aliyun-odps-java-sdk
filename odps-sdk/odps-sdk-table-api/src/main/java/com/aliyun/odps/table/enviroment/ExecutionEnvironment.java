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

package com.aliyun.odps.table.enviroment;

import com.aliyun.odps.Odps;
import com.aliyun.odps.ProxyConfig;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.transport.TableTransport;
import com.aliyun.odps.tracing.RequestTrace;

import java.net.URI;
import java.net.URISyntaxException;

import static com.aliyun.odps.table.utils.ConfigConstants.*;

public abstract class ExecutionEnvironment {

    protected final EnvironmentSettings settings;

    private boolean initialized = false;

    public ExecutionEnvironment(EnvironmentSettings settings) {
        this.settings = settings;
    }

    public static ExecutionEnvironment create(EnvironmentSettings settings) {
        switch (settings.getExecutionMode()) {
            case REMOTE:
                return new RemoteEnvironment(settings);
            case LOCAL:
                return new LocalEnvironment(settings);
            case AUTOMATIC:
                return createDefaultEnvironment(settings);
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Initialize if not done previously.
     **/
    protected void ensureInitialized() {
        if (!initialized) {
            initialize();
            initialized = true;
        }
    }

    abstract protected void initialize();

    /**
     * Get service endpoint
     *
     * @return String
     */
    public String getServiceEndpoint() {
        return settings.getServiceEndpoint().orElseThrow(() ->
                new IllegalStateException("Get empty service endpoint from execution environment!"));
    }

    /**
     * Get tunnel endpoint
     *
     * @return String
     */
    abstract public String getTunnelEndpoint(String targetProject);


    /**
     * Get environment settings
     *
     * @return {@link EnvironmentSettings}.
     */
    public EnvironmentSettings getSettings() {
        return settings;
    }

    /**
     * Create http client
     *
     * @return {@link RestClient}.
     */
    public RestClient createHttpClient(String targetProject) {
        ensureInitialized();

        Credentials credentials = settings.getCredentials();
        Integer writeTimeoutSeconds = settings.getRestOptions()
                .flatMap(restOptions -> restOptions.getWriteTimeout())
                .orElse(null);
        RestClient restClient = new RestClient(new TableTransport(writeTimeoutSeconds));
        restClient.setRequestTrace(settings.getRequestTrace());
        restClient.enableTunnelRetryHeaders();
        restClient.setAccount(credentials.getAccount());
        credentials.getAppAccount().ifPresent(restClient::setAppAccount);
        credentials.getAppStsAccount().ifPresent(restClient::setAppStsAccount);
        try {
            URI endpoint = new URI(getTunnelEndpoint(targetProject));
            restClient.setEndpoint(endpoint.toString());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Execution environment get tunnel endpoint error:" + e.getMessage());
        }
        setRestOptions(restClient);
        return restClient;
    }

    /**
     * Create odps client
     *
     * @return {@link Odps}.
     */
    public Odps createOdpsClient() {
        Credentials credentials = settings.getCredentials();
        Odps odps = new Odps(credentials.getAccount());
        RequestTrace trace = settings.getRequestTrace();
        odps.options().setUserAgentSuffix(trace.getSuffix());
        if (trace.isManaged()) {
            odps.options().setTraceContext(trace.getParent(), trace.getState());
        }
        settings.getDefaultProject().ifPresent(odps::setDefaultProject);
        settings.getDefaultSchema().ifPresent(odps::setCurrentSchema);
        credentials.getAppAccount().ifPresent(odps::setAppAccount);
        credentials.getAppStsAccount().ifPresent(odps::setAppStsAccount);
        try {
            URI endpoint = new URI(getServiceEndpoint());
            odps.setEndpoint(endpoint.toString());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Execution environment get service endpoint error:" + e.getMessage());
        }
        // auto read from env to set proxy
        setRestOptions(odps.getRestClient());
        return odps;
    }

    void setRestOptions(RestClient restClient) {
        restClient.setUserAgent("StorageAPI");
        settings.getDefaultProject().ifPresent(restClient::setDefaultProject);
        settings.getDefaultSchema().ifPresent(restClient::setCurrentSchema);
        applyProxy(restClient);
        settings.getRestOptions().ifPresent(restOptions -> {
            restOptions.getConnectTimeout().ifPresent(restClient::setConnectTimeout);
            restOptions.getReadTimeout().ifPresent(restClient::setReadTimeout);
            restOptions.getUserAgent().ifPresent(restClient::setUserAgent);
            restOptions.getRetryTimes().ifPresent(restClient::setRetryTimes);
            restOptions.getRetryWaitTimeInSeconds().ifPresent(restClient::setRetryWaitTime);
            restOptions.isIgnoreCerts().ifPresent(restClient::setIgnoreCerts);
        });
    }

    /**
     * Applies the proxy configured through {@link EnvironmentSettings} to a client this module
     * builds.
     *
     * <p>{@code withHttpProxy/withHttpsProxy} used to be stored and never read: every request left
     * through whatever the host JVM happened to use, and there was no way at all to put an
     * authenticating proxy between this client and the service. {@code java.net.Proxy} cannot carry
     * a credential, so the credential travels next to it as {@link ProxyCredential}: the OkHttp
     * based connection ({@code OkHttpConnection}) sends it to the proxy, including when no write
     * timeout was explicitly configured. Requests without proxy credentials retain their previous
     * transport selection.
     *
     * <p>Which setting applies follows the scheme of the endpoint the client is about to talk to:
     * an https endpoint uses the https proxy, anything else the http proxy. When no proxy is
     * configured nothing is touched, so the default path keeps its previous shape.
     */
    void applyProxy(RestClient restClient) {
        ProxyConfig proxyConfig = proxyConfig();
        if (proxyConfig == null) {
            return;
        }
        ProxyConfig.Type type = isHttpsEndpoint(restClient.getEndpoint())
                ? ProxyConfig.Type.HTTPS
                : ProxyConfig.Type.HTTP;
        proxyConfig.getProxy(type).ifPresent(proxy -> {
            restClient.setProxy(proxy);
            restClient.setProxyCredential(proxyConfig.getProxyCredential(type).orElse(null));
        });
    }

    /**
     * Builds the proxy config from the two endpoint strings, which may carry userinfo credentials
     * ({@code http://user:pass@host:port}). {@code null} means "no proxy was configured".
     */
    private ProxyConfig proxyConfig() {
        String httpProxy = settings.getHttpProxy().orElse(null);
        String httpsProxy = settings.getHttpsProxy().orElse(null);
        boolean hasHttp = httpProxy != null && !httpProxy.isEmpty();
        boolean hasHttps = httpsProxy != null && !httpsProxy.isEmpty();
        if (!hasHttp && !hasHttps) {
            return null;
        }
        ProxyConfig.Builder builder = ProxyConfig.builder();
        if (hasHttp) {
            builder.withHttpProxy(httpProxy);
        }
        if (hasHttps) {
            builder.withHttpsProxy(httpsProxy);
        }
        return builder.build();
    }

    private static boolean isHttpsEndpoint(String endpoint) {
        return endpoint != null && endpoint.trim().toLowerCase().startsWith("https");
    }

    private static ExecutionEnvironment createDefaultEnvironment(EnvironmentSettings settings) {
        String mode = System.getenv(MAX_STORAGE_MODE);
        if (MAX_STORAGE_VSHM_MODE.equals(mode) ||
                MAX_STORAGE_NET_MODE.equals(mode)) {
            return new LocalEnvironment(settings);
        }
        return new RemoteEnvironment(settings);
    }
}
