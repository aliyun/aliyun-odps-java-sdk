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

package com.aliyun.odps.table.impl.v2;

import java.time.Duration;

import com.aliyun.odps.account.Account;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.StubSettings;
import com.aliyun.odps.storage.settings.HttpSettings;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.commons.transport.Request;

/**
 * Holds the lazily-created Storage API v2 {@link StorageStub} for one read session and
 * caches the route token returned by the service.
 *
 * <p>Credentials are bridged from the table-api {@link Account} through
 * {@link AccountCredentialProvider}; when the account rotates credentials dynamically the
 * provider delegates to {@link Account#getCredentials()} on every request, so STS refresh
 * keeps working without recreating the client.
 */
public class V2ClientHolder implements AutoCloseable {

    private final TableIdentifier identifier;

    private final EnvironmentSettings settings;

    private volatile StorageStub stub;

    private volatile String routeToken;

    public V2ClientHolder(TableIdentifier identifier, EnvironmentSettings settings) {
        this.identifier = identifier;
        this.settings = settings;
    }

    public EnvironmentSettings settings() {
        return settings;
    }

    public TableIdentifier identifier() {
        return identifier;
    }

    public String routeToken() {
        return routeToken;
    }

    public void updateRouteToken(String token) {
        if (token != null && !token.isEmpty()) {
            this.routeToken = token;
        }
    }

    public synchronized StorageStub stub() {
        if (stub == null) {
            stub = createStub();
        }
        return stub;
    }

    /** Returns an independently owned client when a worker supplies different settings. */
    public V2ClientHolder forDataPlane(EnvironmentSettings workerSettings) {
        if (workerSettings == null || workerSettings == settings) {
            return this;
        }
        V2ClientHolder client = new V2ClientHolder(identifier, workerSettings);
        client.updateRouteToken(routeToken);
        return client;
    }

    @Override
    public synchronized void close() {
        if (stub != null) {
            stub.close();
        }
    }

    private StorageStub createStub() {
        ExecutionEnvironment env = ExecutionEnvironment.create(settings);
        String tunnelEndpoint = env.getTunnelEndpoint(identifier.getProject());

        Account account = settings.getCredentials().getAccount();

        StubSettings.StubSettingsBuilder builder = StubSettings.newBuilder()
                .withCredentialsProvider(new AccountCredentialProvider(account))
                .withEndpoint(env.getServiceEndpoint())
                .withTunnelEndpoint(tunnelEndpoint)
                .withProject(identifier.getProject())
                .withRequestTrace(settings.getRequestTrace())
                .withUserAgent("MaxCompute-TableAPI-v2");
        settings.getQuotaName().ifPresent(builder::withQuota);

        settings.getSharingQuotaToken().ifPresent(builder::withSharingQuotaToken);
        settings.getTags().ifPresent(tags -> builder.withTunnelTags(String.join(",", tags)));
        if (settings.getCredentials().getAppAccount().isPresent()
                || settings.getCredentials().getAppStsAccount().isPresent()) {
            builder.withAdditionalHeaderSigner(headers -> {
                Request request = new Request();
                request.setHeaders(headers);
                settings.getCredentials().getAppAccount().ifPresent(
                        app -> app.getRequestSigner().sign("", request));
                settings.getCredentials().getAppStsAccount().ifPresent(
                        app -> app.getRequestSigner().sign("", request));
            });
        }

        HttpSettings.HttpSettingsBuilder httpSettings = HttpSettings.newBuilder()
                .withConnectTimeout(Duration.ofSeconds(10))
                .withReadTimeout(Duration.ofMinutes(5));
        settings.getRestOptions().ifPresent(options -> {
            options.getConnectTimeout().ifPresent(value ->
                    httpSettings.withConnectTimeout(Duration.ofSeconds(value)));
            options.getReadTimeout().ifPresent(value ->
                    httpSettings.withReadTimeout(Duration.ofSeconds(value)));
            options.getWriteTimeout().ifPresent(value ->
                    httpSettings.withWriteTimeout(Duration.ofSeconds(value)));
            options.isIgnoreCerts().ifPresent(httpSettings::withIgnoreCerts);
            options.getUserAgent().ifPresent(builder::withUserAgent);
            if (options.getRetryTimes().isPresent()
                    || options.getRetryWaitTimeInSeconds().isPresent()) {
                builder.withRetryHandler(new V2RetryHandler(options));
            }
        });
        builder.withHttpSettings(httpSettings.build());

        return new StorageStub(builder.build());
    }
}
