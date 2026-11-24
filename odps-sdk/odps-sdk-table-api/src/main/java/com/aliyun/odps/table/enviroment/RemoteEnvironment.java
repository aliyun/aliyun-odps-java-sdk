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
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.transport.TableTransport;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class RemoteEnvironment extends ExecutionEnvironment {

    public RemoteEnvironment(EnvironmentSettings settings) {
        super(settings);
    }

    @Override
    protected void initialize() {
        // Do noting
    }

    @Override
    public String getTunnelEndpoint(String targetProject) {
        return settings.getTunnelEndpoint().orElseGet(() -> {
            try {
                Odps odps = createOdpsClient();
                if (odps.getRestClient().getProxyCredential() != null) {
                    return getTunnelEndpointThroughAuthenticatedProxy(targetProject);
                }
                return odps.projects().get(targetProject)
                        .getTunnelEndpoint(settings.getQuotaName().orElse(null));
            } catch (OdpsException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private String getTunnelEndpointThroughAuthenticatedProxy(String targetProject)
            throws OdpsException {
        Integer writeTimeout = settings.getRestOptions()
                .flatMap(options -> options.getWriteTimeout()).orElse(null);
        RestClient client = new RestClient(new TableTransport(writeTimeout));
        client.setRequestTrace(settings.getRequestTrace());
        client.setEndpoint(getServiceEndpoint());
        client.setAccount(settings.getCredentials().getAccount());
        settings.getCredentials().getAppAccount().ifPresent(client::setAppAccount);
        settings.getCredentials().getAppStsAccount().ifPresent(client::setAppStsAccount);
        setRestOptions(client);

        Map<String, String> params = new HashMap<>();
        params.put("service", null);
        settings.getQuotaName().filter(name -> !name.isEmpty())
                .ifPresent(name -> params.put("quotaName", name));
        Response response = client.request(
                ResourceBuilder.buildProjectResource(targetProject).concat("/tunnel"),
                "GET", params, null, null);
        if (!response.isOK()) {
            throw new OdpsException("Can't get tunnel server address: " + response.getStatus());
        }
        return URI.create(getServiceEndpoint()).getScheme() + "://"
                + new String(response.getBody());
    }
}
