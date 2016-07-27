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

import java.util.HashMap;

import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;

public class LogView {

  private static final String POLICY_TYPE = "BEARER";
  private static final String HOST_DEFAULT = "http://logview.odps.aliyun.com";
  private String logViewHost = "";

  Odps odps;

  public LogView(Odps odps) {
    this.odps = odps;
  }

  private String getLogviewHost() {
    if (odps.getLogViewHost() != null) {
      return odps.getLogViewHost();
    } else {
      RestClient restClient = odps.clone().getRestClient();
      try {
        String resource = "/logview/host";
        HashMap<String, String> params = new HashMap<String, String>();
        Response resp = restClient.request(resource, "GET", params, null, null);
        String logViewHost = new String(resp.getBody());
        if (StringUtils.isNullOrEmpty(logViewHost)) {
          return HOST_DEFAULT;
        } else
        {
          return logViewHost;
        }
      } catch (Exception e) {
        return HOST_DEFAULT;
      }
    }
  }

  public String getLogViewHost() {
    if (StringUtils.isNullOrEmpty(logViewHost)) {
      logViewHost = getLogviewHost();
    }

    return logViewHost;
  }

  public void setLogViewHost(String logViewHost) {
    this.logViewHost = logViewHost;
  }

  public String generateLogView(Instance instance, long hours) throws OdpsException {
    if (StringUtils.isNullOrEmpty(logViewHost)) {
      logViewHost = getLogviewHost();
    }

    SecurityManager sm = odps.projects().get(instance.getProject()).getSecurityManager();
    String policy = generatePolicy(instance, hours);
    String token = sm.generateAuthorizationToken(policy, POLICY_TYPE);
    String logview = logViewHost + "/logview/?h=" + odps.getEndpoint() + "&p="
                     + instance.getProject() + "&i=" + instance.getId() + "&token=" + token;
    return logview;
  }

  private String generatePolicy(Instance instance, long hours) {
    String policy = "{\n"
                    + "    \"expires_in_hours\": " + String.valueOf(hours) + ",\n"
                    + "    \"policy\": {\n"
                    + "        \"Statement\": [{\n"
                    + "            \"Action\": [\"odps:Read\"],\n"
                    + "            \"Effect\": \"Allow\",\n"
                    + "            \"Resource\": \"acs:odps:*:projects/" + instance.getProject()
                    + "/instances/" + instance.getId() + "\"\n"
                    + "        }],\n"
                    + "        \"Version\": \"1\"\n"
                    + "    }\n"
                    + "}";
    return policy;
  }
}
