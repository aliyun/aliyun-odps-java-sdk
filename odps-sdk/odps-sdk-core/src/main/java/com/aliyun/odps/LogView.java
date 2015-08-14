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

import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;

public class LogView {

  private static final String POLICY_TYPE = "BEARER";
  private static final String HOST_DEFAULT = "http://logview.odps.aliyun.com";
  private String logViewHost = HOST_DEFAULT;

  Odps odps;

  public LogView(Odps odps) {
    this.odps = odps;
    if (odps.getLogViewHost() != null) {
      logViewHost = odps.getLogViewHost();
    }
  }

  public String getLogViewHost() {
    return logViewHost;
  }

  public void setLogViewHost(String logViewHost) {
    this.logViewHost = logViewHost;
  }

  public String generateLogView(Instance instance, long hours) throws OdpsException {
    if (StringUtils.isNullOrEmpty(logViewHost)) {
      return "";
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
