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
  private static final String HOST_DEFAULT = "http://logview.aliyun.com";
  private String logViewHost = "";

  private static final String HOST_DEFAULT_V2 = "https://maxcompute.console.aliyun.com";

  private int version = 1;

  Odps odps;

  public LogView(Odps odps) {
    this(odps, 1);
  }

  public LogView(Odps odps, int version) {
    this.odps = odps;
    this.version = version;
  }

  private String getLogviewHost() {
    if (2 == version) {
      return HOST_DEFAULT_V2;
    }

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

  /**
   * 获取 logview host 地址
   *
   * @return logview host 地址
   */
  public String getLogViewHost() {
    if (StringUtils.isNullOrEmpty(logViewHost)) {
      logViewHost = getLogviewHost();
    }

    return logViewHost;
  }

  /**
   * 设置 logview host 地址
   * @param logViewHost
   *          host 地址
   */
  public void setLogViewHost(String logViewHost) {
    this.logViewHost = logViewHost;
  }

  /**
   * 生成 logview 链接
   *
   * @param instance
   *          instance 对象
   * @param hours
   *           token 有效时间
   * @return  logview
   * @throws OdpsException
   */
  public String generateLogView(Instance instance, long hours) throws OdpsException {
    return generateLogView(instance, hours, null, null);
  }

  /**
   * 生成 session subquery logview 链接
   *
   * @param instance
   *          instance 对象
   * @param queryId
   *           session subquery id
   * @param hours
   *           token 有效时间
   * @return  logview
   * @throws OdpsException
   */
  public String generateSubQueryLogView(Instance instance, int queryId, long hours) throws OdpsException {
    return generateLogView(instance, hours, queryId, null);
  }

  /**
   * 生成 session subquery logview 链接
   *
   * @param instance
   *          instance 对象
   * @param queryId
   *           session subquery id
   * @param token
   *           同一个attach session 可以复用已有的token
   * @return  logview
   */
  public String generateSubQueryLogView(Instance instance, int queryId, String token)
      throws OdpsException {
    return generateLogView(instance, 24, queryId, token);
  }

  private String generateLogView(Instance instance, long hours, Integer queryId, String token)
      throws OdpsException {
    if (StringUtils.isNullOrEmpty(logViewHost)) {
      logViewHost = getLogviewHost();
    }

    if (1 == version) {
      StringBuilder urlBuilder = new StringBuilder(logViewHost);
      urlBuilder.append("/logview/?h=").append(odps.getEndpoint())
          .append("&p=").append(instance.getProject())
          .append("&i=").append(instance.getId());
      if (queryId != null) {
        urlBuilder.append("&subQuery=").append(queryId);
      }
      if (token == null) {
        token = generateInstanceToken(instance, hours);
      }
      urlBuilder.append("&token=").append(token);
      return urlBuilder.toString();
    } else if (2 == version) {
      String url = logViewHost + "/" + odps.projects().get().getRegionId()
             + "/job-insights?h=" + odps.getEndpoint()
             + "&p=" + instance.getProject()
             + "&i=" + instance.getId();
      if (queryId != null) {
        url += "&subQuery=" + queryId;
      }
      return url;
    } else {
      throw new IllegalArgumentException("logview version must be 1 or 2");
    }
  }

  /**
   * 生成带有 instance 访问权限的 token
   *
   * @param instance
   *          instance 对象
   * @param hours
   *           token 有效时间
   * @return  token
   * @throws OdpsException
   */
  public String generateInstanceToken(Instance instance, long hours) throws OdpsException {
    SecurityManager sm = odps.projects().get(instance.getProject()).getSecurityManager();

    return sm.generateAuthorizationToken(generatePolicy(instance, hours), POLICY_TYPE);
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
