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
import com.aliyun.odps.options.OdpsOptions;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;

/**
 * JobInsight is the name of new version of Logview.
 * In MaxCompute S54, we will use JobInsight to replace Logview.
 *
 */
public class LogView {

  private static final String POLICY_TYPE = "BEARER";
  private static final String LOGVIEW_HOST_DEFAULT = "http://logview.alibaba-inc.com";
  private String logViewHost = "";

  private static final String JOBINSIGHT_HOST_DEFAULT = "https://maxcompute.console.aliyun.com";
  private String jobInsightHost = "";

  private final int version;

  Odps odps;

  public LogView(Odps odps) {
    this(odps, null);
  }

  public LogView(Odps odps, Integer version) {
    this.odps = odps;
    if (version == null) {
      Boolean useLegacyLogview = odps.options().isUseLegacyLogview();
      if (useLegacyLogview == null) {
        this.jobInsightHost = getJobInsightHost();
        if (StringUtils.isNullOrEmpty(jobInsightHost)) {
          this.version = 1;
        } else {
          this.version = 2;
        }
      } else if (useLegacyLogview) {
        this.version = 1;
      } else {
        this.version = 2;
      }
    } else {
      this.version = version;
    }
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
          return LOGVIEW_HOST_DEFAULT;
        } else
        {
          return logViewHost;
        }
      } catch (Exception e) {
        return LOGVIEW_HOST_DEFAULT;
      }
    }
  }


  private String getJobInsightHost() {
    if (odps.getJobInsightHost() != null) {
      return odps.getJobInsightHost();
    } else {
      RestClient restClient = odps.clone().getRestClient();
      restClient.setConnectTimeout(3);
      restClient.setReadTimeout(10);
      try {
        String resource = "/webconsole/host";
        HashMap<String, String> params = new HashMap<String, String>();
        Response resp = restClient.request(resource, "GET", params, null, null);
        return new String(resp.getBody());
      } catch (Exception e) {
        return null;
      }
    }
  }

  /**
   * 获取 logview host 地址
   *
   * @return logview host 地址
   */
  public String getLogViewHost() {
    if (version == 2) {
      if (StringUtils.isNullOrEmpty(jobInsightHost)) {
        jobInsightHost = getJobInsightHost();
      }
      return jobInsightHost;
    } else {
      if (StringUtils.isNullOrEmpty(logViewHost)) {
        logViewHost = getLogviewHost();
      }
      return logViewHost;
    }
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
   * 设置 logview host 地址
   * @param jobInsightHost
   *          host 地址
   */
  public void setJobInsightHost(String jobInsightHost) {
    this.jobInsightHost = jobInsightHost;
  }

  /**
   * 生成 logview 链接，默认有效时间 24h
   *
   * @param instance
   *          instance 对象
   * @return  logview
   * @throws OdpsException
   */
  public String generateLogView(Instance instance) throws OdpsException {
    return generateLogView(instance, 24, null, null);
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
    if (1 == version) {
      if (StringUtils.isNullOrEmpty(logViewHost)) {
        logViewHost = getLogviewHost();
      }
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
      if (StringUtils.isNullOrEmpty(jobInsightHost)) {
        jobInsightHost = getJobInsightHost();
      }
      String regionId = "cn";
      try {
        regionId = odps.projects().get(instance.getProject()).getRegionId();
      } catch (Exception ignore) {
      }
      String url = jobInsightHost + "/" + regionId
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
