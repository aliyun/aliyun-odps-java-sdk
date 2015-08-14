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

package com.aliyun.odps.local.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.utils.StringUtils;

public class TestUtils {

  private static final Properties props = new Properties();

  // projects
  public static String odps_test_mrtask = "odps_test_mrtask";
  public static String odps_test_openmr2 = "odps_test_openmr2";
  public static String pipe = "pipe";
  public static String odps_test_bridgejob = "odps_test_bridgejob";
  public static String bug = "bug";

  static {
    try {
      loadConfig();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Properties loadConfig() throws IOException {

    InputStream is = null;
    try {
      is = TestUtils.class.getClassLoader().getResourceAsStream("test.conf");
      props.load(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }

    return props;
  }

  public static Odps newOdps() {
    String accessId = props.getProperty("default.access.id");
    String accessKey = props.getProperty("default.access.key");
    String endpoint = props.getProperty("default.endpoint");
    String project = props.getProperty("default.project");

    Account account = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(account);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);
    return odps;
  }

  public static TableTunnel newTableTunnel(Odps odps) {
    TableTunnel tunnel = new TableTunnel(odps);
    String tunnelEndpoint = props.getProperty("default.tunnel.endpoint");
    if (!StringUtils.isNullOrEmpty(tunnelEndpoint)) {
      tunnel.setEndpoint(tunnelEndpoint);
    }
    return tunnel;
  }

}
