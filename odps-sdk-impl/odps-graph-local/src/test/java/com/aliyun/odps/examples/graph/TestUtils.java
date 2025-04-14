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

package com.aliyun.odps.examples.graph;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.ApsaraAccount;
import com.aliyun.odps.mapred.conf.SessionState;

public class TestUtils {

  // projects
  public static String odps_test_mrtask = "odps_test_mrtask";
  public static String odps_test_openmr2 = "odps_test_openmr2";
  public static String pipe = "pipe";
  public static String odps_test_bridgejob = "odps_test_bridgejob";
  public static String bug = "bug";
  public static String yzy2 = "yzy2";
  
  private static final Properties props = new Properties();

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

  public static void setEnvironment(String projectName) {
    String accessId = props.getProperty("default.access.id");
    String accessKey = props.getProperty("default.access.key");
    String endpoint = "";
    if (projectName.equals(odps_test_bridgejob)) {
      endpoint = props.getProperty("default.endpoint");
    } else if (projectName.equals(odps_test_openmr2)) {
      endpoint = props.getProperty("default.endpoint");
    } else if (projectName.equals(pipe)) {
      endpoint = props.getProperty("default.endpoint");
    } else if (projectName.equals(odps_test_mrtask)) {
      endpoint = props.getProperty("default.endpoint");
    } else {
      endpoint = "xxx";
    }

    Account account = new ApsaraAccount(accessId, accessKey);
    Odps odps = new Odps(account);
    odps.setDefaultProject(projectName);
    odps.setEndpoint(endpoint);

    SessionState.get().setOdps(odps);
    SessionState.get().setLocalRun(true);
  }

  public static Odps newOdps() {
    String accessId = props.getProperty("default.access.id");
    String accessKey = props.getProperty("default.access.key");
    String endpoint = props.getProperty("default.endpoint");
    String project = "odps_test_bridgejob";
    Account account = new ApsaraAccount(accessId, accessKey);
    Odps odps = new Odps(account);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);
    return odps;
  }

  public static String getInputTableName(Object object) {
    String result = object.getClass().getName();
    result = result.substring(result.lastIndexOf(".") + 1);
    return result.toLowerCase() + "_in";
  }

  public static String getOutputTableName(Object object) {
    String result = object.getClass().getName();
    result = result.substring(result.lastIndexOf(".") + 1);
    return result.toLowerCase() + "_out";
  }

}
