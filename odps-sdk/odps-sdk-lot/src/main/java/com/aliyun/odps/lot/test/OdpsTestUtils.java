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

package com.aliyun.odps.lot.test;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.ApsaraAccount;

public class OdpsTestUtils {

  private static final Properties props = new Properties();

  static {
    try {
      loadConfig();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 从CLASSPATH加载test.properties
   *
   * @return
   * @throws IOException
   */
  public static Properties loadConfig() throws IOException {

    InputStream is = null;
    try {
      is = OdpsTestUtils.class.getClassLoader().getResourceAsStream("test.conf");
      props.load(is);

    } finally {
      if (is != null) {
        is.close();
      }
    }

    return props;
  }

  /**
   * 根据test.properties的设置创建一个新的Odps对象
   *
   * @return
   */
  public static Odps newDefaultOdps() {
    Odps odps = null;

    String accessId = props.getProperty("default.access.id");
    String accessKey = props.getProperty("default.access.key");
    String endpoint = props.getProperty("default.endpoint");
    String project = props.getProperty("default.project");

    Account account = new ApsaraAccount(accessId, accessKey);
    odps = new Odps(account);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);

    return odps;
  }

  private static SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");

  public static synchronized String currentTime() {
    return df.format(new Date());
  }

  public static String getProperty(String name) {
    return props.getProperty(name);
  }

  public static void cleanup() {
  }

  /**
   * 将属性文件中的字符串转化为相应的布尔值
   *
   * @param value
   * @return 当输入为：空字符串、“0”、“false"时返回false，否则返回true
   */

  public static boolean checkBoolean(String value) {
    if (value == null) {
      return false;
    }
    return Boolean.parseBoolean(value.trim());
  }

  public static Odps newHttpsOdps() {
    Odps odps = newDefaultOdps();
    odps.setEndpoint(props.getProperty("https.endpoint"));
    return odps;
  }

}
