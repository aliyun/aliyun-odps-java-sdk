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

package com.aliyun.odps.commons.util;

import java.io.InputStream;
import java.util.Properties;

public class SvnRevisionUtils {

  public static String revision = "UNKNOWN";
  public static String mavenVersion = "UNKNOWN";
  public static String javaVersion = "UNKNOWN";

  static {
    InputStream is = null;
    try {
      is = SvnRevisionUtils.class.getResourceAsStream("/com/aliyun/odps/core/version.txt");
      Properties properties = new Properties();
      properties.load(is);
      revision = properties.getProperty("Revision");
      mavenVersion = properties.getProperty("MavenVersion");
      javaVersion = System.getProperty("java.version");
    } catch (Exception e) {
    }
  }

  public static String getSvnRevision() {
    return revision;
  }

  public static String getMavenVersion() {
    return mavenVersion;
  }

  public static String getJavaVersion() {
    return javaVersion;
  }
}
