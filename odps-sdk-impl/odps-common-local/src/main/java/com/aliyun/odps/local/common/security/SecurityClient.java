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

package com.aliyun.odps.local.common.security;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Policy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.utils.ReflectionUtils;

public class SecurityClient {
  private static String lineSeperator = System.getProperty("line.separator", "\n");

  private static SecurityClient securityClient;
  private String policyFilePath;

  // user parameters
  private boolean isSecurityEnabled;
  private boolean isJNIEnabled; // is jni enabled
  private String userDefinePolicy;

  private SecurityManager oldSecurityManager;
  private Policy oldPolicy;

  // MR,UDF
  private ApplicatitionType appType;
  private List<String> appCodeBase;
  private Map<String, String> replacement; // replace module in policy file, like
                                           // $TEMP_HOME$,$WAREHOUSE_HOME$

  private SecurityClient() {}

  private static SecurityClient getInstance() {
    if (securityClient == null) {
      synchronized (SecurityClient.class) {
        if (securityClient == null) {
          securityClient = new SecurityClient();
        }
      }
    }
    return securityClient;
  }


  public static void init(ApplicatitionType appType, List<String> appCodeBase,
      Map<String, String> replacement, boolean isSecurityEnabled, boolean isJNIEnabled,
      String userDefinePolicy) {

    getInstance().appType = appType;
    getInstance().appCodeBase = appCodeBase;

    getInstance().isSecurityEnabled = isSecurityEnabled;

    if (!getInstance().isSecurityEnabled) {
      return;
    }

    getInstance().isJNIEnabled = isJNIEnabled;

    if (userDefinePolicy == null || userDefinePolicy.length() < "permission".length()) {
      userDefinePolicy = "";
    }

    //delete useless quotes ( from console set)
    if (userDefinePolicy.startsWith("\"") && userDefinePolicy.endsWith("\"")) {
      userDefinePolicy = userDefinePolicy.substring(1, userDefinePolicy.length() - 1);
    } else if (userDefinePolicy.startsWith("'") && userDefinePolicy.endsWith("'")) {
      userDefinePolicy = userDefinePolicy.substring(1, userDefinePolicy.length() - 1);
    }
    
    userDefinePolicy=userDefinePolicy.trim();

    if (!userDefinePolicy.isEmpty() && !userDefinePolicy.endsWith(";")) {
      userDefinePolicy = userDefinePolicy + ";";
    }

    if (getInstance().isJNIEnabled) {
      userDefinePolicy += "permission java.lang.RuntimePermission \"loadLibrary.*\";";
      try {
        if (WareHouse.getInstance().getOdps() != null
            && WareHouse.getInstance().getOdps().getDefaultProject() != null) {
          addJavaLibPath(WareHouse.getInstance()
              .getResourceDir(WareHouse.getInstance().getOdps().getDefaultProject())
              .getAbsolutePath());
        }
      } catch (IOException e) {
        System.err.println("Add java.library.path failed! " + e.getMessage());
      }
    }

    // access warehouse and temp directory is enabled

    if (WareHouse.getInstance() != null && WareHouse.getInstance().getWarehouseDir() != null) {
      userDefinePolicy +=
          "permission java.io.FilePermission  \""
              + WareHouse.getInstance().getWarehouseDir().getAbsolutePath() + File.separator + "-"
              + "\" , \"read,write,delete\";";
    }
    if (WareHouse.getInstance() != null && WareHouse.getInstance().getJobDirStr() != null) {
      userDefinePolicy +=
          "permission java.io.FilePermission  \"" + WareHouse.getInstance().getJobDirStr()
              + File.separator + "-" + "\" , \"read,write,delete\";";
    }

    getInstance().userDefinePolicy = userDefinePolicy;
    getInstance().replacement = replacement;

    copyPolicyFile();

  }


  private static void copyPolicyFile() {
    String policyFileName;
    switch (getInstance().appType) {
      case MR:
        policyFileName = "mr.policy";
        break;
      case UDF:
        policyFileName = "udf.policy";
        break;
      default:
        policyFileName = "mr.policy";
        break;
    }


    StringBuffer sb = new StringBuffer();
    BufferedReader br = null;
    try {
      br =
          new BufferedReader(new InputStreamReader(SecurityClient.class.getClassLoader()
              .getResourceAsStream(policyFileName)));
      String line;
      while ((line = br.readLine()) != null) {
        if (sb.length() > 0) {
          sb.append(lineSeperator);
        }
        sb.append(line);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
      return;
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

    }

    String policyContent = handlePolicyTemplet(sb.toString());

    String tempPolicyFilePath = null;
    try {
      tempPolicyFilePath =
          WareHouse.getInstance().getJobDir().getJobDir() + File.separator + "temp."
              + policyFileName;
    } catch (Exception exception) {
    }

    // some application has no warehouse,ie. udf
    if (tempPolicyFilePath == null) {
      tempPolicyFilePath = "temp." + policyFileName;
    }
    PrintWriter pw = null;
    try {
      pw = new PrintWriter(tempPolicyFilePath);
      pw.write(policyContent);
      pw.flush();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } finally {
      if (pw != null) {
        pw.close();
      }
    }

    getInstance().policyFilePath = tempPolicyFilePath;

  }

  private static String handlePolicyTemplet(String policyTemplet) {
    // replace module in policy file, like
    // $TEMP_HOME$,$WAREHOUSE_HOME$
    if (getInstance().replacement != null) {
      for (String key : getInstance().replacement.keySet()) {
        policyTemplet = policyTemplet.replaceAll(key, getInstance().replacement.get(key));
      }
    }

    policyTemplet = policyTemplet.replace("$USER_DEFINE$", getInstance().userDefinePolicy);


    // replace $FRAMEWORK_GRANT$
    visitedPath.clear();
    StringBuffer frameWorkLibGrant = new StringBuffer();

    // odps-sdk-common
    String frameWorkLibPath =
        ReflectionUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    frameWorkLibGrant.append(grantAllPermission(frameWorkLibPath));

    // odps-sdk-core
    frameWorkLibPath = Odps.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    frameWorkLibGrant.append(lineSeperator);
    frameWorkLibGrant.append(grantAllPermission(frameWorkLibPath));

    // odps-common-local
    frameWorkLibPath =
        SecurityClient.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    frameWorkLibGrant.append(lineSeperator);
    frameWorkLibGrant.append(grantAllPermission(frameWorkLibPath));

    // module lib path: odps-mapred-local,odps-graph-local,odps-udf-local
    if (getInstance().appCodeBase != null) {
      for (String path : getInstance().appCodeBase) {
        frameWorkLibPath = path;
        frameWorkLibGrant.append(lineSeperator);
        frameWorkLibGrant.append(grantAllPermission(frameWorkLibPath));
      }
    }



    policyTemplet = policyTemplet.replace("$FRAMEWORK_GRANT$", frameWorkLibGrant);

    return policyTemplet;

  }

  private static Map<String, Object> visitedPath = new HashMap<String, Object>();
  private static Object obj = new Object();

  private static String grantAllPermission(String path) {
    if (visitedPath.containsKey(path)) {
      return "";
    }
    StringBuffer sb = new StringBuffer();
    if (path.endsWith(".jar")) {
      // add jar file, for maven project
      sb.append("grant codeBase \"file:");
      sb.append(path + "\"{");
      sb.append(lineSeperator);
      sb.append("permission java.security.AllPermission;");
      sb.append(lineSeperator);
      sb.append("};");
      sb.append(lineSeperator);
      visitedPath.put(path, obj);

      return sb.toString();

      // add parent directory
      // path = path.substring(0, path.lastIndexOf(File.separator) + 1);
    }

    path = path.endsWith(File.separator) ? path : path + File.separator;

    if (visitedPath.containsKey(path)) {
      return sb.toString();
    }

    sb.append("grant codeBase \"file:");
    sb.append(path + "-\"{");
    sb.append(lineSeperator);
    sb.append("permission java.security.AllPermission;");
    sb.append(lineSeperator);
    sb.append("};");
    sb.append(lineSeperator);

    visitedPath.put(path, obj);

    return sb.toString();
  }


  public static boolean isSecurityEnabled() {
    return getInstance().isSecurityEnabled;
  }

  public static boolean isJNIEnabled() {
    return getInstance().isJNIEnabled;
  }

  public static void open() {
    if (getInstance().isSecurityEnabled) {

      getInstance().oldPolicy = Policy.getPolicy();
      getInstance().oldSecurityManager = System.getSecurityManager();

      try {
        Class policyFileClass = Class.forName("sun.security.provider.PolicyFile");
        Constructor constructor = policyFileClass.getConstructor(URL.class);
        Policy policy =
            (Policy) constructor.newInstance(new URL("file:" + getInstance().policyFilePath));
        Policy.setPolicy(policy);
        Policy.getPolicy().refresh();
        System.setSecurityManager(new DefaultSecurityManager());
      } catch (Exception e) {
        System.err.println("Your JVM not support local sandbox figture! " + e.getMessage());
      }

    }
  }

  public static void close() {
    if (getInstance().isSecurityEnabled) {
      System.setSecurityManager(getInstance().oldSecurityManager);
      Policy.setPolicy(getInstance().oldPolicy);
      Policy.getPolicy().refresh();
    }
  }

  private static void addJavaLibPath(String path) throws IOException {
    try {
      Field field = ClassLoader.class.getDeclaredField("usr_paths");
      field.setAccessible(true);
      String[] oldPaths = (String[]) field.get(null);
      for (int i = 0; i < oldPaths.length; i++) {
        if (path.trim().equals(oldPaths[i])) {
          return;
        }
      }
      String[] newPaths = new String[oldPaths.length + 1];
      System.arraycopy(oldPaths, 0, newPaths, 0, oldPaths.length);
      newPaths[oldPaths.length] = path;
      field.set(null, newPaths);
    } catch (IllegalAccessException e) {
      throw new IOException(e.getMessage());
    } catch (NoSuchFieldException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * restore environment
   */
  public static void clear() {
    System.clearProperty(Constants.LOCAL_SECURITY_JNI_ENABLE);
    System.clearProperty(Constants.LOCAL_USER_DEFINE_POLICY);
    System.clearProperty(Constants.LOCAL_SECURITY_ENABLE);
  }

  public static void setIsSecurityEnabled(boolean flag) {
    System.setProperty(Constants.LOCAL_SECURITY_ENABLE, flag ? "true" : "false");
  }

  public static void setIsJNIEnabled(boolean flag) {
    System.setProperty(Constants.LOCAL_SECURITY_ENABLE, flag ? "true" : "false");
  }

  public static void setUserDefinePolicy(String userDefinePolicy) {
    if (userDefinePolicy != null) {
      System.setProperty(Constants.LOCAL_USER_DEFINE_POLICY, userDefinePolicy);
    } else {
      System.clearProperty(Constants.LOCAL_USER_DEFINE_POLICY);
    }
  }
}
