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

package com.aliyun.odps.mapred.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import com.alibaba.fastjson.JSON;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsHook;
import com.aliyun.odps.OdpsHooks;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.Account.AccountProvider;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.utils.StringUtils;

/**
 * SessionState指定了Odps MapReduce任务的会话上下文。
 *
 * SessionState保存的信息包括：
 * <p>
 * <li>{@link com.aliyun.odps.Odps}实例</li>
 * <li>默认的{@link com.aliyun.odps.mapred.conf.JobConf}</li>
 * <li>local run 开关</li>
 * </p>
 *
 * 以下代码示例如何设置当前运行模式为local run：
 *
 * <pre>
 * SessionState ss = SessionState.get();
 * ss.setLocalRun(true);
 * </pre>
 */
public class SessionState {

  private static ThreadLocal<SessionState> tss = new InheritableThreadLocal<SessionState>() {
    @Override
    public SessionState initialValue() {
      return new SessionState();
    }

    @Override
    protected SessionState childValue(SessionState parentValue) {
      return new SessionState(parentValue);
    }
  };

  private Odps odps;
  private JobConf defaultJob;
  private boolean isLocalRun;
  private boolean isCostMode;
  private Map<String, String> aliases;
  // if use internal cli
  private boolean internalCli = false;
  private String commandText = "";

  private SessionState() {
    try {
      aliases = new HashMap<String, String>();
      defaultJob = new JobConf(false);
      String conf = System.getProperties().getProperty(MR_JOB_CONF);
      if (conf != null && !conf.isEmpty()) {
        try {
          defaultJob.addResource(new FileInputStream(new File(conf)));
        } catch (FileNotFoundException e) {
          System.err.print("conf file " + conf + " not found, ignored!");
        }
      }
      parseOldCli(defaultJob); // Try to be compatible with old console
    } catch (AccessControlException e) {
      // just ignore it for it maybe called by user code indirectly through JobConf constructors 
    }
  }

  public SessionState(SessionState ss) {
    if (ss.odps != null) {
      odps = new Odps(ss.odps);
    }
    if (ss.defaultJob != null) {
      defaultJob = new JobConf(ss.defaultJob);
    }
    isLocalRun = ss.isLocalRun;
    aliases = new HashMap<String, String>();
    if (ss.aliases != null) {
      aliases.putAll(ss.aliases);
    }
  }

  // Copied from old OdpsConf.
  private final static String OLD_PROJNAME_KEY = "odps.project.name";
  private final static String OLD_ENDPOINT_KEY = "odps.end.point";
  private final static String OLD_ACCESSID_KEY = "odps.access.id";
  private final static String OLD_ACCESSKEY_KEY = "odps.access.key";
  private final static String OLD_RUNMODE_KEY = "odps.runner.mode";
  private final static String OLD_RESOURCE_KEY = "odps.cache.resources";
  private final static String OLD_CP_RESOURCE_KEY = "odps.classpath.resources";
  private final static String OLD_CONTEXT_FILE = "odps.exec.context.file";

  private final static String OLD_ACCOUNT_PROVIDER_KEY = "odps.account.provider";
  private final static String OLD_TAOBAO_TOKEN_KEY = "odps.taobao.token";
  private final static String OLD_TAOBAO_ALGORITHM_KEY = "odps.taobao.algorithm";

  // Local MR
  private final static String LOCAL_TEMP_DIR = "odps.mapred.local.temp.dir";
  private final static String LOCAL_TEMP_RETAIN = "odps.mapred.local.temp.retain";
  private final static String LOCAL_RECORD_LIMIT = "odps.mapred.local.record.download.limit";
  public static final String LOCAL_DOWNLOAD_MODE = "odps.mapred.local.download.mode"; //always|auto|never; default auto
  //Local security
  public static final String LOCAL_SECURITY_ENABLE = "odps.local.security.enable";
  public static final String LOCAL_SECURITY_JNI_ENABLE = "odps.local.security.jni.enable";
  public static final String LOCAL_USER_DEFINE_POLICY = "odps.local.user.define.policy";

  public static final String COST = "cost";
  public static final String ODPS_JOB_COST_ESTIMATE = "odps.task.cost.estimate";

  private final static String
      LOCAL_INPUT_COLUMN_SEPERATOR =
      "odps.mapred.local.input.column.seperator";
  private final static String
      LOCAL_OUTPUT_COLUMN_SEPERATOR =
      "odps.mapred.local.output.column.seperator";

  private static final String MR_JOB_CONF = "odps.mr.job.conf";

  /**
   * From old Console load context file , include settings & aliases
   *
   * @return
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void loadContextFile(Properties prop) throws IOException {
    String fileName = prop.getProperty(OLD_CONTEXT_FILE);
    if (fileName == null) {
      return;
    }

    String jsonStr = FileUtils.readFileToString(new File(fileName));
    Map context = JSON.parseObject(jsonStr, Map.class);
    if (context == null) {
      return;
    }
    // client set ignore certs to true when request from internal-cli
    internalCli = true;
    getOdps().getRestClient().setIgnoreCerts(true);
    if (context.containsKey("settings")) {
      Map<String, String> settings = (Map<String, String>) context.get("settings");
      for (Entry<String, String> setting : settings.entrySet()) {
        defaultJob.set(setting.getKey(), setting.getValue());
      }
    }

    if (context.containsKey("aliases")) {
      aliases = (Map<String, String>) context.get("aliases");
    }

    if (context.containsKey("context")) {
      // extract info from execution context
      Map<String, Object> ctx = (Map<String, Object>) context.get("context");
      // for compatibility
      Object priority = ctx.get("priority");
      if (priority instanceof Integer) {
        defaultJob.setInstancePriority((Integer) priority);
      } else {
        defaultJob.setInstancePriority(Integer.parseInt((String) priority));
      }
      OdpsHooks.clearRegisteredHooks();
      String hookString = (String) ctx.get("odpsHooks");
      if (!StringUtils.isNullOrEmpty(hookString)) {
        try {
          String[] hooks = hookString.split(",");
          List<Class<? extends OdpsHook>> hookList = new ArrayList<Class<? extends OdpsHook>>();
          for (String hook : hooks) {
            hookList.add((Class<? extends OdpsHook>) Class.forName(hook));
          }
          OdpsHooks.registerHooks(hookList);

        } catch (ClassNotFoundException e) {
          throw new IOException(e.getMessage(), e);
        }
      }

      String runningCluster = (String) ctx.get("runningCluster");
      if (!StringUtils.isNullOrEmpty(runningCluster)) {
        odps.instances().setDefaultRunningCluster(runningCluster);
      }

      if (ctx.get("logViewHost") != null) {
        String logViewHost = (String) ctx.get("logViewHost");
        odps.setLogViewHost(logViewHost);
      }

      if (ctx.containsKey("https_check")) {
        odps.getRestClient().setIgnoreCerts(!(Boolean) ctx.get("https_check"));
      }

    }

    if (context.containsKey("commandText")) {
      setCommandText((String) context.get("commandText"));
    }
  }

  /**
   * Parse old command line settings and set to new API accordingly.
   */
  private void parseOldCli(JobConf conf) {
    Properties prop = System.getProperties();

    //check for cost mode
    String cost = prop.getProperty(COST);
    if (cost != null && "true".equals(cost)) {
      defaultJob.setBoolean(ODPS_JOB_COST_ESTIMATE, true);
      setCostMode(true);
    }

    if (prop.getProperty(OLD_ENDPOINT_KEY) != null) {
      String endpoint = prop.getProperty(OLD_ENDPOINT_KEY);
      String project = prop.getProperty(OLD_PROJNAME_KEY);
      String accessId = prop.getProperty(OLD_ACCESSID_KEY);
      String accessKey = prop.getProperty(OLD_ACCESSKEY_KEY);
      String runmode = prop.getProperty(OLD_RUNMODE_KEY, "remote");

      if (runmode.equalsIgnoreCase("local")) {
        handleLocalMR(prop);
      } else {
        System.err.println("Running job in console.");
      }

      AccountProvider accountProvider = AccountProvider.ALIYUN;
      String apStr = prop.getProperty(OLD_ACCOUNT_PROVIDER_KEY);
      if (apStr != null) {
        apStr = apStr.trim().toUpperCase();
        try {
          accountProvider = AccountProvider.valueOf(apStr);
        } catch (Exception exception) {
          throw new RuntimeException("Unsupport account provider:" + apStr);
        }
      }

      Account account = null;
      switch (accountProvider) {
        case ALIYUN:
          account = new AliyunAccount(accessId, accessKey);
          break;

        default:
          throw new RuntimeException("unsupport account provider:" + accountProvider);
      }
      Odps odps = new Odps(account);
      odps.setDefaultProject(project);
      odps.setEndpoint(endpoint);
      setOdps(odps);
      setLocalRun(runmode.equalsIgnoreCase("local"));

      String resources = prop.getProperty(OLD_RESOURCE_KEY, "");
      String cpresources = prop.getProperty(OLD_CP_RESOURCE_KEY, "");
      if (!resources.isEmpty() && !cpresources.isEmpty()) {
        resources += ",";
      }
      resources += cpresources;
      conf.setResources(resources);
    }

    try {
      loadContextFile(prop);
    } catch (IOException e) {
      // Silently swallow it because adapter is not required to load context
      // file.
    }
  }

  /**
   * 获取当前线程的SessionState
   *
   * @return 当前线程的SessionState
   */
  public static SessionState get() {
    return tss.get();
  }

  /**
   * 获取{@link com.aliyun.odps.Odps}实例
   *
   * @return {@link com.aliyun.odps.Odps}实例
   */
  public Odps getOdps() {
    return odps;
  }

  /**
   * 设置{@link com.aliyun.odps.Odps}实例
   *
   * @param odps
   *     {@link com.aliyun.odps.Odps}实例
   */
  public void setOdps(Odps odps) {
    this.odps = odps;
    if (internalCli) {
      odps.getRestClient().setIgnoreCerts(true);
    }
  }

  /**
   * 设置默认的默认的{@link com.aliyun.odps.mapred.conf.JobConf}。默认的
   * {@link com.aliyun.odps.mapred.conf.JobConf}的默认构方法会载入这个默认配置。
   *
   * @return 默认的默认的{@link com.aliyun.odps.mapred.conf.JobConf}
   */
  public JobConf getDefaultJob() {
    return defaultJob;
  }

  /**
   * 返回local run模式
   *
   * @return true如果是local run模式
   */
  public boolean isLocalRun() {
    return isLocalRun;
  }

  /**
   * 设置local run模式
   *
   * @param b
   *     true如果需要local run
   */
  public void setLocalRun(boolean b) {
    isLocalRun = b;
  }

  /**
   * 设置是否是计费预估模式
   *
   * @param b
   *     true如果是计费预估模式
   */
  public void setCostMode(boolean b) {
    isCostMode = b;
  }

  /**
   * 返回是否是计费预估模式
   *
   * @return true如果是计费预估模式
   */
  public boolean isCostMode() {
    return isCostMode;
  }

  /**
   * 设置默认的默认的{@link com.aliyun.odps.mapred.conf.JobConf}
   *
   * @param defaultJob
   */
  public void setDefaultJob(JobConf defaultJob) {
    this.defaultJob = defaultJob;
  }

  /**
   * 设置Alias
   *
   * @param aliases
   */
  public void setAliases(Map<String, String> aliases) {
    this.aliases = aliases;
  }

  /**
   * 返回Alias
   */
  public Map<String, String> getAliases() {
    return aliases;
  }

  private void handleLocalMR(Properties prop) {
    String tempDir = prop.getProperty(LOCAL_TEMP_DIR);
    if (tempDir != null && !tempDir.isEmpty()) {
      defaultJob.set(LOCAL_TEMP_DIR, tempDir);
    }

    String tempRetain = prop.getProperty(LOCAL_TEMP_RETAIN);
    if (tempRetain != null && tempRetain.trim().equalsIgnoreCase("false")) {
      defaultJob.setBoolean(LOCAL_TEMP_RETAIN, false);
    } else {
      defaultJob.setBoolean(LOCAL_TEMP_RETAIN, true);
    }

    String limitDownloadCount = prop.getProperty(LOCAL_RECORD_LIMIT);
    if (limitDownloadCount != null && !limitDownloadCount.isEmpty()) {
      try {
        int limit = Integer.parseInt(limitDownloadCount);
        if (limit > 0) {
          defaultJob.setInt(LOCAL_RECORD_LIMIT, limit);
        }
      } catch (Exception e) {
      }
    }

    String downloadMode = prop.getProperty(LOCAL_DOWNLOAD_MODE, "AUTO");
    defaultJob.set(LOCAL_DOWNLOAD_MODE, downloadMode);

    String inputColumnSeperator = prop.getProperty(LOCAL_INPUT_COLUMN_SEPERATOR);
    if (inputColumnSeperator != null && !inputColumnSeperator.isEmpty()) {
      defaultJob.set(LOCAL_INPUT_COLUMN_SEPERATOR, inputColumnSeperator);
    }

    String outputColumnSeperator = prop.getProperty(LOCAL_OUTPUT_COLUMN_SEPERATOR);
    if (outputColumnSeperator != null && !outputColumnSeperator.isEmpty()) {
      defaultJob.set(LOCAL_OUTPUT_COLUMN_SEPERATOR, outputColumnSeperator);
    }

    boolean isSecurityEnabled =
        prop.getProperty(LOCAL_SECURITY_ENABLE, "false").equalsIgnoreCase("true");
    if (isSecurityEnabled) {
      boolean isJNIEnabled =
          prop.getProperty(LOCAL_SECURITY_JNI_ENABLE, "false").equalsIgnoreCase("true");
      String userDefinePolicy = prop.getProperty(LOCAL_USER_DEFINE_POLICY, "");
      defaultJob.setBoolean(LOCAL_SECURITY_ENABLE, isSecurityEnabled);
      defaultJob.setBoolean(LOCAL_SECURITY_JNI_ENABLE, isJNIEnabled);
      defaultJob.set(LOCAL_USER_DEFINE_POLICY, userDefinePolicy);
    }

  }

  public String getCommandText() {
    return commandText;
  }

  public void setCommandText(String commandText) {
    this.commandText = stripNonValidXMLCharacters(commandText);
  }

  /**
   * This method ensures that the output String has only valid XML unicode characters as specified
   * by the XML 1.0 standard. For reference, please see <a
   * href="http://www.w3.org/TR/2000/REC-xml-20001006#NT-Char">the standard</a>. This method will
   * return an empty String if the input is null or empty.
   * 
   * @param in The String whose non-valid characters we want to remove.
   * @return The in String, stripped of non-valid characters.
   */
  private String stripNonValidXMLCharacters(String in) {
    StringBuilder out = new StringBuilder();
    char current;
    if (in == null || ("".equals(in)))
      return "";
    for (int i = 0; i < in.length(); i++) {
      current = in.charAt(i);
      if ((current == 0x9) || (current == 0xA) || (current == 0xD)
          || ((current >= 0x20) && (current <= 0xD7FF))
          || ((current >= 0xE000) && (current <= 0xFFFD)))
        out.append(current);
    }
    return out.toString();
  }
}
