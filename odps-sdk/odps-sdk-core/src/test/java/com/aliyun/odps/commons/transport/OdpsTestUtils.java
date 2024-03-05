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

package com.aliyun.odps.commons.transport;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.AppAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.VolumeTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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

  public static String getSchemaProject1() {
    return props.getProperty("schema.project.1");
  }

  public static String getSchemaProject2() {
    return props.getProperty("schema.project.2");
  }

  public static String getDefaultOwner() {
    return props.getProperty("default.project.owner");
  }

  public static String getGrantUser() {
    return props.getProperty("grant.user");
  }

  public static String getRamUser() {
    return props.getProperty("test.ram.user");
  }

  public static String getRamRole() {
    return props.getProperty("test.ram.role");
  }

  public static String getMultiIDUser() {
    return props.getProperty("test.multi.id.user");
  }

  public static String getMultiIDNickname() {
    return props.getProperty("test.multi.id.nickname");
  }

  public static String getMultiIDKp() {
    return props.getProperty("test.muiti.id.kp");
  }

  public static String getNickerNickname() {
    return props.getProperty("test.nicker.nickname");
  }

  public static String getCurrentUser(Odps odps) throws OdpsException {
    String userDetail =
        odps.projects().get().getSecurityManager().runQuery("whoami", true);
    JsonObject jsonObject = new JsonParser().parse(userDetail).getAsJsonObject();
    return jsonObject
        .has("DisplayName") ? jsonObject.get("DisplayName").getAsString() : null;
  }

  /**
   * 根据test.properties的grant设置创建一个新的Odps对象
   *
   * @return
   */
  public static Odps newGrantOdps(String project) {
    Odps odps = null;

    String accessId = props.getProperty("grant.access.id");
    String accessKey = props.getProperty("grant.access.key");
    String endpoint = props.getProperty("default.endpoint");

    Account account = new AliyunAccount(accessId, accessKey);
    odps = new Odps(account);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);

    return odps;
  }

  /**
   * 根据test.properties的设置创建一个新的Odps对象
   *
   * @return
   */
  public static Odps newSecurityOdps() {
    Odps odps = null;

    String accessId = props.getProperty("security.access.id");
    String accessKey = props.getProperty("security.access.key");
    String endpoint = props.getProperty("security.endpoint");
    String project = props.getProperty("security.project");

    Account account = new AliyunAccount(accessId, accessKey);
    odps = new Odps(account);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);

    return odps;
  }

  /**
   * 根据test.properties的设置创建一个新的Odps对象
   *
   * @return
   */
  public static Odps newDefaultOdps() {
    Odps odps;

    String accessId = props.getProperty("default.access.id");
    String accessKey = props.getProperty("default.access.key");
    String endpoint = props.getProperty("default.endpoint");
    String project = props.getProperty("default.project");
    String appAccessId = props.getProperty("default.app.access.id");
    String appAccessKey = props.getProperty("default.app.access.key");
    Account account = new AliyunAccount(accessId, accessKey);
    if (appAccessId != null && appAccessKey != null) {
      AppAccount appAccount = new AppAccount(new AliyunAccount(appAccessId, appAccessKey));
      odps = new Odps(account, appAccount);
    } else {
      odps = new Odps(account);
    }
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);
    odps.getRestClient().setRetryTimes(0);

    return odps;
  }

  /**
   * 根据test.properties的设置创建一个新的Odps对象
   *
   * @return
   */
  public static Odps newStorageTierOdps() {
    Odps odps = newDefaultOdps();
    odps.setDefaultProject(props.getProperty("storagetier.project"));
    return odps;
  }

  public static Odps newSchemaOdps() {
    Odps odps;

    String accessId = props.getProperty("default.access.id");
    String accessKey = props.getProperty("default.access.key");
    String endpoint = props.getProperty("default.endpoint");
    String project = getSchemaProject1();
    String appAccessId = props.getProperty("default.app.access.id");
    String appAccessKey = props.getProperty("default.app.access.key");
    Account account = new AliyunAccount(accessId, accessKey);
    if (appAccessId != null && appAccessKey != null) {
      AppAccount appAccount = new AppAccount(new AliyunAccount(appAccessId, appAccessKey));
      odps = new Odps(account, appAccount);
    } else {
      odps = new Odps(account);
    }
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);
    odps.getRestClient().setRetryTimes(0);

    return odps;
  }

  /**
   * 根据test.properties的设置创建一个新的Odps对象
   * 运营商账号
   *
   * @return
   */
  public static Odps newOperatorAccountOdps() {
    Odps odps = null;

    String accessId = props.getProperty("user.operator.access.id");
    String accessKey = props.getProperty("user.operator.access.key");
    String endpoint = props.getProperty("default.endpoint");
    String project = props.getProperty("default.project");

    Account account = new AliyunAccount(accessId, accessKey);
    odps = new Odps(account);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);

    return odps;
  }

  /**
   * 根据 odps 对象创建 TableTunnel 对象，并根据 test.conf 设置 tunnel endpoint
   * @return
   */
  public static TableTunnel newTableTunnel(Odps odps) {
    TableTunnel tunnel = new TableTunnel(odps);
    String tunnelEndpoint = props.getProperty("default.tunnel.endpoint");
    if (!StringUtils.isNullOrEmpty(tunnelEndpoint)) {
      tunnel.setEndpoint(tunnelEndpoint);
    }
    return tunnel;
  }

  public static InstanceTunnel newInstanceTunnel(Odps odps) {
    InstanceTunnel tunnel = new InstanceTunnel(odps);
    String tunnelEndpoint = props.getProperty("default.tunnel.endpoint");
    if (!StringUtils.isNullOrEmpty(tunnelEndpoint)) {
      tunnel.setEndpoint(tunnelEndpoint);
    }
    return tunnel;
  }

  public static RestClient newRestClient() {
    String accessId = props.getProperty("default.access.id");
    String accessKey = props.getProperty("default.access.key");
    String endpoint = props.getProperty("default.endpoint");

    Account account = new AliyunAccount(accessId, accessKey);
    RestClient client = new RestClient(new DefaultTransport());
    client.setAccount(account);
    client.setUserAgent("Mock Rest Client (test only)");
    client.setEndpoint(endpoint);
    return client;
  }

  private static SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");

  public static synchronized String currentTime() {
    return df.format(new Date());
  }

  public static enum Env {
    _daily,
    _test_trunk,
    _test_release,
    _5ktest
  }

  public static Env getEnv() {
    return Env.valueOf("_" + getProperty("LABEL").replace("-", "_"));
  }

  public static String getProperty(String name) {
    return props.getProperty(name);
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
  
  public static void createEmptyTableWithEmptyPartitionForTest(String tableName, String partValue)
      throws TunnelException, OdpsException, IOException {
    createEmptyTableWithEmptyPartitionForTest(newDefaultOdps(), tableName, partValue);
  }
 
  public static void createEmptyTableWithEmptyPartitionForTest(Odps odps,String tableName, String partValue) throws TunnelException, OdpsException,                                                              IOException {
   if (!odps.tables().exists(tableName)) {
     TableSchema schema = new TableSchema();
     schema.addColumn(new Column("c1", OdpsType.BIGINT));
     schema.addPartitionColumn(new Column("p1", OdpsType.STRING));
     odps.tables().create(tableName, schema);
     odps.tables().get(tableName).createPartition(new PartitionSpec("p1='"+partValue+"'"));    
   }
  }
  
  public static void createTableForTest(String tableName) throws TunnelException, OdpsException,
      IOException {
    createTableForTest(newDefaultOdps(), tableName);
  }

  public static void createTableForTest(
      Odps odps,
      String tableName) throws OdpsException, IOException {

    if (!odps.tables().exists(tableName)) {
      TableSchema schema = new TableSchema();
      schema.addColumn(new Column("c1", OdpsType.BIGINT));
      odps.tables().create(tableName, schema);

      TableTunnel tunnel = newTableTunnel(odps);
      TableTunnel.UploadSession session = tunnel.createUploadSession(odps.getDefaultProject(),
                                                                     tableName);
      TunnelRecordWriter rw = (TunnelRecordWriter) session.openRecordWriter(0L);
      Record record;
      for (int i = 0; i < 20; ++i) {
        record = session.newRecord();
        record.set(0, 1L);
        rw.write(record);
      }
      record = session.newRecord();
      record.set(0, 0L);
      rw.write(record);
      rw.close();
      Long[] blocks = {0L};
      session.commit(blocks);
    }
  }
  
  public static void createBigTableForTest(String tableName) throws OdpsException, IOException {
    createBigTableForTest(newDefaultOdps(), tableName);
  }


  public static void createBigTableForTest(Odps odps, String tableName) throws OdpsException, IOException {
    if (!odps.tables().exists(tableName)) {
      TableSchema schema = new TableSchema();
      schema.addColumn(new Column("c1", OdpsType.BIGINT));
      odps.tables().create(tableName, schema);

      TableTunnel tunnel = newTableTunnel(odps);
      TableTunnel.UploadSession session = tunnel.createUploadSession(odps.getDefaultProject(),
                                                                     tableName);
      TunnelRecordWriter rw = (TunnelRecordWriter) session.openRecordWriter(0L);
      Record record;
      for (int i = 0; i < 10009; ++i) {
        record = session.newRecord();
        record.set(0, Long.valueOf(i));
        rw.write(record);
      }
      record = session.newRecord();
      record.set(0, 10009L);
      rw.write(record);
      rw.close();
      Long[] blocks = {0L};
      session.commit(blocks);
    }
  }

  public static VolumeTunnel newVolumeTunnel(Odps odps) {
    VolumeTunnel tunnel = new VolumeTunnel(odps);
    String tunnelEndpoint = props.getProperty("default.tunnel.endpoint");
    if (!StringUtils.isNullOrEmpty(tunnelEndpoint)) {
      tunnel.setEndpoint(tunnelEndpoint);
    }
    return tunnel;
  }

  public static String getRandomName(String prefix) {
    return prefix + "_" + getRandomName();
  }

  public static String getRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
