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

package com.aliyun.odps.sqa;

import static com.aliyun.odps.OdpsType.STRING;
import static org.junit.Assert.assertTrue;

import com.aliyun.odps.*;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.utils.StringUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class SQLExecutorTest extends TestBase {
  private static Session session = null;
  private static String sessionName = "public.default";
  private static String tableName = "test_session_table" + System.currentTimeMillis();
  private static String complexTableName = "test_session_complex_table" + System.currentTimeMillis();
  private static String emptyTableName = "test_session_empty_table" + System.currentTimeMillis();
  private static String bigTableName = "test_big_session_table" + System.currentTimeMillis();
  private static String streamTableName = "test_streamupload_session_table" + System.currentTimeMillis();
  private static String sql = "select * from " + tableName + " where c1!='NotExist';";
  private static String bigSql = "select * from " + bigTableName + " where c1!='NotExist';";
  private static String complexSql = "select * from " + complexTableName + " where c0>0;";
  private static String multiQuerySql = "select c1, sum(c2) from " + bigTableName + " group by c1;";
  private static String fallbackSql = "select * from " + tableName + " union all select * from " + tableName + ";";
  private static String sqlNotSelect = "insert overwrite table " + tableName + " select * from "+ tableName +";";
  private static String sqlLocalMode = "select * from " + bigTableName + ";";
  private static String sqlLocalModeEmpty = "select * from " + emptyTableName + ";";
  private static String sqlLocalModeCol = "select c1 from " + bigTableName + ";";
  private static String sqlLocalModeDisableTunnel = "select * from " + tableName + ";";
  private static String sqlStreamUpload = "select * from " + streamTableName + " where c1!='NotExist';";
  private static String sqlStreamUploadLocalMode = "select * from " + streamTableName + ";";
  private static String sqlSelectInternalDayType = "select INTERVAL '1' DAY;";
  private static String createComplexTypeSql = "create table if not exists " + complexTableName +
                                               " (c0 BIGINT, c1 struct< field1:bigint, field2:array< int>, field3:map< int, int>>);";
  private static String insertComplexTypeSql = "insert overwrite table " + complexTableName +
                                               " values (1, named_struct('field1', 100L, 'field2', array(1, 2), 'field3', map(1, 100, 2, 200)))," +
                                               "(2, named_struct('field1', 100L, 'field2', array(1, 2), 'field3', map(1, 100, 2, 200)))," +
                                               "(3, named_struct('field1', 100L, 'field2', array(1, 2), 'field3', map(1, 100, 2, 200)));";
  private static int streamRecordCount = 300;
  private static int recordCount = 2998;
  private static int bigRecordCount = 100000;

  @AfterClass
  public static void after() throws OdpsException {
    odps.tables().delete(emptyTableName, true);
    odps.tables().delete(tableName, true);
    odps.tables().delete(bigTableName, true);
    odps.tables().delete(streamTableName, true);
    odps.tables().delete(complexTableName, true);
  }

  @BeforeClass
  public static void prepare() throws OdpsException, IOException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.STRING));
    schema.addColumn(new Column("c2", OdpsType.BIGINT));

    odps.tables().create(emptyTableName, schema, true);
    odps.tables().create(tableName, schema, true);

    TableTunnel tableTunnel = new TableTunnel(odps);
    TableTunnel.UploadSession uploadSession = tableTunnel.createUploadSession(odps.getDefaultProject(), tableName);
    RecordWriter writer = uploadSession.openRecordWriter(0);
    for (long i = 0; i < recordCount; ++i) {
      Record record = new ArrayRecord(schema);
      record.setString(0, "test" + i);
      record.setBigint(1, i);
      writer.write(record);
    }
    writer.close();
    uploadSession.commit();

    odps.tables().create(bigTableName, schema, true);

    uploadSession = tableTunnel.createUploadSession(odps.getDefaultProject(), bigTableName);
    writer = uploadSession.openRecordWriter(0);
    for (long i = 0; i < bigRecordCount; ++i) {
      Record record = new ArrayRecord(schema);
      record.setString(0, "testBigTableMaxSize_" + i);
      record.setBigint(1, i);
      writer.write(record);
    }
    writer.close();
    uploadSession.commit();

    Instance ins = SQLTask.run(odps, createComplexTypeSql);
    ins.waitForSuccess();

    ins = SQLTask.run(odps, insertComplexTypeSql);
    ins.waitForSuccess();

    odps.tables().create(streamTableName, schema, true);
    TableTunnel.StreamUploadSession upload = tableTunnel.buildStreamUploadSession(odps.getDefaultProject(), streamTableName).build();
    Record record = upload.newRecord();
    record.setString(0, "value0");
    record.setBigint(1, 1L);
    TableTunnel.StreamRecordPack rawPack = upload.newRecordPack();
    TableTunnel.StreamRecordPack compressedPack = upload.newRecordPack(new CompressOption());
    for (int i = 0; i < 2; ++i) {
      TableTunnel.StreamRecordPack pack;
      if (i % 2 == 0) {
        pack = rawPack;
      } else {
        pack = compressedPack;
      }
      for (int j = 0; j < streamRecordCount; ++j) {
        pack.append(record);
      }
      System.out.println(pack.flush());
    }
  }

  @Test
  public void testExecutorCancel() throws OdpsException, IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.localmode.enable", "false");
    sqlExecutor.run(sql, hint);
    try {
      sqlExecutor.cancel();
      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(true, false);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (OdpsException e) {

    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testTunnelFetchRefreshAttachLiveTime() throws OdpsException, IOException, InterruptedException {
    Map<String, String> properties = new HashMap<>();
    properties.put("odps.sql.session.idle.timeout", "1");
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .useInstanceTunnel(true)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run("select 123;", hint);
    try {
      List<Record> records = sqlExecutor.getResult();
      Thread.sleep(30000);
      records = sqlExecutor.getResult();
      Thread.sleep(20000);
      records = sqlExecutor.getResult();
      Thread.sleep(15000);
      records = sqlExecutor.getResult();
      Thread.sleep(10000);
      records = sqlExecutor.getResult();
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (OdpsException e) {
      throw e;
    } catch (InterruptedException e) {
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorPoolNormal() throws OdpsException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorPoolBuilder sqlExecutorPoolBuilder = SQLExecutorPoolBuilder.builder();
    sqlExecutorPoolBuilder.odps(odps)
        .initPoolSize(1)
        .maxPoolSize(3)
        .serviceName(sessionName)
        .properties(properties)
        .taskName(SQLExecutorConstants.DEFAULT_TASK_NAME);
    SQLExecutorPool sqlExecutorPool = sqlExecutorPoolBuilder.build();

    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 1);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 0);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 1);
    SQLExecutor popExecutor = sqlExecutorPool.getExecutor();
    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 0);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 1);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 1);
    sqlExecutorPool.releaseExecutor(popExecutor);
    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 1);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 0);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 1);

    SQLExecutor popExecutor1 = sqlExecutorPool.getExecutor();
    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 0);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 1);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 1);

    SQLExecutor popExecutor2 = sqlExecutorPool.getExecutor();
    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 0);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 2);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 2);

    SQLExecutor popExecutor3 = sqlExecutorPool.getExecutor();
    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 0);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 3);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 3);

    try {
      SQLExecutor popExecutor4 = sqlExecutorPool.getExecutor();
      Assert.assertTrue(false);
    } catch (OdpsException e) {
      if (e.getMessage().indexOf("No active executor in pool") != -1) {
        Assert.assertTrue(true);
      } else {
        Assert.assertTrue(false);
      }
    }
    sqlExecutorPool.releaseExecutor(popExecutor1);
    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 1);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 2);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 3);
    sqlExecutorPool.releaseExecutor(popExecutor2);
    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 2);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 1);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 3);
    // in pool mode, close executor will release it to pool
    popExecutor3.close();
    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 3);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 0);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 3);

    sqlExecutorPool.releaseExecutor(popExecutor3);
    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 3);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 0);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 3);

    SQLExecutor popExecutor4 = sqlExecutorPool.getExecutor();
    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 2);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 1);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 3);

    SQLExecutor popExecutor5 = sqlExecutorPool.getExecutor();
    Assert.assertEquals(sqlExecutorPool.getActiveCount(), 1);
    Assert.assertEquals(sqlExecutorPool.getBusyCount(), 2);
    Assert.assertEquals(sqlExecutorPool.getExecutorCount(), 3);

    sqlExecutorPool.close();
  }

  @Test
  public void testExecutorResultSet() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(sql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), recordCount);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorResultSetComplexTableSchema() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(complexSql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), 3);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorResultSetTunnelInternalDayType() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.type.system.odps2", "true");
    sqlExecutor.run(sqlSelectInternalDayType, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), 1);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorWithLimitFdcMode() throws OdpsException,IOException,InterruptedException {
    int limit = 5;
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    String queryId1 = null;
    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "fdc");
    sqlExecutor.run(sql, hint);

    try {
      queryId1 = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId1);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult(Long.valueOf(limit));
      Assert.assertEquals(records.size(), limit);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }

    String queryId2 = null;
    sqlExecutor.run(sql, hint);
    try {
      queryId2 = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId2);
      Assert.assertNotEquals(queryId1, queryId2);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet(Long.valueOf(limit));
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), limit);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorWithLimitHybridMode() throws OdpsException,IOException,InterruptedException {
    int limit = 5;
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    String queryId1 = null;
    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "hybrid");
    sqlExecutor.run(sql, hint);

    try {
      queryId1 = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId1);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult(Long.valueOf(limit));
      Assert.assertEquals(records.size(), limit);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }

    String queryId2 = null;
    sqlExecutor.run(sql, hint);
    try {
      queryId2 = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId2);
      Assert.assertNotEquals(queryId1, queryId2);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet(Long.valueOf(limit));
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), limit);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorDefaultOffline() throws OdpsException,IOException {
    int limit = 3;
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.OFFLINE)
        .properties(properties)
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(sql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNull(queryId);

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), recordCount);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testExecutorResultSetDisableFallback() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy())
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    // will 100% fallback if use this flag
    hint.put("odps.mcqa.disable", "true");
    sqlExecutor.run(fallbackSql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      // should not fallback, throw exception
      Assert.assertTrue(false);
    } catch (OdpsException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorRecover() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    Session attach = Session.attach(odps, sessionName, properties, SQLExecutorConstants.DEFAULT_ATTACH_TIMEOUT);
    Instance oldInstance = attach.getInstance();
    String oldInstanceId = attach.getInstance().getId();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .recoverFrom(oldInstance);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());
    Assert.assertEquals(sqlExecutor.getInstance().getId(), oldInstanceId);
    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(sql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), recordCount);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorRecoverInvalid() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    Session attach = Session.attach(odps, sessionName, properties, SQLExecutorConstants.DEFAULT_ATTACH_TIMEOUT);
    Instance oldInstance = attach.getInstance();
    String oldInstanceId = attach.getInstance().getId();
    attach.stop();
    try {
      while(oldInstance.getStatus().equals(Instance.Status.RUNNING)) {
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {

    }
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .recoverFrom(oldInstance);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());
    Assert.assertNotEquals(sqlExecutor.getInstance().getId(), oldInstanceId);
    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(sql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), recordCount);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorReattach() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    Session attach = Session.attach(odps, sessionName, properties, SQLExecutorConstants.DEFAULT_ATTACH_TIMEOUT);
    Instance oldInstance = attach.getInstance();
    String oldInstanceId = attach.getInstance().getId();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .recoverFrom(oldInstance);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());
    Assert.assertEquals(sqlExecutor.getInstance().getId(), oldInstanceId);
    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(sql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), recordCount);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
    // stop session
    attach.stop();
    try {
      while(sqlExecutor.isActive()) {
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {

    }
    sqlExecutor.run(sql, hint);
    Assert.assertNotEquals(sqlExecutor.getInstance().getId(), oldInstanceId);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), recordCount);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorDisableReattach() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    Session attach = Session.attach(odps, sessionName, properties, SQLExecutorConstants.DEFAULT_ATTACH_TIMEOUT);
    attach.printLogView();
    Instance oldInstance = attach.getInstance();
    String oldInstanceId = attach.getInstance().getId();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .enableReattach(false)
        .recoverFrom(oldInstance);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());
    Assert.assertEquals(sqlExecutor.getInstance().getId(), oldInstanceId);
    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(sql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), recordCount);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
    // stop session
    attach.stop();
    try {
      while(sqlExecutor.isActive()) {
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {

    }
    try {
      sqlExecutor.run(sql, hint);
      Assert.assertTrue(false);
    } catch (OdpsException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorNotSelect() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.select.only", "false");
    sqlExecutor.run(sqlNotSelect, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      System.out.println(sqlExecutor.getLogView());
      Assert.assertNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), 0);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testExecutorResultSetNotSelect() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.select.only", "false");
    sqlExecutor.run(sqlNotSelect, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      System.out.println(sqlExecutor.getLogView());
      Assert.assertNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      Assert.assertEquals(resultSet.getRecordCount(), 0);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testExecutorDisableTunnel() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .fallbackPolicy(FallbackPolicy.alwaysFallbackPolicy())
        .properties(properties)
        .useInstanceTunnel(false)
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(sql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());
      // can not use limit if tunnel disabled
      try {
        List<Record> records = sqlExecutor.getResult(10L);
        Assert.assertTrue(false);
      } catch (OdpsException e) {
        Assert.assertTrue(true);
      }
      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), recordCount);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }

    sqlExecutor.run(sql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());
      // can not use limit if tunnel disabled
      try {
        ResultSet resultSet = sqlExecutor.getResultSet(10L);
        Assert.assertTrue(false);
      } catch (OdpsException e) {
        Assert.assertTrue(true);
      }
      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), recordCount);
      Assert.assertNotNull(resultSet.getTableSchema());
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testExecutorGetResultTwiceFdcMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "fdc");
    sqlExecutor.run(sql, hint);
    try {
      Thread.sleep(2000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      printRecords(records);
      Assert.assertEquals(records.size(), recordCount);

      List<Record> records2 = sqlExecutor.getResult();
      printRecords(records2);
      Assert.assertEquals(records2.size(), recordCount);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorGetResultTwiceHybridMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "hybrid");
    sqlExecutor.run(sql, hint);
    try {
      Thread.sleep(2000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      printRecords(records);
      Assert.assertEquals(records.size(), recordCount);

      List<Record> records2 = sqlExecutor.getResult();
      printRecords(records2);
      Assert.assertEquals(records2.size(), recordCount);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorGetResultLimitSizeFdcMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "fdc");
    sqlExecutor.run(sql, hint);
    try {
      Thread.sleep(2000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult(10L, 1L);
      Assert.assertTrue(false);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    } catch (OdpsException e){
      throw e;
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("InvalidArgument"));
    }

    try {
      Thread.sleep(2000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet result = sqlExecutor.getResultSet(10L, 1L);
      List<Record> records = new ArrayList<>();
      while(result.hasNext()) {
        records.add(result.next());
      }
      Assert.assertTrue(false);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    } catch (OdpsException e){
      throw e;
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("InvalidArgument"));
    }
    finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorGetResultLimitSizeHybridMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "hybrid");
    sqlExecutor.run(sql, hint);
    try {
      Thread.sleep(2000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult(10L, 1L);
      Assert.assertTrue(false);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    } catch (OdpsException e){
      throw e;
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("InvalidArgument"));
    }

    try {
      Thread.sleep(2000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet result = sqlExecutor.getResultSet(10L, 1L);
      List<Record> records = new ArrayList<>();
      while(result.hasNext()) {
        records.add(result.next());
      }
      Assert.assertTrue(false);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    } catch (OdpsException e){
      throw e;
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("InvalidArgument"));
    }
    finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorGetResultOffsetFdcMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "fdc");
    sqlExecutor.run(sql, hint);
    try {
      Thread.sleep(2000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());
      Long offset = 15L;
      List<Record> records = sqlExecutor.getResult(offset, 10L, null);
      printRecords(records);
      Assert.assertEquals(records.size(), 10);
      Assert.assertEquals(records.get(0).getBigint(1), offset);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    }

    try {
      Thread.sleep(2000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());
      Long offset = 5L;

      ResultSet resultSet = sqlExecutor.getResultSet(offset, 10L, null);
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), 10);
      Assert.assertNotNull(resultSet.getTableSchema());
      Assert.assertEquals(records.get(0).getBigint(1), offset);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorGetResultOffsetHybridMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "hybrid");
    sqlExecutor.run(sql, hint);
    try {
      Thread.sleep(2000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());
      Long offset = 15L;
      List<Record> records = sqlExecutor.getResult(offset, 10L, null);
      printRecords(records);
      Assert.assertEquals(records.size(), 10);
      Assert.assertEquals(records.get(0).getBigint(1), offset);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    }

    try {
      Thread.sleep(2000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());
      Long offset = 5L;

      ResultSet resultSet = sqlExecutor.getResultSet(offset, 10L, null);
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), 10);
      Assert.assertNotNull(resultSet.getTableSchema());
      Assert.assertEquals(records.get(0).getBigint(1), offset);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorEnableLimitFdcMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    //disable limit 10000
    hint.put("odps.sql.select.auto.limit", "10000000");
    hint.put("odps.sql.session.result.mode", "fdc");
    sqlExecutor.run(multiQuerySql, hint);
    try {
      Long offset = 0L;
      Long count = 100000L;
      List<Record> records1 = sqlExecutor.getResult(offset, count, null, true);
      Assert.assertEquals(records1.size(), 10000);

      List<Record> records2 = sqlExecutor.getResult(offset, count, null, false);
      Assert.assertEquals(records2.size(), bigRecordCount);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorEnableLimitHybridMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    //disable limit 10000
    hint.put("odps.sql.select.auto.limit", "10000000");
    hint.put("odps.sql.session.result.mode", "hybrid");
    sqlExecutor.run(multiQuerySql, hint);
    try {
      Long offset = 0L;
      Long count = 100000L;
      List<Record> records1 = sqlExecutor.getResult(offset, count, null, true);
      Assert.assertEquals(records1.size(), 10000);

      List<Record> records2 = sqlExecutor.getResult(offset, count, null, false);
      Assert.assertEquals(records2.size(), bigRecordCount);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorResultCacheFdcMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    //enable result cache
    hint.put("odps.sql.session.result.cache.enable", "true");
    hint.put("odps.sql.session.result.mode", "fdc");
    sqlExecutor.run(sql, hint);
    try {
      String queryId1 = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId1);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet1 = sqlExecutor.getResultSet();
      List<Record> records1 = new ArrayList<>();
      while(resultSet1.hasNext()) {
        records1.add(resultSet1.next());
      }
      Assert.assertEquals(records1.size(), recordCount);
      printRecords(records1);

      sqlExecutor.run(sql, hint);
      String queryId2 = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId2);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet2 = sqlExecutor.getResultSet();
      List<Record> records2 = new ArrayList<>();
      while(resultSet2.hasNext()) {
        records2.add(resultSet2.next());
      }
      Assert.assertEquals(records2.size(), recordCount);
      printRecords(records2);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorResultCacheHybridMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    //enable result cache
    hint.put("odps.sql.session.result.cache.enable", "true");
    hint.put("odps.sql.session.result.mode", "hybrid");
    sqlExecutor.run(sql, hint);
    try {
      String queryId1 = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId1);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet1 = sqlExecutor.getResultSet();
      List<Record> records1 = new ArrayList<>();
      while(resultSet1.hasNext()) {
        records1.add(resultSet1.next());
      }
      Assert.assertEquals(records1.size(), recordCount);
      printRecords(records1);

      sqlExecutor.run(sql, hint);
      String queryId2 = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId2);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet2 = sqlExecutor.getResultSet();
      List<Record> records2 = new ArrayList<>();
      while(resultSet2.hasNext()) {
        records2.add(resultSet2.next());
      }
      Assert.assertEquals(records2.size(), recordCount);
      printRecords(records2);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorResultCacheHybridModeMaxIo() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    //enable result cache
    hint.put("odps.sql.session.result.cache.enable", "true");
    hint.put("odps.sql.session.result.mode", "hybrid");
    hint.put("odps.sql.runtime.flag.executionengine_SessionHybridFdcResultMaxSizeMB", "1");
    sqlExecutor.run(bigSql, hint);
    try {
      String queryId1 = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId1);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet1 = sqlExecutor.getResultSet();
      List<Record> records1 = new ArrayList<>();
      while(resultSet1.hasNext()) {
        records1.add(resultSet1.next());
      }
      Assert.assertEquals(records1.size(), bigRecordCount);
      printRecords(records1);

      sqlExecutor.run(bigSql, hint);
      String queryId2 = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId2);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet2 = sqlExecutor.getResultSet();
      List<Record> records2 = new ArrayList<>();
      while(resultSet2.hasNext()) {
        records2.add(resultSet2.next());
      }
      Assert.assertEquals(records2.size(), bigRecordCount);
      printRecords(records2);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorLocalModeFdcMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "fdc");
    hint.put("odps.sql.session.localmode.enable", "true");
    hint.put("odps.sql.select.auto.limit", "10000000");
    sqlExecutor.run(sqlLocalMode, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), bigRecordCount);

    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorLocalModeHybridMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "hybrid");
    hint.put("odps.sql.session.localmode.enable", "true");
    hint.put("odps.sql.select.auto.limit", "10000000");
    sqlExecutor.run(sqlLocalMode, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), bigRecordCount);

    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorHybridModeMaxIo() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.runtime.flag.executionengine_SessionHybridFdcResultMaxSizeMB", "1");
    hint.put("odps.sql.session.result.mode", "hybrid");
    sqlExecutor.run(bigSql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), bigRecordCount);

    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorLocalModeHybridModeMaxIo() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.runtime.flag.executionengine_SessionHybridFdcResultMaxSizeMB", "1");
    hint.put("odps.sql.session.result.mode", "hybrid");
    hint.put("odps.sql.session.localmode.enable", "true");
    hint.put("odps.sql.select.auto.limit", "10000000");
    sqlExecutor.run(sqlLocalMode, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), bigRecordCount);

    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorLocalModeEmptyTableFdcMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "fdc");
    hint.put("odps.sql.session.localmode.enable", "true");
    sqlExecutor.run(sqlLocalModeEmpty, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), 0);

    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorLocalModeEmptyTableHybridMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "hybrid");
    hint.put("odps.sql.session.localmode.enable", "true");
    sqlExecutor.run(sqlLocalModeEmpty, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), 0);

    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorLocalModeSpecificColFdcMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "fdc");
    hint.put("odps.sql.session.localmode.enable", "true");
    hint.put("odps.sql.select.auto.limit", "10000000");
    sqlExecutor.run(sqlLocalModeCol, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), bigRecordCount);

    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorLocalModeSpecificColHybridMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "hybrid");
    hint.put("odps.sql.session.localmode.enable", "true");
    hint.put("odps.sql.select.auto.limit", "10000000");
    sqlExecutor.run(sqlLocalModeCol, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), bigRecordCount);

    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorLocalModeSpecificColDisableTunnelFdcMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .useInstanceTunnel(false)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "fdc");
    hint.put("odps.sql.session.localmode.enable", "true");
    sqlExecutor.run(sqlLocalModeDisableTunnel, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), recordCount);

    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorWithSummary() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run("select \"1\" = 1;", hint);
    try {
      Thread.sleep(10000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      printRecords(records);
      Assert.assertEquals(records.size(), 1);
      String summary = sqlExecutor.getSummary();
      System.out.println(summary);
      for(int retry = 0; retry < 10; retry++) {
        summary = sqlExecutor.getSummary();
        if(!StringUtils.isNullOrEmpty(summary)) {
          break;
        }
        Thread.sleep(5000);
      }
      Assert.assertFalse(StringUtils.isNullOrEmpty(summary));
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorFallbackWithSummary() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .fallbackPolicy(FallbackPolicy.alwaysFallbackPolicy())
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.mcqa.disable", "true");
    sqlExecutor.run(fallbackSql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), recordCount * 2);

      String summary = sqlExecutor.getSummary();
      System.out.println(summary);
      Assert.assertFalse(StringUtils.isNullOrEmpty(summary));

      System.out.println(sqlExecutor.getExecutionLog());
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorFallbackForAttachFailed() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName("session_not_exist")
        .fallbackPolicy(FallbackPolicy.alwaysFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(sqlLocalModeDisableTunnel, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      System.out.println(sqlExecutor.getLogView());
      Assert.assertNull(queryId);
      Assert.assertFalse(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      Assert.assertEquals(resultSet.getRecordCount(), recordCount);

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), recordCount);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testStreamUploadSelectFdcMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "fdc");
    sqlExecutor.run(sqlStreamUpload, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), streamRecordCount * 2);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testStreamUploadSelectHybridMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.result.mode", "hybrid");
    sqlExecutor.run(sqlStreamUpload, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), streamRecordCount * 2);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testStreamUploadLocalMode() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.localmode.enable", "true");
    sqlExecutor.run(sqlStreamUploadLocalMode, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), streamRecordCount * 2);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorWithTunnelEndpoint() throws OdpsException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .tunnelEndpoint("http://wrong");
    try {
      SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    } catch (IllegalArgumentException e) {
        Assert.assertEquals("Invalid endpoint.", e.getMessage());
    }
}

  @Test
  public void testExecutorResultSetWithTunnelTimeout() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName)
        .tunnelSocketTimeout(5)
        .tunnelReadTimeout(1)
        .fallbackPolicy(FallbackPolicy.alwaysFallbackExceptAttachPolicy());
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.runtime.flag.executionengine_SessionHybridFdcResultMaxSizeMB", "1");
    hint.put("odps.sql.session.result.mode", "hybrid");
    sqlExecutor.run(bigSql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), bigRecordCount);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (OdpsException e) {
      if (e.getCause() == null) {
        Assert.assertEquals("ErrorCode=Local Error, ErrorMessage=Read timed out", e.getLocalizedMessage());
      } else {
        Assert.assertEquals("Read timed out", e.getCause().getLocalizedMessage());
      }
    } finally {
      sqlExecutor.close();
    }
  }

  private static void printRecords(List<Record> records) {
    for (Record record : records) {
      for (int k = 0; k < record.getColumnCount(); k++) {
        System.out.print("\tColumn index: " + k);
        if (record.getColumns()[k].getType().equals(OdpsType.STRING)) {
          System.out.print("\t value: " + record.getString(k));
        } else if  (record.getColumns()[k].getType().equals(OdpsType.BIGINT)) {
          System.out.print("\t value: " + record.getBigint(k));
        } else {
          System.out.print("\t value: " + record.get(k));
        }
      }
      System.out.println("");
    }
  }
}
