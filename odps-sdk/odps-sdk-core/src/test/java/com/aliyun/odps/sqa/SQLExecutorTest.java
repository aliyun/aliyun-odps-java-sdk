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

import com.aliyun.odps.*;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.tunnel.TableTunnel;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLExecutorTest extends TestBase {
  private static Session session = null;
  private static String sessionName = "test_sdk_session" + System.currentTimeMillis();
  private static String tableName = "test_session_table" + System.currentTimeMillis();
  private static String sql = "select * from " + tableName + ";";
  private static String fallbackSql = "select * from " + tableName + " union all select * from " + tableName + ";";
  private static String sqlNotSelect = "insert overwrite table " + tableName + " select * from "+ tableName +";";

  private static int recordCount = 2998;

  @AfterClass
  public static void after() throws OdpsException {
    if (session != null) {
      session.stop();
    }
    odps.tables().delete(tableName, true);
  }

  @BeforeClass
  public static void prepare() throws OdpsException, IOException {

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.STRING));
    schema.addColumn(new Column("c2", OdpsType.BIGINT));

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

    Map<String, String> flags = new HashMap<String, String>();
    flags.put("odps.sql.session.worker.count", "1");
    flags.put("odps.sql.session.worker.sparespan", "0-0");
    flags.put("odps.sql.jobconf.odps2", "true");
    flags.put("odps.sql.session.worker.memory", "1024");
    flags.put("odps.sql.session.version2", "true");
    flags.put("odps.sql.session.worker.cpu", "33");
    flags.put("odps.sql.session.worker.cache.size", "64");
    flags.put("set odps.sql.session.max.slot.number", "5");

    session = Session.create(odps, sessionName, odps.getDefaultProject(), flags, 0L);
    System.out.println("Create session success: " + session.getInstance().getId());
    Instance i = session.getInstance();
    System.out.println(odps.logview().generateLogView(i, 7*24));
    session.waitForStart(0);
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
  public void testExecutor() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(sql, hint);
    try {
      Thread.sleep(5000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      printRecords(records);
      Assert.assertEquals(records.size(), recordCount);
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
  public void testExecutorResultSet() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName);
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
  public void testExecutorWithLimit() throws OdpsException,IOException {
    int limit = 5;
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    String queryId1 = null;
    Map<String, String> hint = new HashMap<>();
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

    sqlExecutor.run(sql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNull(queryId);

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
    }

    sqlExecutor.run(sql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNull(queryId);

      List<Record> records = sqlExecutor.getResult(Long.valueOf(limit));
      Assert.assertEquals(records.size(), limit);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }

    sqlExecutor.run(sql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNull(queryId);

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
  public void testExecutorFallback() throws OdpsException,IOException {
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
    // will 100% fallback if use this flag
    hint.put("odps.isolation.session.enable", "all");
    sqlExecutor.run(fallbackSql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), recordCount*2);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorResultSetFallback() throws OdpsException,IOException {
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
    // will 100% fallback if use this flag
    hint.put("odps.isolation.session.enable", "all");
    sqlExecutor.run(fallbackSql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), recordCount*2);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorResultSetDisableFallback() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .fallbackPolicy(new FallbackPolicy())
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    // will 100% fallback if use this flag
    hint.put("odps.isolation.session.enable", "all");
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
      Thread.sleep(2000);
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
      Thread.sleep(2000);
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
      Thread.sleep(2000);
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
        .fallbackPolicy(FallbackPolicy.alwaysFallbackPolicy())
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    hint.put("odps.sql.session.select.only", "false");
    sqlExecutor.run(sqlNotSelect, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      System.out.println(sqlExecutor.getLogView());
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), 0);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }

    hint.put("odps.sql.session.select.only", "true");
    // fallback offline
    sqlExecutor.run(sqlNotSelect, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      System.out.println(sqlExecutor.getLogView());
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), 0);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorResultSetNotSelect() throws OdpsException,IOException {
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
    hint.put("odps.sql.session.select.only", "false");
    sqlExecutor.run(sqlNotSelect, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      System.out.println(sqlExecutor.getLogView());
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      Assert.assertEquals(resultSet.getRecordCount(), 0);
      Assert.assertEquals(resultSet.getTableSchema(), null);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }

    hint.put("odps.sql.session.select.only", "true");
    // fallback offline
    sqlExecutor.run(sqlNotSelect, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      Assert.assertEquals(resultSet.getRecordCount(), 0);
      Assert.assertEquals(resultSet.getTableSchema(), null);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorDisableTunnel() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .useInstanceTunnel(false)
        .fallbackPolicy(FallbackPolicy.alwaysFallbackPolicy())
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

    // will 100% fallback if use this flag
    hint.put("odps.isolation.session.enable", "all");
    sqlExecutor.run(fallbackSql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult();
      Assert.assertEquals(records.size(), recordCount*2);
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }

    // will 100% fallback if use this flag
    hint.put("odps.isolation.session.enable", "all");
    sqlExecutor.run(fallbackSql, hint);
    try {
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet resultSet = sqlExecutor.getResultSet();
      List<Record> records = new ArrayList<>();
      while(resultSet.hasNext()) {
        records.add(resultSet.next());
      }
      Assert.assertEquals(records.size(), recordCount*2);
      Assert.assertNotNull(resultSet.getTableSchema());
      printRecords(records);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      sqlExecutor.close();
    }
  }

  @Test
  public void testExecutorGetResultTwice() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(sql, hint);
    try {
      Thread.sleep(5000);
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
  public void testExecutorGetResultLimitSize() throws OdpsException,IOException {
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .serviceName(sessionName);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl)builder.build();
    Assert.assertNotNull(sqlExecutor.getId());

    Map<String, String> hint = new HashMap<>();
    sqlExecutor.run(sql, hint);
    try {
      Thread.sleep(5000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      List<Record> records = sqlExecutor.getResult(10L, 1L);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    } catch (OdpsException e){
      Assert.assertTrue(e.getMessage().contains("InvalidArgument"));
    }

    try {
      Thread.sleep(5000);
      String queryId = sqlExecutor.getQueryId();
      Assert.assertNotNull(queryId);
      Assert.assertTrue(sqlExecutor.isActive());

      ResultSet result = sqlExecutor.getResultSet(10L, 1L);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (InterruptedException e) {
      throw new OdpsException(e.getMessage());
    } catch (OdpsException e){
      Assert.assertTrue(e.getMessage().contains("InvalidArgument"));
    }
    finally {
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
