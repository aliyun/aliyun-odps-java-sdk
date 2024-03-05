package com.aliyun.odps.sqa;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.task.SQLTask;

public class SQLExecutorGetResultTest extends TestBase {

  private static final String tableName = "sqlExecutror_getResult_test";
  private static final String dropTableSql = "drop table if exists " + tableName + ";";
  private static String
      createTableSql =
      "create table " + tableName + " (key string, value bigint);";
  private static String selectSql = "select * from " + tableName + ";";
  private static String selectSql2 = "from " + tableName + " select *" + ";";
  private static String
      selectSql3 =
      "with a as (select * from  " + tableName + ") select * from a" + ";";

  private static String nonSelectSql = "explain select * from " + tableName + ";";
  private static String nonSelectSql2 = "show create table " + tableName + ";";

  @BeforeClass
  public static void prepare() throws OdpsException {
    SQLTask.run(odps, dropTableSql).waitForSuccess();
    SQLTask.run(odps, createTableSql).waitForSuccess();
    SQLTask.run(odps, "insert into " + tableName + " values(\"test\", 123L);").waitForSuccess();
  }

  @AfterClass
  public static void after() throws OdpsException {
    SQLTask.run(odps, dropTableSql).waitForSuccess();
  }

  @Test
  public void mcqaTunnelGetResultSetTest() throws OdpsException, IOException {
    System.out.println("mcqaTunnelGetResultSetTest");
    getResultSetTest(ExecuteMode.INTERACTIVE, true);
    System.out.println();
  }

//1
  @Test
  public void mcqaNoTunnelGetResultSetTest() throws OdpsException, IOException {
    System.out.println("mcqaNoTunnelGetResultSetTest");
    getResultSetTest(ExecuteMode.INTERACTIVE, false);
    System.out.println();
  }

  @Test
  public void offlineTunnelGetResultSetTest() throws OdpsException, IOException {
    System.out.println("offlineTunnelGetResultSetTest");
    getResultSetTest(ExecuteMode.OFFLINE, true);
    System.out.println();
  }

  @Test
  public void offlineNoTunnelGetResultSetTest() throws OdpsException, IOException {
    System.out.println("offlineNoTunnelGetResultSetTest");
    getResultSetTest(ExecuteMode.OFFLINE, false);
    System.out.println();
  }


  @Test
  public void mcqaTunnelGetResultTest() throws OdpsException, IOException {
    System.out.println("mcqaTunnelGetResultTest");
    getResultSetTest(ExecuteMode.INTERACTIVE, true);
    System.out.println();
  }

//2
  @Test
  public void mcqaNoTunnelGetResultTest() throws OdpsException, IOException {
    System.out.println("mcqaNoTunnelGetResultTest");
    getResultSetTest(ExecuteMode.INTERACTIVE, false);
    System.out.println();
  }

  @Test
  public void offlineTunnelGetResultTest() throws OdpsException, IOException {
    System.out.println("offlineTunnelGetResultTest");
    getResultSetTest(ExecuteMode.OFFLINE, true);
    System.out.println();
  }

  @Test
  public void offlineNoTunnelGetResultTest() throws OdpsException, IOException {
    System.out.println("offlineNoTunnelGetResultTest");
    getResultSetTest(ExecuteMode.OFFLINE, false);
    System.out.println();
  }

  public void getResultSetTest(ExecuteMode mode, boolean isUseInstanceTunnel)
      throws OdpsException, IOException {
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(mode)
        .useInstanceTunnel(isUseInstanceTunnel);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl) builder.build();
    ResultSet resultSet;
    Record record;

    // MCQA Tunnel
    // 1. select
    sqlExecutor.run(selectSql, null);
    resultSet = sqlExecutor.getResultSet();
    System.out.println(selectSql);
    record = resultSet.next();
    Assert.assertEquals(record.getString(0), "test");
    if (isUseInstanceTunnel) {
      Assert.assertEquals(record.getBigint(1).longValue(), 123L);
    } else {
      Assert.assertEquals(record.get(1).toString(), "123");
    }

    sqlExecutor.run(selectSql2, null);
    resultSet = sqlExecutor.getResultSet();
    System.out.println(selectSql2);
    record = resultSet.next();
    Assert.assertEquals(record.getString(0), "test");
    if (isUseInstanceTunnel) {
      Assert.assertEquals(record.getBigint(1).longValue(), 123L);
    } else {
      Assert.assertEquals(record.get(1).toString(), "123");
    }

    sqlExecutor.run(selectSql3, null);
    resultSet = sqlExecutor.getResultSet();
    System.out.println(selectSql3);
    record = resultSet.next();
    Assert.assertEquals(record.getString(0), "test");
    if (isUseInstanceTunnel) {
      Assert.assertEquals(record.getBigint(1).longValue(), 123L);
    } else {
      Assert.assertEquals(record.get(1).toString(), "123");
    }

    // 2. non-select
    sqlExecutor.run(nonSelectSql, null);
    resultSet = sqlExecutor.getResultSet();
    System.out.println(nonSelectSql);
    record = resultSet.next();
    System.out.println(record.getString(0));
    Assert.assertNotNull(record.getString(0));

    sqlExecutor.run(nonSelectSql2, null);
    resultSet = sqlExecutor.getResultSet();
    System.out.println(nonSelectSql2);
    record = resultSet.next();
    System.out.println(record.getString(0));
    Assert.assertNotNull(record.getString(0));

    Map<String, String> hints = new HashMap<>();
    hints.put("odps.sql.session.select.only", "false");
    sqlExecutor.run(nonSelectSql2, hints);
    resultSet = sqlExecutor.getResultSet();
    System.out.println(nonSelectSql2);
    record = resultSet.next();
    System.out.println(record.getString(0));
    Assert.assertNotNull(record.getString(0));
  }


  public void getResultTest(ExecuteMode mode, boolean isUseInstanceTunnel)
      throws OdpsException, IOException {

    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(mode)
        .useInstanceTunnel(isUseInstanceTunnel);
    SQLExecutorImpl sqlExecutor = (SQLExecutorImpl) builder.build();
    Record record;
    List<Record> results;

    sqlExecutor.run(selectSql, null);
    results = sqlExecutor.getResult();
    System.out.println(selectSql);
    record = results.get(0);
    Assert.assertEquals(record.getString(0), "test");
    if (isUseInstanceTunnel) {
      Assert.assertEquals(record.getBigint(1).longValue(), 123L);
    } else {
      Assert.assertEquals(record.get(1).toString(), "123");
    }

    sqlExecutor.run(selectSql2, null);
    results = sqlExecutor.getResult();
    System.out.println(selectSql2);
    record = results.get(0);
    Assert.assertEquals(record.getString(0), "test");
    if (isUseInstanceTunnel) {
      Assert.assertEquals(record.getBigint(1).longValue(), 123L);
    } else {
      Assert.assertEquals(record.get(1).toString(), "123");
    }

    sqlExecutor.run(selectSql3, null);
    results = sqlExecutor.getResult();
    System.out.println(selectSql3);
    record = results.get(0);
    Assert.assertEquals(record.getString(0), "test");
    if (isUseInstanceTunnel) {
      Assert.assertEquals(record.getBigint(1).longValue(), 123L);
    } else {
      Assert.assertEquals(record.get(1).toString(), "123");
    }

    sqlExecutor.run(nonSelectSql, null);
    results = sqlExecutor.getResult();
    System.out.println(nonSelectSql);
    record = results.get(0);
    System.out.println(record.getString(0));
    Assert.assertNotNull(record.getString(0));

    sqlExecutor.run(nonSelectSql2, null);
    results = sqlExecutor.getResult();
    System.out.println(nonSelectSql2);
    record = results.get(0);
    System.out.println(record.getString(0));
    Assert.assertNotNull(record.getString(0));


    Map<String, String> hints = new HashMap<>();
    hints.put("odps.sql.session.select.only", "false");
    sqlExecutor.run(nonSelectSql2, hints);
    results = sqlExecutor.getResult();
    System.out.println(nonSelectSql2);
    record = results.get(0);
    System.out.println(record.getString(0));
    Assert.assertNotNull(record.getString(0));

  }

}
