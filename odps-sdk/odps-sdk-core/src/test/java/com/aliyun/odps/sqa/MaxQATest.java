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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Quota;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.sqa.v2.FallbackInfo;
import com.aliyun.odps.sqa.v2.InfoResultSet;
import com.aliyun.odps.sqa.v2.MaxQAConnInfo;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.utils.StringUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class MaxQATest {

  private static SQLExecutor executor;
  private static SQLExecutor tunnelExecutor;
  private static SQLExecutor commandApiExecutor;
  private static Odps odps;
  public static final String QUOTA_NAME = "maxqa_huigui_quota";
  public static final String PROJECT_NAME = "three_pangu2_odps2";

  private static Map<String, String> hints;

  @BeforeClass
  public static void initExecutor() throws OdpsException {
    try {
      SQLExecutorBuilder sqlExecutorBuilder = new SQLExecutorBuilder();
      odps = OdpsTestUtils.newDefaultOdps();
      odps.setDefaultProject("three_pangu2_odps2");
      executor =
          sqlExecutorBuilder.odps(odps)
              .quotaName(QUOTA_NAME)
              .useInstanceTunnel(false)
            .enableMaxQA(true)
              .build();
      tunnelExecutor = new SQLExecutorBuilder().odps(odps)
          .useInstanceTunnel(true)
        .enableMaxQA(true)
        .maxQAConnInfo(MaxQAConnInfo.builder().quotaName(QUOTA_NAME).build())
          .build();
      commandApiExecutor = new SQLExecutorBuilder().odps(odps)
          .quotaName(QUOTA_NAME)
          .enableCommandApi(true)
        .enableMaxQA(true)
          .build();
      hints = new HashMap<>();

      executor.run("create table if not exists mcqa2_test(c1 bigint);", null);
      executor.getResult();
    } catch (Exception e) {
      // skip tests if odps is not available
      Assume.assumeNoException(e);
    }
  }

  @Test
  public void testGetResultWithoutTunnelWithAllTypes() throws Exception {

    Map<String, String> hints = new HashMap<>();
    hints.put("odps.sql.decimal2.extended.scale.enable", "true");
    hints.put("odps.sql.type.system.odps2", "true");
    hints.put("odps.sql.decimal.odps2", "true");
    hints.put("odps.sql.hive.compatible", "true");
    hints.put("odps.sql.mcqa2.result.cache.enable", "false");

    String dropTable = "DROP TABLE IF EXISTS all_types_test_data;";
    executor.run(dropTable, hints);
    System.out.println(executor.getLogView());
    System.out.println(executor.getResult().toString());

    String createTable = "CREATE TABLE IF NOT EXISTS all_types_test_data\n"
               + "(\n"
               + "    -- 描述，用于标识测试用例\n"
               + "    test_case_description   STRING COMMENT 'Test case description, e.g., max_values, min_values, zero, nulls',\n"
               + "\n"
               + "    -- 整型\n"
               + "    c_tinyint               TINYINT COMMENT 'TINYINT type test column',\n"
               + "    c_smallint              SMALLINT COMMENT 'SMALLINT type test column',\n"
               + "    c_int                   INT COMMENT 'INT type test column',\n"
               + "    c_bigint                BIGINT COMMENT 'BIGINT type test column',\n"
               + "\n"
               + "    -- 浮点型\n"
               + "    c_float                 FLOAT COMMENT 'FLOAT type test column',\n"
               + "    c_double                DOUBLE COMMENT 'DOUBLE type test column',\n"
               + "    \n"
               + "    -- 高精度 Decimal\n"
               + "    -- 标准 DECIMAL\n"
               + "    c_decimal_std           DECIMAL(38, 18) COMMENT 'Standard DECIMAL(38, 18)',\n"
               + "    -- 极限 precision 和 scale (需要开启扩展)\n"
               + "    c_decimal_extended      DECIMAL(38, 38) COMMENT 'Extended DECIMAL(38, 38) for high precision fractions',\n"
               + "    -- 整数 DECIMAL\n"
               + "    c_decimal_integer       DECIMAL(38, 0) COMMENT 'Integer-only DECIMAL(38, 0)',\n"
               + "\n"
               + "    -- 字符/字符串类型\n"
               + "    c_varchar               VARCHAR(20) COMMENT 'VARCHAR(20) type test column',\n"
               + "    c_char                  CHAR(10) COMMENT 'CHAR(10) type test column',\n"
               + "    c_string                STRING COMMENT 'STRING type test column',\n"
               + "\n"
               + "    -- 二进制类型\n"
               + "    c_binary                BINARY COMMENT 'BINARY type test column',\n"
               + "    \n"
               + "    -- 日期和时间类型\n"
               + "    c_date                  DATE COMMENT 'DATE type test column',\n"
               + "    c_datetime              DATETIME COMMENT 'DATETIME type test column',\n"
               + "    c_timestamp             TIMESTAMP COMMENT 'TIMESTAMP type test column',\n"
               + "    c_timestamp_ntz         TIMESTAMP_NTZ COMMENT 'TIMESTAMP_NTZ (No Time Zone) type test column',\n"
               + "    \n"
               + "    -- 布尔类型\n"
               + "    c_boolean               BOOLEAN COMMENT 'BOOLEAN type test column'\n"
               + "    \n"
               + ")\n"
               + "COMMENT 'A comprehensive table for testing all data types and their boundary values';\n"
               + "\n";
    executor.run(createTable, hints);
    System.out.println(executor.getLogView());
    System.out.println(executor.getResult().toString());

    String insertSql = "-- 在执行前，请确保你已经开启了 decimal 扩展支持（如果需要测试 DECIMAL(38,38)）\n"
               + "-- SET odps.sql.decimal2.extended.scale.enable=true;\n"
               + "\n"
               + "INSERT OVERWRITE TABLE all_types_test_data VALUES\n"
               + "    -- Test Case 1: Maximum Values\n"
               + "    ('Maximum Values', \n"
               + "     127Y,                                          -- c_tinyint\n"
               + "     32767S,                                        -- c_smallint\n"
               + "     2147483647,                                    -- c_int\n"
               + "     9223372036854775807L,                          -- c_bigint\n"
               + "     CAST('3.4028235E+38' AS FLOAT),                -- c_float (approx max)\n"
               + "     CAST('1.7976931348623157E+308' AS DOUBLE),     -- c_double (approx max)\n"
               + "     99999999999999999999.999999999999999999BD,      -- c_decimal_std (38, 18) max\n"
               + "     0.99999999999999999999999999999999999999BD,    -- c_decimal_extended (38, 38) max\n"
               + "     99999999999999999999999999999999999999BD,      -- c_decimal_integer (38, 0) max\n"
               + "     'varchar_max_len_test',                        -- c_varchar\n"
               + "     'char_max  ',                                  -- c_char (会自动补空格到10位)\n"
               + "     'A very long string test case with various characters:!@#$%^&*()_+{}[]:;\"<>,.?/~` and 中文、日語、한국어', -- c_string\n"
               + "     unhex('FFFEFD'),                               -- c_binary\n"
               + "     DATE'9999-12-31',                              -- c_date\n"
               + "     DATETIME'9999-12-31 23:59:59',                 -- c_datetime\n"
               + "     TIMESTAMP'9999-12-31 23:59:59.999999999',      -- c_timestamp\n"
               + "     TIMESTAMP_NTZ'9999-12-31 23:59:59.999999999',  -- c_timestamp_ntz\n"
               + "     True                                          -- c_boolean\n"
               + "    ),\n"
               + "\n"
               + "    -- Test Case 2: Minimum Values\n"
               + "    ('Minimum Values',\n"
               + "     -128Y,                                         -- c_tinyint\n"
               + "     -32768S,                                       -- c_smallint\n"
               + "     -2147483648,                                   -- c_int\n"
               + "     -9223372036854775807L,                         -- c_bigint (注意：ODPS文档写的是-2^63+1，但实际通常是-2^63)\n"
               + "     CAST('-3.4028235E+38' AS FLOAT),               -- c_float (approx min)\n"
               + "     CAST('-1.7976931348623157E+308' AS DOUBLE),    -- c_double (approx min)\n"
               + "     -9999999999999999999.999999999999999999BD,     -- c_decimal_std (38, 18) min\n"
               + "     -0.99999999999999999999999999999999999999BD,   -- c_decimal_extended (38, 38) min\n"
               + "     -9999999999999999999999999999999999999BD,     -- c_decimal_integer (38, 0) min\n"
               + "     'varchar_min_test',                            -- c_varchar\n"
               + "     'char_min',                                    -- c_char\n"
               + "     'Another string with escapes \\' and \\\" and \\\\',-- c_string\n"
               + "     unhex('000102'),                               -- c_binary\n"
               + "     DATE'0001-01-01',                              -- c_date\n"
               + "     DATETIME'0001-01-01 00:00:00',                 -- c_datetime\n"
               + "     TIMESTAMP'0001-01-01 00:00:00.000000000',      -- c_timestamp\n"
               + "     TIMESTAMP_NTZ'0000-01-01 00:00:00.000000000',  -- c_timestamp_ntz\n"
               + "     False                                         -- c_boolean\n"
               + "    ),\n"
               + "    \n"
               + "    -- Test Case 3: Zero / Empty / Epoch values\n"
               + "    ('Zero and Empty Values',\n"
               + "     0Y,                                            -- c_tinyint\n"
               + "     0S,                                            -- c_smallint\n"
               + "     0,                                             -- c_int\n"
               + "     0L,                                            -- c_bigint\n"
               + "     0.0F,                                          -- c_float\n"
               + "     0.0D,                                          -- c_double\n"
               + "     0.0BD,                                         -- c_decimal_std\n"
               + "     0.0BD,                                         -- c_decimal_extended\n"
               + "     0BD,                                           -- c_decimal_integer\n"
               + "     '',                                            -- c_varchar (empty string)\n"
               + "     '',                                            -- c_char (empty string, will be padded to 10 spaces)\n"
               + "     '',                                            -- c_string (empty string)\n"
               + "     X'',                                           -- c_binary (empty binary)\n"
               + "     DATE'1970-01-01',                              -- c_date (Epoch date)\n"
               + "     DATETIME'1970-01-01 00:00:00',                 -- c_datetime (Epoch datetime)\n"
               + "     TIMESTAMP'1970-01-01 00:00:00.000',            -- c_timestamp (Epoch timestamp)\n"
               + "     TIMESTAMP_NTZ'1970-01-01 00:00:00.000',        -- c_timestamp_ntz\n"
               + "     False                                         -- c_boolean\n"
               + "    ),\n"
               + "    \n"
               + "    -- Test Case 4: High Precision and Low Precision\n"
               + "    ('High and Low Precision',\n"
               + "     1Y,                                            -- c_tinyint\n"
               + "     1S,                                            -- c_smallint\n"
               + "     1,                                             -- c_int\n"
               + "     1L,                                            -- c_bigint\n"
               + "     CAST('1.401298E-45' AS FLOAT),                 -- c_float (smallest positive non-zero)\n"
               + "     CAST('4.9E-324' AS DOUBLE),                    -- c_double (smallest positive non-zero)\n"
               + "     0.000000000000000001BD,                        -- c_decimal_std (low precision)\n"
               + "     0.00000000000000000000000000000000000001BD,    -- c_decimal_extended (ultra low precision)\n"
               + "     1BD,                                           -- c_decimal_integer\n"
               + "     '高精度低精度',                                  -- c_varchar\n"
               + "     '高精度',                                      -- c_char\n"
               + "     'Test Precision',                              -- c_string\n"
               + "     X'616263',                                     -- c_binary (abc)\n"
               + "     DATE'2023-10-27',                              -- c_date\n"
               + "     DATETIME'2023-10-27 10:30:15',                 -- c_datetime\n"
               + "     TIMESTAMP'2023-10-27 10:30:15.123456789',      -- c_timestamp (high precision)\n"
               + "     TIMESTAMP_NTZ'2023-10-27 10:30:15.123',        -- c_timestamp_ntz (low precision)\n"
               + "     True                                          -- c_boolean\n"
               + "    ),\n"
               + "    \n"
               + "    -- Test Case 5: All NULL values\n"
               + "    ('All NULL values',\n"
               + "     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL\n"
               + "    );\n";
    executor.run(insertSql, hints);
    System.out.println(executor.getLogView());
    System.out.println(executor.getResult().toString());

    String selectSql = "select * from all_types_test_data;";
    executor.run(selectSql, hints);
    System.out.println(executor.getLogView());
    System.out.println(executor.getResult().toString());
  }



  @Test
  public void testIsSelect() throws Exception {
    executor.run("select 1;", hints);
    executor.getInstance().waitForTerminatedAndGetResult();
    boolean select = executor.getInstance().isSelect(executor.getTaskName());
    Assert.assertTrue(select);

    executor.run("create table if not exists mcqa2_test(c1 string) lifecycle 1;", hints);
    executor.getInstance().waitForTerminatedAndGetResult();
    boolean select2 = executor.getInstance().isSelect(executor.getTaskName());
    Assert.assertFalse(select2);
  }

  @Test
  public void testParseXml() throws Exception {
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
               + "<Instance>\n"
               + "<Tasks>\n"
               + "  <Task Type=\"SQL\">\n"
               + "    <Name>console_query_task_13</Name>\n"
               + "    <Status>Success</Status>\n"
               + "    <ResultDescriptor>{\n"
               + "    \"IsSelect\": false,\n"
               + "    \"Schema\": {\n"
               + "      \"Columns\": [\n"
               + "        {\n"
               + "            \"Name\": \"wr_returned_date_sk\",\n"
               + "            \"Type\": \"Decimal(38,8)\"\n"
               + "        },\n"
               + "        {\n"
               + "            \"Name\": \"wr_returned_time_sk\",\n"
               + "            \"Type\": \"bigint\"\n"
               + "        }\n"
               + "        ]\n"
               + "      }\n"
               + "    }\n"
               + "    </ResultDescriptor>\n"
               + "    <Result Format=\"text\">1365937150772213:a_view\n"
               + "1365937150772213:acid2_table_dest\n"
               + "1365937150772213:acid2_table_src\n"
               + "1365937150772213:acid_address_book_base\n"
               + "    </Result>\n"
               + "  </Task>\n"
               + "</Tasks></Instance>";

    Instance.InstanceResultModel
        taskResult =
        SimpleXmlUtils.unmarshal(xml.getBytes(), Instance.InstanceResultModel.class);
    Assert.assertNotNull(taskResult);
  }


  @Test
  public void testCancelJobTest() throws OdpsException, IOException {
    executor.run("select 1;", hints);
    executor.getResultSet().forEach(System.out::println);
    executor.cancel();
  }

  @Test
  public void testLoadQuota() throws OdpsException {
    Quota quota = odps.quotas()
        .getWlmQuota(odps.getDefaultProject(), QUOTA_NAME, null);
    boolean interactiveQuota = quota.isInteractiveQuota();
    Assert.assertTrue(interactiveQuota);
    String mcqaConnHeader = quota.getMcqaConnHeader();
    Assert.assertNotNull(mcqaConnHeader);
  }

  @Test
  public void testThrowExceptionWhenCreateError() throws OdpsException, IOException {
    String sql = "CREATE TABLE dwd_caefi_ent_keywords_info\n"
                 + "(\n"
                 + "    id         INT COMMENT 'id'\n"
                 + "    ,mid       STRING COMMENT 'mid(标题+发布时间)'\n"
                 + "    ,keywords  STRING COMMENT '关键字 多个以,拼接'\n"
                 + "    ,keywords_data  STRING COMMENT '关键字'\n"
                 + "    ,created   DATETIME COMMENT '创建时间'\n"
                 + "    ,updated   DATETIME COMMENT '修改时间'\n"
                 + "    ,PRIMARY KEY (id)\n"
                 + ")\n"
                 + "STORED AS ALIORC\n"
                 + "TBLPROPERTIES ('comment' = '舆情数据-企业名称(keywords炸开)','transactional' = 'true','write.bucket.num' = '16');\n";

    tunnelExecutor.run(sql, hints);
    System.out.println(tunnelExecutor.getLogView());
    try {
      ResultSet resultSet = tunnelExecutor.getResultSet();
      resultSet.forEach(record -> System.out.println(record.toString()));
      Assert.fail();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testRunDDL() throws OdpsException, IOException {
    executor.run("create table if not exists sdk_ddl_test(c1 string);", hints);
    List<Record> result = executor.getResult();
    result.forEach(System.out::println);
  }

  @Test
  public void testCommandApi() throws OdpsException, IOException {
    commandApiExecutor.run("desc mcqa2_test;", hints);
    List<Record> result = commandApiExecutor.getResult();
    Assert.assertTrue(result != null && !result.isEmpty());

    ResultSet resultSet = commandApiExecutor.getResultSet();
    Assert.assertTrue(resultSet != null && resultSet.getRecordCount() == -1);
  }

  @Test
  public void testGetTaskSummary() throws OdpsException, InterruptedException {
    executor.run("select 1;", hints);
    executor.getInstance().waitForSuccess();
    TimeUnit.SECONDS.sleep(10);
    String summary = executor.getSummary();
    System.out.println(summary);
    Assert.assertTrue(StringUtils.isNotBlank(summary));
  }

  @Test
  public void testSelectNonTunnel() throws OdpsException, IOException {
    executor.run("select 1;", hints);
    List<Record> result = executor.getResult();
    Assert.assertTrue(result != null && !result.isEmpty());
    ResultSet resultSet = executor.getResultSet();
    Assert.assertTrue(resultSet != null && resultSet.getRecordCount() > 0);
  }

  @Test
  public void testNonSelectNonTunnel() throws OdpsException, IOException {
    executor.run("desc mcqa2_test;", hints);
    List<Record> result = executor.getResult();
    Assert.assertTrue(result != null && !result.isEmpty());
    result.forEach(r -> System.out.println(r.toString()));

    ResultSet resultSet = executor.getResultSet();
    Assert.assertTrue(resultSet instanceof InfoResultSet);
    Assert.assertTrue(resultSet.getRecordCount() == 1);
  }


  @Test
  public void testSelectByTunnel() throws OdpsException, IOException {
    tunnelExecutor.run("select 1;", hints);
    List<Record> result = tunnelExecutor.getResult();
    Assert.assertTrue(result != null && !result.isEmpty());
    ResultSet resultSet = tunnelExecutor.getResultSet();
    Assert.assertTrue(resultSet != null && resultSet.getRecordCount() > 0);
  }

  @Test
  public void testNonSelectByTunnel() throws OdpsException, IOException {
    tunnelExecutor.run("desc mcqa2_test;", hints);
    List<Record> result = tunnelExecutor.getResult();
    Assert.assertTrue(result != null && !result.isEmpty());
    ResultSet resultSet = tunnelExecutor.getResultSet();
    Assert.assertTrue(resultSet instanceof InfoResultSet);
    Assert.assertTrue(resultSet.getRecordCount() == 1);
  }

  @Test
  public void generateLogview() throws OdpsException {
    executor.run("select 1;", hints);
    String logView = executor.getLogView();
    System.out.println(logView);
    Assert.assertNotNull(logView);
  }

  @Test
  public void testSpecialType() throws Exception {
    executor.run("select repeat(\"A\", 1), repeat(\"A\", 3), repeat(\"abc\\0d\", 2), repeat(NULL, 2), repeat(\"\", NULL);", hints);
    List<Record> result = executor.getResult();
    Assert.assertTrue(result != null && !result.isEmpty());
    result.forEach(r -> System.out.println(r.toString()));
  }

  @Test
  public void testGetBigResultByTunnel() throws Exception {
    odps.tables().delete("bigTable", true);

    odps.tables().newTableCreator("bigTable", TableSchema.builder().withStringColumn("c1").build())
        .withLifeCycle(1L)
        .ifNotExists()
        .create();
    TableTunnel.UploadSession uploadSession = odps.tableTunnel()
        .createUploadSession(odps.getDefaultProject(), "bigTable");
    RecordWriter recordWriter = uploadSession.openRecordWriter(0);
    for(int i = 0; i < 100000; i++) {
      Record record = uploadSession.newRecord();
      record.set(0, "test");
      recordWriter.write(record);
    }
    recordWriter.close();
    uploadSession.commit();

    tunnelExecutor.run("select * from bigTable;", null);
    ResultSet resultSet = tunnelExecutor.getResultSet();
    Assert.assertEquals(100000, resultSet.getRecordCount());

    int count = 0;
    while (resultSet.hasNext()) {
      count++;
      resultSet.next();
    }
    System.out.println(count);
    Assert.assertEquals(100000, count);
  }

  @Test
  public void testGetBigResultByApi() throws Exception {
    odps.tables().delete("bigTable", true);

    odps.tables().newTableCreator("bigTable", TableSchema.builder().withStringColumn("c1").build())
      .withLifeCycle(1L)
      .ifNotExists()
      .create();
    TableTunnel.UploadSession uploadSession = odps.tableTunnel()
      .createUploadSession(odps.getDefaultProject(), "bigTable");
    RecordWriter recordWriter = uploadSession.openRecordWriter(0);
    for(int i = 0; i < 100000; i++) {
      Record record = uploadSession.newRecord();
      record.set(0, "test");
      recordWriter.write(record);
    }
    recordWriter.close();
    uploadSession.commit();

    executor.run("select * from bigTable;", null);

    System.out.println(executor.getLogView());
    System.out.println(executor.getInstance().getResultDescriptor(executor.getTaskName())
                         .getSelectResultStatus());
    Assert.assertEquals(Instance.ResultDescriptor.SelectResultStatus.TRUNCATED,
                        executor.getInstance().getResultDescriptor(executor.getTaskName())
                          .getSelectResultStatus());

    ResultSet resultSet = executor.getResultSet();
    Assert.assertEquals(100000, resultSet.getRecordCount());

    int count = 0;
    while (resultSet.hasNext()) {
      count++;
      resultSet.next();
    }
    System.out.println(count);

    executor.getExecutionLog().forEach(System.out::println);
    Assert.assertEquals(100000, count);
  }

  @Test
  public void notSpecificQuotaTest() {
    try {
      SQLExecutor sqlExecutor = new SQLExecutorBuilder()
        .odps(odps)
        .enableMaxQA(true)
        .build();
    } catch (OdpsException e) {
      e.printStackTrace();
      Assert.assertTrue("Expect error contains 'Cannot find MCQA quota'",
                        e.getMessage().contains("Cannot find MCQA quota to start connection"));
    }
  }

  @Test
  public void fallbackTest() throws Exception {
      SQLExecutor sqlExecutor = new SQLExecutorBuilder()
        .odps(odps)
        .enableMaxQA(true)
        .maxQAConnInfo(MaxQAConnInfo.builder().
                         quotaName(QUOTA_NAME)
                         .fallbackInfo(FallbackInfo.enable("default")).
                         build())
        .build();

      sqlExecutor.run("select 1;", null);

  }
}
