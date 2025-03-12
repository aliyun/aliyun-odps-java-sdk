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
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.sqa.v2.InfoResultSet;
import com.aliyun.odps.utils.StringUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SQLExecutorV2Test {

  private static SQLExecutor executor;
  private static SQLExecutor tunnelExecutor;
  private static SQLExecutor commandApiExecutor;
  private static Odps odps;
  private static final String QUOTA_NAME = "dailyrunnew_mcqa2_quota_nick";

  private static Map<String, String> hints;

  @BeforeClass
  public static void initExecutor() throws OdpsException {
    try {
      SQLExecutorBuilder sqlExecutorBuilder = new SQLExecutorBuilder();
      odps = OdpsTestUtils.newDefaultOdps();
      odps.setDefaultProject("odps2_vm");
      executor =
          sqlExecutorBuilder.odps(odps)
              .quotaName(QUOTA_NAME)
              .useInstanceTunnel(false)
              .enableMcqaV2(true)
              .build();
      tunnelExecutor = new SQLExecutorBuilder().odps(odps)
          .quotaName(QUOTA_NAME)
          .useInstanceTunnel(true)
          .enableMcqaV2(true)
          .build();
      commandApiExecutor = new SQLExecutorBuilder().odps(odps)
          .quotaName(QUOTA_NAME)
          .enableCommandApi(true)
          .enableMcqaV2(true)
          .build();
      hints = new HashMap<>();
    } catch (Exception e) {
      // skip tests if odps is not available
      Assume.assumeNoException(e);
    }
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
               + "            \"Type\": \"bigint\"\n"
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
}
