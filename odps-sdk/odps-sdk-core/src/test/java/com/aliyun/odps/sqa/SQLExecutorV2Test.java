package com.aliyun.odps.sqa;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Quota;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Ignore
public class SQLExecutorV2Test {

  private static SQLExecutor executor;
  private static SQLExecutor tunnelExecutor;
  private static Odps odps;
  private static final String QUOTA_NAME = "mx_vw";

  private static Map<String, String> hints;

  private static Odps getTestOdps() throws OdpsException {
    Account
        account =
        new AliyunAccount("ak", "sk");
    Odps odps = new Odps(account);
    odps.setEndpoint("endpoint");
    odps.setDefaultProject("vw2");
    return odps;
  }


  @Test
  public void testCancelJobTest() throws OdpsException, IOException {
    executor.run("select 1;", hints);
    executor.getResultSet().forEach(System.out::println);
    executor.cancel();
    try {
      executor.getInstance().stop();
    } catch (OdpsException e) {
      System.out.println(e.getErrorCode());
      if (e.getErrorCode().equals("InvalidStateSetting")) {
        System.out.println("good");
      }
    }
  }


  @Test
  public void testGetTenantId() throws OdpsException {
    System.out.println(odps.projects().get("three_schema_project").getTenantId());
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
    ResultSet resultSet = tunnelExecutor.getResultSet();

    resultSet.forEach(record -> System.out.println(record.toString()));
  }


  @BeforeClass
  public static void initExecutor() throws OdpsException {
    SQLExecutorBuilder sqlExecutorBuilder = new SQLExecutorBuilder();
    odps = getTestOdps();
    executor =
        sqlExecutorBuilder.odps(odps)
            .quotaName(QUOTA_NAME)
            .useInstanceTunnel(false)
            .enableMcqaV2(true)
            .enableCommandApi(true)
            .build();
    tunnelExecutor = new SQLExecutorBuilder().odps(odps)
        .quotaName(QUOTA_NAME)
        .useInstanceTunnel(true)
        .enableMcqaV2(true)
        .build();
    hints = new HashMap<>();
    hints.put("odps.service.mode", "only");
    hints.put("odps.sql.enable.enhanced.local.mode", "false");
    hints.put("odps.sql.runtime.flag.executionengine_CheckJobPlanVersionInSmode", "false");
  }

  @Test
  public void testCheckQuota() throws OdpsException {
    Quota quota = odps.quotas().getWlmQuota(odps.getDefaultProject(), QUOTA_NAME);
    System.out.println(
        quota.isInteractiveQuota());
    System.out.println(new String(Base64.getDecoder().decode(quota.getMcqaConnHeader())));
  }

  @Test
  public void testRunDDL() throws OdpsException, IOException {
    executor.run("create table if not exists sdk_ddl_test(c1 string);", hints);
    List<Record> result = executor.getResult();
    result.forEach(System.out::println);
  }

  @Test
  public void testGetTaskSummary() throws OdpsException, IOException {
    executor.run("select 1;", hints);
    System.out.println(executor.getSummary());
  }

  @Test
  public void testSync() throws OdpsException, IOException {
    executor.run("select 1;", hints);
    List<Record> result = executor.getResult();
    result.forEach(r -> System.out.println(r.toString()));

    System.out.println(executor.getSummary());
  }

  @Test
  public void testSyncByTunnel() throws OdpsException, IOException {
    tunnelExecutor.run("select 1;", hints);
    List<Record> result = tunnelExecutor.getResult();
    result.forEach(r -> System.out.println(r.toString()));

    System.out.println(tunnelExecutor.getSummary());
  }

  @Test
  public void testTpcdsQ4() throws OdpsException, IOException {
    executor.run("SELECT  dt.d_year\n"
                 + "        ,item.i_brand_id brand_id\n"
                 + "        ,item.i_brand brand\n"
                 + "        ,SUM(ss_sales_price) sum_agg\n"
                 + "FROM    date_dim dt\n"
                 + ",       store_sales\n"
                 + ",       item\n"
                 + "WHERE   dt.d_date_sk = store_sales.ss_sold_date_sk\n"
                 + "AND     store_sales.ss_item_sk = item.i_item_sk\n"
                 + "AND     item.i_manufact_id = 190\n"
                 + "AND     dt.d_moy = 12\n"
                 + "GROUP BY dt.d_year\n"
                 + "          ,item.i_brand\n"
                 + "          ,item.i_brand_id\n"
                 + "ORDER BY dt.d_year,sum_agg DESC,brand_id\n"
                 + "LIMIT   100\n"
                 + ";", hints);
    ResultSet resultSet = executor.getResultSet();
    resultSet.forEach(System.out::println);
    System.out.println(resultSet.getRecordCount());
    System.out.println(executor.getLogView());

    //System.out.println(executor.getInstance().getTaskDetailJson2(executor.getTaskName()));

    System.out.println(executor.getSummary());
  }

  @Test
  public void testTpcdsQ4UseTunnel() throws OdpsException, IOException {
    tunnelExecutor.run("SELECT  dt.d_year\n"
                 + "        ,item.i_brand_id brand_id\n"
                 + "        ,item.i_brand brand\n"
                 + "        ,SUM(ss_sales_price) sum_agg\n"
                 + "FROM    date_dim dt\n"
                 + ",       store_sales\n"
                 + ",       item\n"
                 + "WHERE   dt.d_date_sk = store_sales.ss_sold_date_sk\n"
                 + "AND     store_sales.ss_item_sk = item.i_item_sk\n"
                 + "AND     item.i_manufact_id = 190\n"
                 + "AND     dt.d_moy = 12\n"
                 + "GROUP BY dt.d_year\n"
                 + "          ,item.i_brand\n"
                 + "          ,item.i_brand_id\n"
                 + "ORDER BY dt.d_year,sum_agg DESC,brand_id\n"
                 + "LIMIT   100\n"
                 + ";", hints);
    ResultSet resultSet = tunnelExecutor.getResultSet(10L,10L, null);
    resultSet.forEach(System.out::println);
    System.out.println(resultSet.getRecordCount());
  }

  @Test
  public void generateLogview() throws OdpsException {
    try {
      executor.run("select 1;", hints);
      System.out.println(executor.getResult());
    } catch (Exception e) {

    }
    System.out.println(executor.getLogView());
  }

  @Test
  public void testReadResultFromInstanceTunnel() throws OdpsException, IOException {

    tunnelExecutor.run("select 1;", hints);
    System.out.println(tunnelExecutor.getResult());

  }

  @Test
  public void testTpcdsQuery72() throws OdpsException, IOException {
    String tpcdsQuery = "SELECT  i_item_desc\n"
               + "        ,w_warehouse_name\n"
               + "        ,d1.d_week_seq\n"
               + "        ,SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) no_promo\n"
               + "        ,SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) promo\n"
               + "        ,COUNT(*) total_cnt\n"
               + "FROM    catalog_sales\n"
               + "JOIN    inventory\n"
               + "ON      (\n"
               + "            cs_item_sk = inv_item_sk\n"
               + ")\n"
               + "JOIN    warehouse\n"
               + "ON      (\n"
               + "            w_warehouse_sk = inv_warehouse_sk\n"
               + ")\n"
               + "JOIN    item\n"
               + "ON      (\n"
               + "            i_item_sk = cs_item_sk\n"
               + ")\n"
               + "JOIN    customer_demographics\n"
               + "ON      (\n"
               + "            cs_bill_cdemo_sk = cd_demo_sk\n"
               + ")\n"
               + "JOIN    household_demographics\n"
               + "ON      (\n"
               + "            cs_bill_hdemo_sk = hd_demo_sk\n"
               + ")\n"
               + "JOIN    date_dim d1\n"
               + "ON      (\n"
               + "            cs_sold_date_sk = d1.d_date_sk\n"
               + ")\n"
               + "JOIN    date_dim d2\n"
               + "ON      (\n"
               + "            inv_date_sk = d2.d_date_sk\n"
               + ")\n"
               + "JOIN    date_dim d3\n"
               + "ON      (\n"
               + "            cs_ship_date_sk = d3.d_date_sk\n"
               + ")\n"
               + "LEFT OUTER JOIN promotion\n"
               + "ON      (\n"
               + "            cs_promo_sk = p_promo_sk\n"
               + ")\n"
               + "LEFT OUTER JOIN catalog_returns\n"
               + "ON      (\n"
               + "            cr_item_sk = cs_item_sk\n"
               + "            AND     cr_order_number = cs_order_number\n"
               + ")\n"
               + "WHERE   d1.d_week_seq = d2.d_week_seq\n"
               + "AND     inv_quantity_on_hand < cs_quantity\n"
               + "AND     d3.d_date > d1.d_date + 5\n"
               + "AND     hd_buy_potential = '1001-5000'\n"
               + "AND     d1.d_year = 1999\n"
               + "AND     cd_marital_status = 'W'\n"
               + "GROUP BY i_item_desc\n"
               + "          ,w_warehouse_name\n"
               + "          ,d1.d_week_seq\n"
               + "ORDER BY total_cnt DESC,i_item_desc,w_warehouse_name,d_week_seq\n"
               + "LIMIT   100\n"
               + ";";
    System.out.println(tpcdsQuery.replace("\n", " "));
    //System.out.println(tpcdsQuery);
    executor.run(tpcdsQuery, hints);
    System.out.println(executor.getLogView());
    ResultSet resultSet = executor.getResultSet();
    resultSet.forEach(System.out::println);
    System.out.println(resultSet.getRecordCount());
  }

  public static void main(String[] args) {
    String urlString = "http://11.158.199.208:9991/mcqa/projects/tpcds_10g_test3/instances?curr_project=tpcds_10g_test3";

    // 新的 TPC-DS 查询
    String tpcdsQuery = "SELECT  i_item_desc\n"
                        + "        ,w_warehouse_name\n"
                        + "        ,d1.d_week_seq\n"
                        + "        ,SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) no_promo\n"
                        + "        ,SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) promo\n"
                        + "        ,COUNT(*) total_cnt\n"
                        + "FROM    catalog_sales\n"
                        + "JOIN    inventory\n"
                        + "ON      (\n"
                        + "            cs_item_sk = inv_item_sk\n"
                        + ")\n"
                        + "JOIN    warehouse\n"
                        + "ON      (\n"
                        + "            w_warehouse_sk = inv_warehouse_sk\n"
                        + ")\n"
                        + "JOIN    item\n"
                        + "ON      (\n"
                        + "            i_item_sk = cs_item_sk\n"
                        + ")\n"
                        + "JOIN    customer_demographics\n"
                        + "ON      (\n"
                        + "            cs_bill_cdemo_sk = cd_demo_sk\n"
                        + ")\n"
                        + "JOIN    household_demographics\n"
                        + "ON      (\n"
                        + "            cs_bill_hdemo_sk = hd_demo_sk\n"
                        + ")\n"
                        + "JOIN    date_dim d1\n"
                        + "ON      (\n"
                        + "            cs_sold_date_sk = d1.d_date_sk\n"
                        + ")\n"
                        + "JOIN    date_dim d2\n"
                        + "ON      (\n"
                        + "            inv_date_sk = d2.d_date_sk\n"
                        + ")\n"
                        + "JOIN    date_dim d3\n"
                        + "ON      (\n"
                        + "            cs_ship_date_sk = d3.d_date_sk\n"
                        + ")\n"
                        + "LEFT OUTER JOIN promotion\n"
                        + "ON      (\n"
                        + "            cs_promo_sk = p_promo_sk\n"
                        + ")\n"
                        + "LEFT OUTER JOIN catalog_returns\n"
                        + "ON      (\n"
                        + "            cr_item_sk = cs_item_sk\n"
                        + "            AND     cr_order_number = cs_order_number\n"
                        + ")\n"
                        + "WHERE   d1.d_week_seq = d2.d_week_seq\n"
                        + "AND     inv_quantity_on_hand < cs_quantity\n"
                        + "AND     d3.d_date > d1.d_date + 5\n"
                        + "AND     hd_buy_potential = '1001-5000'\n"
                        + "AND     d1.d_year = 1999\n"
                        + "AND     cd_marital_status = 'W'\n"
                        + "GROUP BY i_item_desc\n"
                        + "          ,w_warehouse_name\n"
                        + "          ,d1.d_week_seq\n"
                        + "ORDER BY total_cnt DESC,i_item_desc,w_warehouse_name,d_week_seq\n"
                        + "LIMIT   100\n"
                        + ";";
    String xmlData = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
                     "<Instance><Job><Priority>9</Priority><Tasks>" +
                     "<SQL><Name>test_sql_task_112</Name><Config>" +
                     "<Property><Name>settings</Name><Value>{\"odps.service.mode\":\"all\"," +
                     "\"odps.sql.runtime.flag.executionengine_CheckJobPlanVersionInSmode\":\"false\"," +
                     "\"odps.sql.select.output.format\":\"HumanReadable\"}</Value></Property>" +
                     "</Config><Query>" + escapeXml(tpcdsQuery) + "</Query></SQL>" +
                     "</Tasks></Job></Instance>";

    HttpURLConnection connection = null;

    try {
      // 创建 URL 对象
      URL url = new URL(urlString);
      // 打开连接
      connection = (HttpURLConnection) url.openConnection();
      // 设置请求方法为 POST
      connection.setRequestMethod("POST");
      // 设置请求头
      connection.setRequestProperty("x-odps-request-account", "1365937150772213");
      // 指定请求体的类型
      connection.setRequestProperty("Content-Type", "application/xml");
      // 启用输入输出流
      connection.setDoOutput(true);


      connection.setDoInput(true);


      // 发送请求体
      try (OutputStream os = connection.getOutputStream()) {
        byte[] input = xmlData.getBytes("utf-8");
        os.write(input, 0, input.length);
      }

      // 检查响应码
      int responseCode = connection.getResponseCode();
      System.out.println("Response Code: " + responseCode);
      // 打印所有响应 headers
      Map<String, List<String>> headerFields = connection.getHeaderFields();
      for (Map.Entry<String, List<String>> header : headerFields.entrySet()) {
        // 打印 header 名字和对应的值
        System.out.println(header.getKey() + ": " + String.join(", ", header.getValue()));
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static String escapeXml(String xml) {
    return xml.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&apos;");
  }
}
