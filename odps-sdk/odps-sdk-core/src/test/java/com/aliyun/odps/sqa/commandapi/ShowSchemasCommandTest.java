package com.aliyun.odps.sqa.commandapi;

import java.util.HashMap;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.task.SQLTask;

public class ShowSchemasCommandTest extends TestBase {

  private static final String THREE_PROJECT_NAME = "three_pangu2_odps2";
  private static final String TEST_SCHEMA_NAME = "show_schemas_test_schema_name";

  @BeforeClass
  public static void createSchema() throws OdpsException {
    odps.setDefaultProject(THREE_PROJECT_NAME);

    if (odps.schemas().exists(TEST_SCHEMA_NAME)) {
      dropSchema();
    }
    HashMap<String, String> hints = new HashMap<>();
    hints.put("odps.namespace.schema", "true");
    hints.put("odps.default.schema", TEST_SCHEMA_NAME);

    odps.setCurrentSchema(TEST_SCHEMA_NAME);

    String createSchema = "create schema " + TEST_SCHEMA_NAME + " ;";
    Instance
        instance =
        SQLTask.run(odps, odps.getDefaultProject(), createSchema, hints, null);
    instance.waitForSuccess();
  }

  @AfterClass
  public static void deleteSchema() throws OdpsException {
    if (odps.schemas().exists(TEST_SCHEMA_NAME)) {
      dropSchema();
    }
  }

  private static void dropSchema() throws OdpsException {
    HashMap<String, String> hints = new HashMap<>();
    hints.put("odps.namespace.schema", "true");
    hints.put("odps.default.schema", TEST_SCHEMA_NAME);

    odps.setDefaultProject(THREE_PROJECT_NAME);
    odps.setCurrentSchema(TEST_SCHEMA_NAME);

    String createSchema = "drop schema " + TEST_SCHEMA_NAME + " ;";
    Instance
        instance =
        SQLTask.run(odps, odps.getDefaultProject(), createSchema, hints, null);
    instance.waitForSuccess();
  }

  @Test
  public void showSchemasCommandTest() {
    // 1. 测试语法解析是否可以通过
    String[] positiveCommands = new String[]{
        "show schemas like " + TEST_SCHEMA_NAME + " ;",
        "SHOW schemas IN " + THREE_PROJECT_NAME + " ;",
        "show SCHEMAs from " + THREE_PROJECT_NAME + " LIKE " + TEST_SCHEMA_NAME + " ;"
    };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(positiveCommands));

    String[] negativeCommands = new String[]{
        "show schema like " + TEST_SCHEMA_NAME + " ;",
        "SHOW schemas IN " + THREE_PROJECT_NAME + ".;",
        "show SCHEMAs from " + THREE_PROJECT_NAME + " LIKE ;"
    };
    Assert.assertEquals(negativeCommands.length, CommandTestUtil.getErrorNum(negativeCommands));

    // 2. 测试通过语法解析的正确的command是否能返回正确的结果
    List<Record> recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommands[0]);
    Assert.assertNotNull(recordList);
    Assert.assertEquals(recordList.size(), 1);
    Assert.assertEquals(recordList.get(0).get(0).toString().trim(), TEST_SCHEMA_NAME);

    recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommands[1]);
    Assert.assertNotNull(recordList);
    Assert.assertNotEquals(recordList.size(), 0);

    recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommands[2]);
    Assert.assertNotNull(recordList);
    Assert.assertEquals(recordList.size(), 1);
    Assert.assertEquals(recordList.get(0).get(0).toString().trim(), TEST_SCHEMA_NAME);

    ResultSet resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommands[0]);
    Assert.assertNotNull(resultSet);
    Assert.assertEquals(resultSet.next().get(0).toString().trim(), TEST_SCHEMA_NAME);

    resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommands[1]);
    Assert.assertNotNull(resultSet);

    resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommands[2]);
    Assert.assertNotNull(resultSet);
    Assert.assertEquals(resultSet.next().get(0).toString().trim(), TEST_SCHEMA_NAME);

    // 3. 测试通过语法解析的错误的command能否及时抛出异常退出
    String[] errorCommands = new String[]{
        "SHOW schemas in " + "TEST_PROJECT_NAME_NOT_EXIST" + " ;",
        };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(errorCommands));
    int num = 0;
    for (String errorCommand : errorCommands) {
      try {
        CommandTestUtil.runErrorCommand(odps, errorCommand);
      } catch (Exception e) {
        num++;
      }
    }
    Assert.assertEquals(errorCommands.length, num);

  }
}
