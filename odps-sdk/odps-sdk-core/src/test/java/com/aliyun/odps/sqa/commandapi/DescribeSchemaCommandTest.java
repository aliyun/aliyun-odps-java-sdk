package com.aliyun.odps.sqa.commandapi;

import java.util.HashMap;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.task.SQLTask;

public class DescribeSchemaCommandTest extends TestBase {

  private static final String THREE_PROJECT_NAME = "three_pangu2_odps2";
  private static final String TEST_SCHEMA_NAME = "desc_schemas_test_schema_name";

  @BeforeClass
  public static void createSchema() throws OdpsException {
    Assume.assumeTrue(odps.projects().exists(THREE_PROJECT_NAME));
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
  public void descSchemaCommandTest() {
    String[] positiveCommands = new String[]{
        "desc schema " + TEST_SCHEMA_NAME + " ;",
        "DESC schema " + TEST_SCHEMA_NAME + " ;",
        "describe schema " + THREE_PROJECT_NAME + "." + TEST_SCHEMA_NAME + " ;"
    };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(positiveCommands));

    String[] negativeCommands = new String[]{
        "desc schemas " + TEST_SCHEMA_NAME + " ;",
        "descr " + TEST_SCHEMA_NAME + " ;",
        "describe schema" + THREE_PROJECT_NAME + "." + " ;"
    };
    Assert.assertEquals(negativeCommands.length, CommandTestUtil.getErrorNum(negativeCommands));

    int numOfNull = 0;
    for (String positiveCommand : positiveCommands) {
      List<Record> recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommand);
      if (recordList == null || recordList.size() == 0) {
        numOfNull++;
        continue;
      }

      for (Record record : recordList) {
        Assert.assertNotEquals(record.get(0).toString().trim(), "");
        Assert.assertNotNull(record.get(0));
      }
    }
    Assert.assertEquals(numOfNull, 0);

    numOfNull = 0;
    for (String positiveCommand : positiveCommands) {
      ResultSet resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommand);
      if (resultSet == null) {
        numOfNull++;
        continue;
      }
      while (resultSet.hasNext()) {
        Record record = resultSet.next();
        Assert.assertNotEquals(record.get(0).toString().trim(), "");
        Assert.assertNotNull(record.get(0));
      }
    }
    Assert.assertEquals(numOfNull, 0);

    String[] errorCommands = new String[]{
        "desc schema " + "TEST_SCHEMA_NAME_NOT_EXIST" + " ;"
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
