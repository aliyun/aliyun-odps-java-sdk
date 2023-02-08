package com.aliyun.odps.sqa.commandapi;

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

public class ShowCreateTableCommandTest extends TestBase {

  private static final String TEST_TABLE_NAME = "show_create_table_test_name";

  @BeforeClass
  public static void createTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }

    String
        sql =
        String.format("create table if not exists %s(id int, name string);", TEST_TABLE_NAME);
    Instance
        instance =
        SQLTask.run(odps, odps.getDefaultProject(), sql, null, null);
    instance.waitForSuccess();
  }

  @AfterClass
  public static void deleteTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }
  }

  @Test
  public void descSchemaCommandTest() {
    String[] positiveCommands = new String[]{
        "show create table " + TEST_TABLE_NAME + " ;",
        "SHOW create table " + odps.getDefaultProject() + "." + TEST_TABLE_NAME + " ;",
        };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(positiveCommands));

    String[] negativeCommands = new String[]{
        "show create tables " + TEST_TABLE_NAME + " ;",
        "SHOW create " + odps.getDefaultProject() + "." + TEST_TABLE_NAME + " ;",
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
        "show create table " + "TEST_TABLE_NAME_NOT_EXIST" + " ;"
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
