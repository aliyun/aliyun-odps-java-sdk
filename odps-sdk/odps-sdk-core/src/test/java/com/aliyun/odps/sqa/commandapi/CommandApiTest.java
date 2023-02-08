package com.aliyun.odps.sqa.commandapi;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.type.TypeInfoFactory;

public class CommandApiTest extends TestBase {

  private static final String TEST_TABLE_NAME = "sqlExecutor_non_commandapi_test";

  @BeforeClass
  public static void createTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("col2", TypeInfoFactory.STRING));
    odps.tables().create(TEST_TABLE_NAME, schema);
  }

  @AfterClass
  public static void deleteTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }
  }

  @Test
  public void commandApiTest() throws OdpsException, IOException {
    SQLExecutorBuilder sqlExecutorBuilder = SQLExecutorBuilder.builder();
    SQLExecutor
        sqlExecutor =
        sqlExecutorBuilder.odps(odps).executeMode(ExecuteMode.INTERACTIVE).enableCommandApi(true)
            .build();

    sqlExecutor.run("desc " + TEST_TABLE_NAME + ";", null);
    Assert.assertEquals(sqlExecutor.getResult().get(0).get(1), odps.getDefaultProject());

    Instance
        instance =
        SQLTask.run(odps, "insert into " + TEST_TABLE_NAME + " VALUES(1234, 'test');");
    instance.waitForSuccess();

    sqlExecutor.run("select * from " + TEST_TABLE_NAME + ";", null);
    Assert.assertEquals(sqlExecutor.getResult().get(0).get(0), 1234L);
    Assert.assertEquals(sqlExecutor.getResult().get(0).getString(1), "test");
  }

}
