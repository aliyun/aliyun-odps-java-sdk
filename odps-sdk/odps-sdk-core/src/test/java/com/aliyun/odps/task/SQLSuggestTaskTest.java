package com.aliyun.odps.task;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.type.TypeInfoFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SQLSuggestTaskTest extends TestBase {

  @BeforeClass
  public static void init() throws OdpsException {
    System.out.println("SQLSuggestTaskTest init");
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("id", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("name", TypeInfoFactory.STRING));
    odps.tables().create("sql_suggest_src", schema, true);
    odps.tables().create("sql_suggest_dst", schema, true);
  }



  @AfterClass
  public static void clean() throws OdpsException {
    System.out.println("SQLSuggestTaskTest clean");
    TestBase.odps.tables().delete("sql_suggest_src", true);
    TestBase.odps.tables().delete("sql_suggest_dst", true);
  }



}
