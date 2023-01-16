package com.aliyun.odps;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.task.SQLTask;

public class SchemasTest {

  private static final String DEFAULT_TEST_SCHEMA = "java_sdk_schemas_test";
  private static final String NON_EXIST_SCHEMA = "non_exist_schema";
  private static Schemas schemas;
  private static Odps odps;

  @BeforeClass
  public static void setup() throws Exception {
    odps = OdpsTestUtils.newSchemaOdps();
    schemas = odps.schemas();
    Instance i = SQLTask.run(odps, "create schema if not exists " + DEFAULT_TEST_SCHEMA + ";");
    i.waitForSuccess();
  }

  @Test
  public void testExists() {
    Assert.assertFalse(schemas.exists(NON_EXIST_SCHEMA));
    Assert.assertTrue(schemas.exists(DEFAULT_TEST_SCHEMA));
  }

  @Test
  public void testExistsInOtherProject() {
    //TODO
  }

  @Test
  public void testIterator() {
    Iterator<Schema> it = schemas.iterator();
    while (it.hasNext()) {
      Schema schema = it.next();
      Assert.assertNotNull(schema);
      Assert.assertNotNull(schema.getName());
      break;
    }
  }

  @Test
  public void testIterable() {
    for (Schema schema : schemas) {
      Assert.assertNotNull(schema);
      Assert.assertNotNull(schema.getName());
      break;
    }
  }

}