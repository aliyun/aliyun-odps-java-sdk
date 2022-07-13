package com.aliyun.odps;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.BeforeClass;

import junit.framework.TestCase;

public class SchemasTest extends TestCase {

  private static final String DEFAULT_TEST_SCHEMA = "java_sdk_schemas_test";
  private static final String NON_EXIST_SCHEMA = "non_exist_schema";
  private static Schemas schemas;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // TODO get schemas by three-tier odps project
    // wait for test project update to three-tier
    // create default test schema
  }

  public void testExists() {
    Assert.assertFalse(schemas.exists(NON_EXIST_SCHEMA));
    Assert.assertTrue(schemas.exists(DEFAULT_TEST_SCHEMA));
  }

  public void testExistsInOtherProject() {
    //TODO
  }

  public void testIterator() {
    Iterator<Schema> it = schemas.iterator();
    while (it.hasNext()) {
      Schema schema = it.next();
      Assert.assertNotNull(schema);
      Assert.assertNotNull(schema.getName());
    }
  }

  public void testIterable() {
    for (Schema schema : schemas) {
      Assert.assertNotNull(schema);
      Assert.assertNotNull(schema.getName());
    }
  }

}