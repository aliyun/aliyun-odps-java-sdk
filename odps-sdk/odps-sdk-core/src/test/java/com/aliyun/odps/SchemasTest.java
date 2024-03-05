package com.aliyun.odps;

import java.util.Iterator;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;

public class SchemasTest {

  private static final String TEST_PROJECT_1 = OdpsTestUtils.getSchemaProject1();
  private static final String TEST_PROJECT_2 = OdpsTestUtils.getSchemaProject2();
  private static final String TEST_SCHEMA_IN_PROJECT_1 = "javasdk_schema1";
  private static final String TEST_SCHEMA_ITERATOR_IN_PROJECT_1 = "javasdk_schema_iterator1";
  private static final String TEST_SCHEMA_ITERATOR_IN_PROJECT_2 = "javasdk_schema_iterator2";
  private static final String TEST_SCHEMA_IN_PROJECT_2 = "javasdk_schema2";
  private static final String NON_EXIST_SCHEMA = "non_exist_schema";
  private static final String COMMENT1 = "comment1";
  private static final String COMMENT2 = "comment1";
  private static Schemas schemas;

  @BeforeClass
  public static void setup() throws Exception {
    Odps odps = OdpsTestUtils.newSchemaOdps();
    // make sure all projects are created
    Assume.assumeTrue(odps.projects().exists(odps.getDefaultProject()));
    Assume.assumeTrue(odps.projects().exists(TEST_PROJECT_1));
    Assume.assumeTrue(odps.projects().exists(TEST_PROJECT_2));

    schemas = odps.schemas();
    clean();
    schemas.create(TEST_PROJECT_1, TEST_SCHEMA_ITERATOR_IN_PROJECT_1, COMMENT1, true);
    schemas.create(TEST_PROJECT_2, TEST_SCHEMA_ITERATOR_IN_PROJECT_2, COMMENT2, true);
  }

  @AfterClass
  public static void clean() throws OdpsException, InterruptedException {
    if (schemas == null) {
      return;
    }
    deleteIfExists(TEST_PROJECT_1, TEST_SCHEMA_IN_PROJECT_1);
    deleteIfExists(TEST_PROJECT_1, TEST_SCHEMA_ITERATOR_IN_PROJECT_1);
    deleteIfExists(TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2);
    deleteIfExists(TEST_PROJECT_2, TEST_SCHEMA_ITERATOR_IN_PROJECT_2);
  }

  @Test
  public void testGetSingleSchema() {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    String projectName = TEST_PROJECT_1;
    odps.setDefaultProject(projectName);
    Schema s = odps.schemas().get(TEST_SCHEMA_ITERATOR_IN_PROJECT_1);

    Assert.assertEquals(TEST_SCHEMA_ITERATOR_IN_PROJECT_1, s.getName());
    Assert.assertEquals(Schema.SchemaType.MANAGED.name(), s.getType());
    Assert.assertEquals(projectName, s.getProjectName());
    Assert.assertEquals("comment1", s.getComment());
    Assert.assertNotNull(s.getOwner());
    Assert.assertNotNull(s.getModifiedTime());
    Assert.assertNotNull(s.getCreateTime());

    System.out.println(s.getName());
    System.out.println(s.getType());
    System.out.println(s.getComment());
    System.out.println(s.getCreateTime());
    System.out.println(s.getModifiedTime());
    System.out.println(s.getOwner());
    System.out.println(s.getProjectName());

  }

  @Test
  public void testSchemaFilter() throws OdpsException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    // 3-tier project
    String projectName = TEST_PROJECT_1;
    odps.setDefaultProject(projectName);
    String schemaName = "test_schema_name";
    try {
      odps.schemas().create(schemaName);
    } catch (OdpsException e) {

    }

    SchemaFilter filter = new SchemaFilter();
    filter.setName(schemaName);
    filter.setOwner(OdpsTestUtils.getDefaultOwner());

    Iterator<Schema> iterator = odps.schemas().iterator(odps.getDefaultProject(), filter);
    List<Schema> schemaList = ((ListIterator<Schema>) iterator).list();
    for (Schema schema : schemaList) {
      Assert.assertEquals(schema.getName(), schemaName);
      Assert.assertEquals(schema.getProjectName(), projectName);
      Assert.assertEquals(schema.getOwner(), OdpsTestUtils.getDefaultOwner());
    }
    odps.schemas().delete(schemaName);
  }

  /**
   * test create / get / exists / delete
   */
  @Test
  public void testCRUD() throws OdpsException, InterruptedException {
    // test CRUD in default project
    schemas.create(TEST_SCHEMA_IN_PROJECT_1);
    testSchema(schemas.get(TEST_SCHEMA_IN_PROJECT_1), TEST_PROJECT_1, TEST_SCHEMA_IN_PROJECT_1, "");
    schemas.delete(TEST_SCHEMA_IN_PROJECT_1);
    Thread.sleep(1000);
    Assert.assertFalse(schemas.exists(TEST_SCHEMA_IN_PROJECT_1));


    // test CRUD in other project
    Assert.assertFalse(schemas.exists(TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2));
    schemas.create(TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2);
    Assert.assertTrue(schemas.exists(TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2));
    testSchema(schemas.get(TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2),
               TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2, "");
    schemas.delete(TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2);
    Thread.sleep(1000);
    Assert.assertFalse(schemas.exists(TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2));

    // test create all args
    schemas.create(TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2, COMMENT1, false);
    schemas.create(TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2, COMMENT2, true);
    testSchema(schemas.get(TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2),
               TEST_PROJECT_2, TEST_SCHEMA_IN_PROJECT_2, COMMENT1);

    schemas.create(TEST_PROJECT_1, TEST_SCHEMA_IN_PROJECT_1, "comment", false);
    schemas.create(TEST_PROJECT_1, TEST_SCHEMA_IN_PROJECT_1, "cc", true);

    Assert.assertFalse(schemas.exists(NON_EXIST_SCHEMA));

    try {
      schemas.get(NON_EXIST_SCHEMA).getComment();
      assert false;
    } catch (ReloadException e) {
      System.out.println(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("Schema " + NON_EXIST_SCHEMA + " does not exist"));
    }

    try {
      schemas.delete(NON_EXIST_SCHEMA);
      Thread.sleep(1000);
      assert false;
    } catch (NoSuchObjectException ignore) {
    }
  }

  @Test
  public void testSchemaListIterator() throws Exception{
    Schemas schemas = OdpsTestUtils.newDefaultOdps().schemas();
    SchemaFilter filter = new SchemaFilter();
    filter.setName("schema");
    Iterator<Schema> iterator = schemas.iterator("odps_ywm_test", filter);
    ListIterator<Schema> schemaListIterator = (ListIterator<Schema>) iterator;
    String marker = null;
    long maxItemSize = 10;
    List<Schema> schemaList = schemaListIterator.list(marker, maxItemSize);
    Assert.assertFalse(schemaList.isEmpty());

    marker = schemaListIterator.getMarker();
    Assert.assertNotEquals(marker, null);
  }

  @Test
  public void testIterator() {
    Iterator<Schema> iterator = schemas.iterator();
    boolean find = false;
    while (iterator.hasNext()) {
      Schema schema = iterator.next();
      if (TEST_SCHEMA_ITERATOR_IN_PROJECT_1.equals(schema.getName())) {
        testSchema(schema, TEST_PROJECT_1, TEST_SCHEMA_ITERATOR_IN_PROJECT_1, COMMENT1);
        find = true;
        break;
      }
    }

    Assert.assertTrue(find);

    find = false;
    iterator = schemas.iterator(TEST_PROJECT_2);
    while (iterator.hasNext()) {
      Schema schema = iterator.next();
      if (TEST_SCHEMA_ITERATOR_IN_PROJECT_2.equals(schema.getName())) {
        testSchema(schema, TEST_PROJECT_2, TEST_SCHEMA_ITERATOR_IN_PROJECT_2, COMMENT2);
        find = true;
        break;
      }
    }
    Assert.assertTrue(find);
  }

  @Test
  public void testIterable() {
    boolean find = false;
    for (Schema schema: schemas) {
      if (TEST_SCHEMA_ITERATOR_IN_PROJECT_1.equals(schema.getName())) {
        testSchema(schema, TEST_PROJECT_1, TEST_SCHEMA_ITERATOR_IN_PROJECT_1, COMMENT1);
        find = true;
      }
    }

    Assert.assertTrue(find);

    find = false;
    Iterable<Schema> iterable = schemas.iterable(TEST_PROJECT_2);
    for (Schema schema: iterable) {
      if (TEST_SCHEMA_ITERATOR_IN_PROJECT_2.equals(schema.getName())) {
        testSchema(schema, TEST_PROJECT_2, TEST_SCHEMA_ITERATOR_IN_PROJECT_2, COMMENT2);
        find = true;
        break;
      }
    }
    Assert.assertTrue(find);
  }

  private void testSchema(Schema schema, String projectName, String schemaName, String comment) {
    Assert.assertEquals(comment, schema.getComment());
    Assert.assertEquals(projectName, schema.getProjectName());
    Assert.assertEquals(schemaName, schema.getName());
  }

  private static void deleteIfExists(String project, String schema) throws OdpsException, InterruptedException {
    Thread.sleep(1000);
    if (schemas.exists(project, schema)) {
      schemas.delete(project, schema);
      Thread.sleep(1000);
    }
  }

}