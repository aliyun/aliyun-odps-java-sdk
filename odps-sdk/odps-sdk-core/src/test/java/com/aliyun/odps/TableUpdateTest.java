package com.aliyun.odps;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableList;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TableUpdateTest {

  private Table testTable;


  @Test
  public void testSetLifeCycle() throws Exception {
    testTable.setLifeCycle(10);
    long life = testTable.getLife();
    Assert.assertEquals(10L, life);
  }

  @Test
  public void testChangeComment() throws Exception {
    testTable.changeComment("test comment");
    Assert.assertEquals("test comment", testTable.getComment());
  }

  @Test
  public void testTouch() throws Exception {
    testTable.touch();
  }

  @Test
  public void testChangeClusterInfo() throws Exception {
    Table.ClusterInfo
        expectClusterInfo =
        new Table.ClusterInfo(Table.ClusterInfo.ClusterType.HASH, ImmutableList.of("c1", "c2"),
                              ImmutableList.of(
                                  new Table.SortColumn("c1", Table.SortColumn.Order.DESC),
                                  new Table.SortColumn("c2", Table.SortColumn.Order.ASC)), 10);
    testTable.changeClusterInfo(expectClusterInfo);
    Table.ClusterInfo actualClusterInfo = testTable.getClusterInfo();

    Assert.assertEquals(expectClusterInfo.toString(), actualClusterInfo.toString());
  }

  @Test
  public void testRename() throws Exception {
    testTable.rename("new_sdk_TableUpdateTest");
    Assert.assertEquals("new_sdk_TableUpdateTest", testTable.getName());
    testTable.reload();

    testTable.rename("sdk_TableUpdateTest");
  }


  @Test
  public void testAddColumns() throws Exception {
    Column newColumn = Column.newBuilder("c3", TypeInfoFactory.STRING).build();
    Column newColumn2 = Column.newBuilder("c4", TypeInfoFactory.STRING).build();
    testTable.addColumns(ImmutableList.of(newColumn, newColumn2), true);

    TableSchema schema = testTable.getSchema();
    Assert.assertEquals(4, schema.getColumns().size());
  }


  @Test
  public void testDropColumns() throws Exception {
    testTable.dropColumns(ImmutableList.of("c2"));
    TableSchema schema = testTable.getSchema();
    Assert.assertEquals(1, schema.getColumns().size());
  }

  @Test
  public void testAlterColumnType() throws Exception {
    testTable.alterColumnType("c2", TypeInfoFactory.STRING);
    TableSchema schema = testTable.getSchema();
    Assert.assertEquals(TypeInfoFactory.STRING, schema.getColumn("c2").getTypeInfo());

    try {
      testTable.alterColumnType("c1", TypeInfoFactory.BIGINT);
    } catch (OdpsException e) {
      System.out.println(e.getMessage());
      // ODPS-0130071:[1,89] Semantic analysis exception - changing column data type from STRING to BIGINT is not supported.
      if (e.getMessage().contains("ODPS-0130071")) {
        System.out.println("ok");
        return;
      } else {
        throw e;
      }
    }
    schema = testTable.getSchema();
    Assert.assertEquals(TypeInfoFactory.BIGINT, schema.getColumn("c1").getTypeInfo());
  }

  @Test
  public void changeColumnName() throws Exception {
    testTable.changeColumnName("c1", "c3");
    TableSchema schema = testTable.getSchema();
    Assert.assertEquals("c3", schema.getColumn("c3").getName());
  }

  @Before
  public void initTestTable() throws OdpsException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    TableSchema schema = TableSchema.builder()
        .withStringColumn("c1")
        .withBigintColumn("c2")
        .withPartitionColumn(new Column("p1", TypeInfoFactory.STRING))
        .build();

    odps.tables().newTableCreator("sdk_TableUpdateTest", schema)
        .create();
    testTable = odps.tables().get("sdk_TableUpdateTest");
  }

  @After
  public void dropTestTable() throws OdpsException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    odps.tables().delete("sdk_TableUpdateTest", true);
  }
}
