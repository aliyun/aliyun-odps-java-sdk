package com.aliyun.odps;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.expression.TruncTime;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TableCreatorTest {

  Odps odps = OdpsTestUtils.newDefaultOdps();

  @Test
  public void testCreateAutoPartitionTable() throws OdpsException {
    Column autoPar = Column.newBuilder("p1", TypeInfoFactory.STRING)
        .withGenerateExpression(new TruncTime("c1", TruncTime.DatePart.DAY))
        .build();
    TableSchema
        schema =
        TableSchema.builder().withPartitionColumn(autoPar)
            .withColumn(Column.newBuilder("c1", TypeInfoFactory.DATETIME).build()).build();
    String
        sql =
        odps.tables().newTableCreator("testProject", "testCreateAutoPartitionTable", schema)
            .ifNotExists().getSQL();
    Assert.assertEquals(
        "CREATE TABLE IF NOT EXISTS `testProject`.`testCreateAutoPartitionTable` (`c1` DATETIME) AUTO PARTITIONED BY (trunc_time(c1, 'DAY') AS `p1`);",
        sql);
  }

  @Test
  public void testCreateStructTable() throws OdpsException {
    TableSchema
        schema =
        TableSchema.builder().withColumn(Column.newBuilder("c1", TypeInfoFactory.getStructTypeInfo(
            ImmutableList.of("end"), ImmutableList.of(TypeInfoFactory.STRING))).build()).build();
    String sql =
        odps.tables().newTableCreator("testProject", "testCreateStructTable", schema).ifNotExists()
            .getSQL();
    String
        expect =
        "CREATE TABLE IF NOT EXISTS `testProject`.`testCreateStructTable` (`c1` STRUCT<`end`:STRING>);";

    Assert.assertEquals(expect, sql);
  }

  @Test
  public void testCreateClusterTable() {
    Table.ClusterInfo
        clusterInfo =
        new Table.ClusterInfo(Table.ClusterInfo.ClusterType.HASH, ImmutableList.of("c1", "c2"),
                              ImmutableList.of(
                                  new Table.SortColumn("c1", Table.SortColumn.Order.DESC),
                                  new Table.SortColumn("c2", Table.SortColumn.Order.ASC)), 10);

    String sql = odps.tables().newTableCreator("testProject", "testCreateClusterTable",
                                               TableSchema.builder().withColumn(
                                                       Column.newBuilder("c1", TypeInfoFactory.STRING)
                                                           .build())
                                                   .withColumn(
                                                       Column.newBuilder("c2",
                                                                         TypeInfoFactory.STRING)
                                                           .build())
                                                   .build())
        .withClusterInfo(clusterInfo).getSQL();
    System.out.println(sql);
    String
        expect =
        "CREATE TABLE `testProject`.`testCreateClusterTable` (`c1` STRING,`c2` STRING) CLUSTERED BY (`c1`, `c2`) SORTED BY (`c1` DESC, `c2` ASC) INTO 10 BUCKETS;";
    Assert.assertEquals(expect, sql);
  }

  @Test
  public void testCreateExternalTable() {
    String sql =
        odps.tables().newTableCreator("teseProject", "testCreateExternalTable",
                                      TableSchema.builder().withColumn(
                                              Column.newBuilder("c1", TypeInfoFactory.STRING)
                                                  .build())
                                          .withColumn(
                                              Column.newBuilder("c2",
                                                                TypeInfoFactory.STRING)
                                                  .build())
                                          .build()).externalTable()
            .withStorageHandler("com.aliyun.odps.udf.example.text.TextStorageHandler")
            .withResources(ImmutableList.of("odps-udf-example.jar", "another.jar"))
            .withLocation("MOCKoss://full/uri/path/to/oss/directory/")
            .withSerdeProperties(
                ImmutableMap.of("odps.text.option.delimiter", "|", "my.own.option", "value"))
            .getSQL();

    System.out.println(sql);

    String
        expect =
        "CREATE EXTERNAL TABLE `teseProject`.`testCreateExternalTable` (`c1` STRING,`c2` STRING) STORED BY 'com.aliyun.odps.udf.example.text.TextStorageHandler' WITH SERDEPROPERTIES ('odps.text.option.delimiter'='|' , 'my.own.option'='value') LOCATION 'MOCKoss://full/uri/path/to/oss/directory/' USING 'odps-udf-example.jar,another.jar';";
    Assert.assertEquals(expect, sql);
  }
}
