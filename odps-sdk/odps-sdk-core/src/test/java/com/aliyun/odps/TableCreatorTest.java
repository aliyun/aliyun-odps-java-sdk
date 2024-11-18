package com.aliyun.odps;

import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableList;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TableCreatorTest {

  @Test
  public void testCreateStructTable() throws OdpsException {
    TableSchema
        schema =
        TableSchema.builder().withColumn(Column.newBuilder("c1", TypeInfoFactory.getStructTypeInfo(
            ImmutableList.of("end"), ImmutableList.of(TypeInfoFactory.STRING))).build()).build();

    Odps odps = OdpsTestUtils.newDefaultOdps();
    odps.tables().newTableCreator("testCreateStructTable", schema).debug().ifNotExists().create();
  }
}
