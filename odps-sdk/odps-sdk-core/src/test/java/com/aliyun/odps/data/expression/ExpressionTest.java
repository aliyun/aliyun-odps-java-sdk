package com.aliyun.odps.data.expression;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.task.SQLTask;
import com.google.common.collect.ImmutableMap;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ExpressionTest {

  private static Odps odps;

  @BeforeClass
  public static void setUp() throws OdpsException {
    odps = OdpsTestUtils.newDefaultOdps();
    SQLTask.run(odps, odps.getDefaultProject(),
                "create table if not exists auto_pt(a bigint, d timestamp) auto partitioned by (trunc_time(d, 'day') as part_value);",
                ImmutableMap.of("odps.sql.type.system.odps2", "true"),
                null
    ).waitForSuccess();
  }

  @Test
  public void getTableTest() {
    TableSchema schema = odps.tables().get("auto_pt").getSchema();
    Column ptColumn = schema.getPartitionColumns().get(0);
    System.out.println(ptColumn.getGenerateExpression());
    Assert.assertNotNull(ptColumn.getGenerateExpression());
  }
}
