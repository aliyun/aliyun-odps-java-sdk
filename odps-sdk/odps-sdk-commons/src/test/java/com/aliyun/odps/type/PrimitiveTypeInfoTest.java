package com.aliyun.odps.type;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsType;

/**
 * Created by zhenhong.gzh on 16/7/11.
 */
public class PrimitiveTypeInfoTest {
  @Test
  public void testTypeName() {
    Assert.assertEquals(OdpsType.VOID.name(), TypeInfoFactory.VOID.getTypeName());
    Assert.assertEquals(OdpsType.BOOLEAN.name(), TypeInfoFactory.BOOLEAN.getTypeName());
    Assert.assertEquals(OdpsType.TINYINT.name(), TypeInfoFactory.TINYINT.getTypeName());
    Assert.assertEquals(OdpsType.SMALLINT.name(), TypeInfoFactory.SMALLINT.getTypeName());
    Assert.assertEquals(OdpsType.INT.name(), TypeInfoFactory.INT.getTypeName());
    Assert.assertEquals(OdpsType.BIGINT.name(), TypeInfoFactory.BIGINT.getTypeName());
    Assert.assertEquals(OdpsType.FLOAT.name(), TypeInfoFactory.FLOAT.getTypeName());
    Assert.assertEquals(OdpsType.DOUBLE.name(), TypeInfoFactory.DOUBLE.getTypeName());
    Assert.assertEquals(OdpsType.STRING.name(), TypeInfoFactory.STRING.getTypeName());
    Assert.assertEquals(OdpsType.DATE.name(), TypeInfoFactory.DATE.getTypeName());
    Assert.assertEquals(OdpsType.DATETIME.name(), TypeInfoFactory.DATETIME.getTypeName());
    Assert.assertEquals(OdpsType.TIMESTAMP.name(), TypeInfoFactory.TIMESTAMP.getTypeName());
    Assert.assertEquals(OdpsType.BINARY.name(), TypeInfoFactory.BINARY.getTypeName());
    Assert.assertEquals(OdpsType.INTERVAL_DAY_TIME.name(), TypeInfoFactory.INTERVAL_DAY_TIME.getTypeName());
    Assert.assertEquals(OdpsType.INTERVAL_YEAR_MONTH.name(), TypeInfoFactory.INTERVAL_YEAR_MONTH.getTypeName());
  }
}
