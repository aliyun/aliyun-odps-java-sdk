package com.aliyun.odps.type;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhenhong.gzh on 16/7/12.
 */
public class DecimalTypeInfoTest {
  @Test
  public void testGetTypeInfoName() {
    DecimalTypeInfo decimalTypeInfo = TypeInfoFactory.getDecimalTypeInfo(10, 2);
    Assert.assertEquals("DECIMAL(10,2)", decimalTypeInfo.getTypeName());

    decimalTypeInfo = TypeInfoFactory.getDecimalTypeInfo(DecimalTypeInfo.DEFAULT_PRECISION,
                                                         DecimalTypeInfo.DEFAULT_SCALE);
    Assert.assertEquals("DECIMAL", decimalTypeInfo.getTypeName());
  }

  @Test
  public void testDefaultDecimal() {
    DecimalTypeInfo decimalTypeInfo = TypeInfoFactory.DECIMAL;

    Assert.assertEquals("DECIMAL", decimalTypeInfo.getTypeName());
    Assert.assertEquals(DecimalTypeInfo.DEFAULT_PRECISION, decimalTypeInfo.getPrecision());
    Assert.assertEquals(DecimalTypeInfo.DEFAULT_SCALE, decimalTypeInfo.getScale());
  }

  @Test
  public void testDecimalPrecisionAndScale() {
    DecimalTypeInfo decimalTypeInfo = TypeInfoFactory.getDecimalTypeInfo(DecimalTypeInfo.DEFAULT_PRECISION, DecimalTypeInfo.DEFAULT_SCALE);
    Assert.assertEquals(DecimalTypeInfo.DEFAULT_PRECISION, decimalTypeInfo.getPrecision());
    Assert.assertEquals(DecimalTypeInfo.DEFAULT_SCALE, decimalTypeInfo.getScale());

    decimalTypeInfo = TypeInfoFactory.getDecimalTypeInfo(1, 0);
    Assert.assertEquals(1, decimalTypeInfo.getPrecision());
    Assert.assertEquals(0, decimalTypeInfo.getScale());
  }

  @Test
  public void testDecimalTypeIllegalParameters() {
    // precision illegal
    // scale illegal
    // scale larger than precision, illegal
    int [][] params = {{DecimalTypeInfo.DEFAULT_PRECISION, -1}, {0, 0}, {0, DecimalTypeInfo.DEFAULT_SCALE}, {2, -1}, {3, 4}};

    int count = 0;


    for (int [] pair : params) {
      try {
        TypeInfoFactory.getDecimalTypeInfo(pair[0], pair[1]);
      } catch (IllegalArgumentException e) {
        count ++;
      }
    }

    Assert.assertEquals(params.length, count);
  }
}
