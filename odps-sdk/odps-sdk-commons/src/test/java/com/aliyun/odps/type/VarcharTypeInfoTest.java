package com.aliyun.odps.type;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhenhong.gzh on 16/7/12.
 */
public class VarcharTypeInfoTest {
  @Test
  public void testVarcharTypeInfoName() {
    int [] length = new int [] {1, VarcharTypeInfo.MAX_VARCHAR_LENGTH - 10, VarcharTypeInfo.MAX_VARCHAR_LENGTH};

    for (int i : length) {
      VarcharTypeInfo type = new VarcharTypeInfo(i);
      Assert.assertEquals(String.format("VARCHAR(%s)", i), type.getTypeName());
    }
  }

  @Test
  public void testVarcharTypeLength() {
    VarcharTypeInfo type = new VarcharTypeInfo(1);
    Assert.assertEquals(1, type.getLength());

    type = TypeInfoFactory.getVarcharTypeInfo(10);
    Assert.assertEquals(10, type.getLength());
  }

  @Test
  public void testVarcharTypeIllegalParameters() {
    int count = 0;
    int [] illegalLength = new int[] {0, -10, VarcharTypeInfo.MAX_VARCHAR_LENGTH + 1};

    for (int i : illegalLength) {
      try {
        VarcharTypeInfo type = new VarcharTypeInfo(i);
      } catch (IllegalArgumentException e) {
        count++;
      }
    }

    Assert.assertEquals(illegalLength.length, count);
  }
}
