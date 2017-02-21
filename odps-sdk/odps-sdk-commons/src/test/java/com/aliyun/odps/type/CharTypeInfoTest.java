package com.aliyun.odps.type;

import org.junit.Test;

import org.junit.Assert;

/**
 * Created by zhenhong.gzh on 16/7/12.
 */
public class CharTypeInfoTest {
  @Test
  public void testCharTypeInfoName() {
    int [] length = new int [] {1, CharTypeInfo.MAX_CHAR_LENGTH - 10, CharTypeInfo.MAX_CHAR_LENGTH};

    for (int i : length) {
      CharTypeInfo type = new CharTypeInfo(i);
      Assert.assertEquals(String.format("CHAR(%s)", i), type.getTypeName());
    }
  }

  @Test
  public void testCharTypeLength() {
    CharTypeInfo type = new CharTypeInfo(1);
    Assert.assertEquals(1, type.getLength());

    type = TypeInfoFactory.getCharTypeInfo(10);
    Assert.assertEquals(10, type.getLength());
  }

  @Test
  public void testCharTypeIllegalParameters() {
    int count = 0;
    int [] illegalLength = new int[] {0, -10, CharTypeInfo.MAX_CHAR_LENGTH + 1};

    for (int i : illegalLength) {
      try {
       CharTypeInfo type = new CharTypeInfo(i);
      } catch (IllegalArgumentException e) {
        count++;
      }
    }

    Assert.assertEquals(illegalLength.length, count);
  }
}
