package com.aliyun.odps.type;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhenhong.gzh on 16/7/12.
 */
public class StructTypeInfoTest {

  @Test
  public void testName() {
    String [] names = {"name", "age", "parents"};
    TypeInfo [] infos = {TypeInfoFactory.STRING, TypeInfoFactory.BIGINT, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getCharTypeInfo(
        20))};
    StructTypeInfo structTypeInfo = TypeInfoFactory.getStructTypeInfo(Arrays.asList(names),
                                                                      Arrays.asList(infos));

    Assert.assertArrayEquals(names, structTypeInfo.getFieldNames().toArray());

    Assert.assertEquals("STRUCT<name:STRING,age:BIGINT,parents:ARRAY<CHAR(20)>>", structTypeInfo.getTypeName());
  }
}
