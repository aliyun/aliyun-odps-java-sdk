package com.aliyun.odps.type;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhenhong.gzh on 16/7/12.
 */
public class MapTypeInfoTest {

  private String getExpectName(String key, String value) {
    return String.format("MAP<%s,%s>", key, value);
  }

  @Test
  public void testName() {
    
    MapTypeInfo mapTypeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.BIGINT, TypeInfoFactory.STRING);
    Assert.assertEquals(getExpectName(mapTypeInfo.getKeyTypeInfo().getTypeName(), mapTypeInfo.getValueTypeInfo().getTypeName()), mapTypeInfo.getTypeName());

    // decimal, char, varchar
    mapTypeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.getVarcharTypeInfo(10), TypeInfoFactory.DECIMAL);
    Assert.assertEquals(getExpectName(mapTypeInfo.getKeyTypeInfo().getTypeName(), mapTypeInfo.getValueTypeInfo().getTypeName()), mapTypeInfo.getTypeName());

    mapTypeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.getVarcharTypeInfo(10), TypeInfoFactory.DECIMAL);
    Assert.assertEquals(getExpectName(mapTypeInfo.getKeyTypeInfo().getTypeName(), mapTypeInfo.getValueTypeInfo().getTypeName()), mapTypeInfo.getTypeName());

    mapTypeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.getCharTypeInfo(10), TypeInfoFactory.getDecimalTypeInfo(1, 0));
    Assert.assertEquals(getExpectName(mapTypeInfo.getKeyTypeInfo().getTypeName(), mapTypeInfo.getValueTypeInfo().getTypeName()), mapTypeInfo.getTypeName());

    // nest in map

    mapTypeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.BIGINT, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING));
    Assert.assertEquals(getExpectName(mapTypeInfo.getKeyTypeInfo().getTypeName(), mapTypeInfo.getValueTypeInfo().getTypeName()), mapTypeInfo.getTypeName());

    mapTypeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.BIGINT, TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.getCharTypeInfo(10), TypeInfoFactory.getDecimalTypeInfo(10, 2)));
    Assert.assertEquals(getExpectName(mapTypeInfo.getKeyTypeInfo().getTypeName(), mapTypeInfo.getValueTypeInfo().getTypeName()), mapTypeInfo.getTypeName());
  }
}
