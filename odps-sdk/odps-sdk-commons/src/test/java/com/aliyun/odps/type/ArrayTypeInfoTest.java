package com.aliyun.odps.type;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsType;

/**
 * Created by zhenhong.gzh on 16/7/12.
 */
public class ArrayTypeInfoTest {

  private String getExpectName(String valueTypeName) {
    return  String.format("ARRAY<%s>", valueTypeName);
  }

  @Test
  public void testName() {

    ArrayTypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.VOID);
    Assert.assertEquals(getExpectName(OdpsType.VOID.name()), arrayTypeInfo.getTypeName());

    arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BIGINT);
    Assert.assertEquals(getExpectName(OdpsType.BIGINT.name()), arrayTypeInfo.getTypeName());

    arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getCharTypeInfo(10));
    Assert.assertEquals(getExpectName(arrayTypeInfo.getElementTypeInfo().getTypeName()), arrayTypeInfo.getTypeName());

    arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getVarcharTypeInfo(10));
    Assert.assertEquals(getExpectName(arrayTypeInfo.getElementTypeInfo().getTypeName()), arrayTypeInfo.getTypeName());

    arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getDecimalTypeInfo(2, 1));
    Assert.assertEquals(getExpectName(arrayTypeInfo.getElementTypeInfo().getTypeName()), arrayTypeInfo.getTypeName());

    // array in array
    ArrayTypeInfo arrayTypeInfo1 = TypeInfoFactory.getArrayTypeInfo(arrayTypeInfo);
    Assert.assertEquals(getExpectName(arrayTypeInfo.getTypeName()), arrayTypeInfo1.getTypeName());

    // map in array
    arrayTypeInfo1 = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.BIGINT, TypeInfoFactory.STRING));
    Assert.assertEquals(getExpectName(arrayTypeInfo1.getElementTypeInfo().getTypeName()), arrayTypeInfo1.getTypeName());
  }
}
