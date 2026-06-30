package com.aliyun.odps.type;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsType;

public class VectorTypeInfoTest {

  @Test
  public void testFloatVectorTypeName() {
    VectorTypeInfo typeInfo = TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, 1536);
    Assert.assertEquals(OdpsType.VECTOR, typeInfo.getOdpsType());
    Assert.assertEquals("VECTOR(FLOAT,1536)", typeInfo.getTypeName());
    Assert.assertEquals(TypeInfoFactory.FLOAT, typeInfo.getElementTypeInfo());
    Assert.assertEquals(1536, typeInfo.getDimension());
  }

  @Test
  public void testDoubleVectorTypeName() {
    VectorTypeInfo typeInfo = TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.DOUBLE, 768);
    Assert.assertEquals(OdpsType.VECTOR, typeInfo.getOdpsType());
    Assert.assertEquals("VECTOR(DOUBLE,768)", typeInfo.getTypeName());
    Assert.assertEquals(TypeInfoFactory.DOUBLE, typeInfo.getElementTypeInfo());
    Assert.assertEquals(768, typeInfo.getDimension());
  }

  @Test
  public void testEquals() {
    VectorTypeInfo a = TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, 128);
    VectorTypeInfo b = TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, 128);
    VectorTypeInfo c = TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, 256);
    VectorTypeInfo d = TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.DOUBLE, 128);

    Assert.assertEquals(a, b);
    Assert.assertEquals(a.hashCode(), b.hashCode());
    Assert.assertNotEquals(a, c);
    Assert.assertNotEquals(a, d);
  }

  @Test
  public void testToString() {
    VectorTypeInfo typeInfo = TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, 3);
    Assert.assertEquals("VECTOR(FLOAT,3)", typeInfo.toString());
  }
}
