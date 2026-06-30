package com.aliyun.odps.data;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsType;

public class FloatVectorTest {

  @Test
  public void testOf() {
    FloatVector v = FloatVector.of(1.0f, 2.0f, 3.0f);
    Assert.assertEquals(3, v.dimension());
    Assert.assertEquals(OdpsType.FLOAT, v.elementType());
    Assert.assertEquals(1.0f, v.get(0), 0.0f);
    Assert.assertEquals(2.0f, v.get(1), 0.0f);
    Assert.assertEquals(3.0f, v.get(2), 0.0f);
  }

  @Test
  public void testOfIsCopy() {
    float[] data = {1.0f, 2.0f};
    FloatVector v = FloatVector.of(data);
    data[0] = 999.0f;
    Assert.assertEquals(1.0f, v.get(0), 0.0f);
  }

  @Test
  public void testWrapIsNoCopy() {
    float[] data = {1.0f, 2.0f};
    FloatVector v = FloatVector.wrap(data);
    Assert.assertEquals(1.0f, v.get(0), 0.0f);
  }

  @Test
  public void testValues() {
    FloatVector v = FloatVector.of(1.0f, 2.0f);
    float[] copy = v.values();
    copy[0] = 999.0f;
    Assert.assertEquals(1.0f, v.get(0), 0.0f);
  }

  @Test
  public void testToFloatArray() {
    FloatVector v = FloatVector.of(1.0f, 2.0f, 3.0f);
    float[] arr = v.toFloatArray();
    Assert.assertArrayEquals(new float[]{1.0f, 2.0f, 3.0f}, arr, 0.0f);
  }

  @Test
  public void testToDoubleArray() {
    FloatVector v = FloatVector.of(1.0f, 2.0f, 3.0f);
    double[] arr = v.toDoubleArray();
    Assert.assertArrayEquals(new double[]{1.0, 2.0, 3.0}, arr, 0.001);
  }

  @Test
  public void testEquals() {
    FloatVector a = FloatVector.of(1.0f, 2.0f);
    FloatVector b = FloatVector.of(1.0f, 2.0f);
    FloatVector c = FloatVector.of(1.0f, 3.0f);

    Assert.assertEquals(a, b);
    Assert.assertEquals(a.hashCode(), b.hashCode());
    Assert.assertNotEquals(a, c);
    Assert.assertNotEquals(a, null);
    Assert.assertNotEquals(a, "not a vector");
  }

  @Test
  public void testToString() {
    FloatVector v = FloatVector.of(1.0f, 2.0f, 3.0f);
    Assert.assertEquals("FloatVector(dim=3)", v.toString());
  }

  @Test(expected = NullPointerException.class)
  public void testNullValues() {
    FloatVector.of(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyValues() {
    FloatVector.of(new float[0]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNaN() {
    FloatVector.of(1.0f, Float.NaN);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInfinity() {
    FloatVector.of(Float.POSITIVE_INFINITY, 2.0f);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeInfinity() {
    FloatVector.of(1.0f, Float.NEGATIVE_INFINITY);
  }

  @Test
  public void testSingleElement() {
    FloatVector v = FloatVector.of(42.0f);
    Assert.assertEquals(1, v.dimension());
    Assert.assertEquals(42.0f, v.get(0), 0.0f);
  }

  @Test
  public void testLargeDimension() {
    float[] data = new float[4096];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 0.01f;
    }
    FloatVector v = FloatVector.of(data);
    Assert.assertEquals(4096, v.dimension());
    Assert.assertEquals(0.0f, v.get(0), 0.0001f);
    Assert.assertEquals(40.95f, v.get(4095), 0.01f);
  }
}
