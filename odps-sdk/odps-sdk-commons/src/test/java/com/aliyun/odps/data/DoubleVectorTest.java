package com.aliyun.odps.data;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsType;

public class DoubleVectorTest {

  @Test
  public void testOfAndBasicProperties() {
    DoubleVector vec = DoubleVector.of(1.0, 2.5, -3.14);
    Assert.assertEquals(3, vec.dimension());
    Assert.assertEquals(OdpsType.DOUBLE, vec.elementType());
    Assert.assertEquals(1.0, vec.get(0), 0.0);
    Assert.assertEquals(2.5, vec.get(1), 0.0);
    Assert.assertEquals(-3.14, vec.get(2), 0.0);
  }

  @Test
  public void testWrap() {
    double[] raw = {10.0, 20.0};
    DoubleVector vec = DoubleVector.wrap(raw);
    Assert.assertEquals(2, vec.dimension());
    Assert.assertEquals(10.0, vec.get(0), 0.0);
    Assert.assertEquals(20.0, vec.get(1), 0.0);
  }

  @Test
  public void testValuesIsCopy() {
    DoubleVector vec = DoubleVector.of(1.0, 2.0);
    double[] copy = vec.values();
    copy[0] = 999.0;
    Assert.assertEquals(1.0, vec.get(0), 0.0);
  }

  @Test
  public void testToDoubleArray() {
    DoubleVector vec = DoubleVector.of(1.0, 2.0, 3.0);
    Assert.assertArrayEquals(new double[]{1.0, 2.0, 3.0}, vec.toDoubleArray(), 0.0);
  }

  @Test
  public void testToFloatArray() {
    DoubleVector vec = DoubleVector.of(1.0, 2.0, 3.0);
    Assert.assertArrayEquals(new float[]{1.0f, 2.0f, 3.0f}, vec.toFloatArray(), 0.0f);
  }

  @Test(expected = ArithmeticException.class)
  public void testToFloatArrayOverflow() {
    DoubleVector vec = DoubleVector.of(Double.MAX_VALUE);
    vec.toFloatArray();
  }

  @Test
  public void testEquals() {
    DoubleVector a = DoubleVector.of(1.0, 2.0);
    DoubleVector b = DoubleVector.of(1.0, 2.0);
    DoubleVector c = DoubleVector.of(1.0, 3.0);
    Assert.assertEquals(a, b);
    Assert.assertEquals(a.hashCode(), b.hashCode());
    Assert.assertNotEquals(a, c);
  }

  @Test
  public void testToString() {
    DoubleVector vec = DoubleVector.of(1.0, 2.0, 3.0);
    Assert.assertEquals("DoubleVector(dim=3)", vec.toString());
  }

  @Test(expected = NullPointerException.class)
  public void testNullValues() {
    DoubleVector.of(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyValues() {
    DoubleVector.of(new double[0]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNaN() {
    DoubleVector.of(1.0, Double.NaN);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInfinity() {
    DoubleVector.of(Double.POSITIVE_INFINITY);
  }
}
