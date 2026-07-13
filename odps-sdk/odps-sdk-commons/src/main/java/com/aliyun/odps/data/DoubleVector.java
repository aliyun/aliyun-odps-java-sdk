package com.aliyun.odps.data;

import java.util.Arrays;
import java.util.Objects;

import com.aliyun.odps.OdpsType;

/**
 * 双精度浮点向量实现。
 *
 * <p>不可变语义，内部数据不可被外部修改。
 * <ul>
 *   <li>{@link #of(double...)} 安全构造，拷贝输入数组</li>
 *   <li>{@link #wrap(double[])} 高性能路径，不拷贝（调用方须保证不修改传入数组）</li>
 * </ul>
 */
public final class DoubleVector implements Vector {

  private static final long serialVersionUID = 1L;

  private final double[] values;

  private DoubleVector(double[] values, boolean copy) {
    Objects.requireNonNull(values, "values cannot be null");
    if (values.length == 0) {
      throw new IllegalArgumentException("vector dimension must be > 0");
    }
    validateFinite(values);
    this.values = copy ? values.clone() : values;
  }

  /**
   * 安全构造方法，拷贝传入数组。
   *
   * @param values 向量元素
   * @return DoubleVector 实例
   */
  public static DoubleVector of(double... values) {
    return new DoubleVector(values, true);
  }

  /**
   * 高性能构造方法，不拷贝传入数组。
   * <b>调用方必须保证不修改传入的数组。</b>
   *
   * @param values 向量元素
   * @return DoubleVector 实例
   */
  public static DoubleVector wrap(double[] values) {
    return new DoubleVector(values, false);
  }

  /**
   * 获取指定位置的元素值
   *
   * @param index 元素索引
   * @return 该位置的 double 值
   */
  public double get(int index) {
    return values[index];
  }

  @Override
  public Number getElement(int index) {
    return values[index];
  }

  /**
   * 获取向量值的拷贝
   *
   * @return double 数组拷贝
   */
  public double[] values() {
    return values.clone();
  }

  /**
   * SDK 内部使用的快速路径，调用方不得修改返回的数组。
   */
  double[] rawValuesUnsafe() {
    return values;
  }

  @Override
  public int dimension() {
    return values.length;
  }

  @Override
  public OdpsType elementType() {
    return OdpsType.DOUBLE;
  }

  @Override
  public float[] toFloatArray() {
    float[] out = new float[values.length];
    for (int i = 0; i < values.length; i++) {
      float f = (float) values[i];
      if (Float.isInfinite(f) && !Double.isInfinite(values[i])) {
        throw new ArithmeticException(
            "double value " + values[i] + " at index " + i + " overflows float");
      }
      out[i] = f;
    }
    return out;
  }

  @Override
  public double[] toDoubleArray() {
    return values.clone();
  }

  private static void validateFinite(double[] values) {
    for (double v : values) {
      if (Double.isNaN(v) || Double.isInfinite(v)) {
        throw new IllegalArgumentException("vector contains NaN or Infinity: " + v);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DoubleVector)) {
      return false;
    }
    DoubleVector that = (DoubleVector) o;
    return Arrays.equals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }

  @Override
  public String toString() {
    return "DoubleVector(dim=" + values.length + ")";
  }
}
