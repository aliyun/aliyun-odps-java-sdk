package com.aliyun.odps.data;

import java.util.Arrays;
import java.util.Objects;

import com.aliyun.odps.OdpsType;

/**
 * 单精度浮点向量实现。
 *
 * <p>不可变语义，内部数据不可被外部修改。
 * <ul>
 *   <li>{@link #of(float...)} 安全构造，拷贝输入数组</li>
 *   <li>{@link #wrap(float[])} 高性能路径，不拷贝（调用方须保证不修改传入数组）</li>
 * </ul>
 */
public final class FloatVector implements Vector {

  private static final long serialVersionUID = 1L;

  private final float[] values;

  private FloatVector(float[] values, boolean copy) {
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
   * @return FloatVector 实例
   */
  public static FloatVector of(float... values) {
    return new FloatVector(values, true);
  }

  /**
   * 高性能构造方法，不拷贝传入数组。
   * <b>调用方必须保证不修改传入的数组。</b>
   *
   * @param values 向量元素
   * @return FloatVector 实例
   */
  public static FloatVector wrap(float[] values) {
    return new FloatVector(values, false);
  }

  /**
   * 获取指定位置的元素值
   *
   * @param index 元素索引
   * @return 该位置的 float 值
   */
  public float get(int index) {
    return values[index];
  }

  /**
   * 获取向量值的拷贝
   *
   * @return float 数组拷贝
   */
  public float[] values() {
    return values.clone();
  }

  /**
   * SDK 内部使用的快速路径，调用方不得修改返回的数组。
   */
  float[] rawValuesUnsafe() {
    return values;
  }

  @Override
  public int dimension() {
    return values.length;
  }

  @Override
  public OdpsType elementType() {
    return OdpsType.FLOAT;
  }

  @Override
  public float[] toFloatArray() {
    return values.clone();
  }

  @Override
  public double[] toDoubleArray() {
    double[] out = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = values[i];
    }
    return out;
  }

  private static void validateFinite(float[] values) {
    for (float v : values) {
      if (Float.isNaN(v) || Float.isInfinite(v)) {
        throw new IllegalArgumentException("vector contains NaN or Infinity: " + v);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FloatVector)) {
      return false;
    }
    FloatVector that = (FloatVector) o;
    return Arrays.equals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }

  @Override
  public String toString() {
    return "FloatVector(dim=" + values.length + ")";
  }
}
