package com.aliyun.odps.data;

import java.io.Serializable;
import java.util.List;

import com.aliyun.odps.OdpsType;

/**
 * 向量值的公共接口。
 *
 * <p>向量是固定维度的数值数组，目前支持 FLOAT 和 DOUBLE 两种元素类型。
 * 具体实现类为 {@link FloatVector} 和 {@link DoubleVector}。
 */
public interface Vector extends Serializable {

  /**
   * 获取向量的维度
   *
   * @return 维度值
   */
  int dimension();

  /**
   * 获取向量的元素类型
   *
   * @return 元素的 {@link OdpsType}，如 {@link OdpsType#FLOAT} 或 {@link OdpsType#DOUBLE}
   */
  OdpsType elementType();

  /**
   * 获取指定位置的元素值（装箱）。
   *
   * <p>该方法用于泛型序列化路径，与 {@code writeField}/{@code readField} 配合使用，
   * 使得 Vector 的序列化逻辑可以像 List/Array 一样统一处理，无需按元素类型分支。
   *
   * @param index 元素索引，范围 [0, dimension())
   * @return 装箱后的元素值
   */
  Number getElement(int index);

  /**
   * 将向量值转为 float 数组。
   * 如果底层元素为 double 且超出 float 范围，将抛出异常。
   *
   * @return float 数组（拷贝）
   */
  float[] toFloatArray();

  /**
   * 将向量值转为 double 数组。
   *
   * @return double 数组（拷贝）
   */
  double[] toDoubleArray();

  /**
   * 从 Number 列表构建 Vector 实例。
   *
   * <p>根据 elementType 自动创建对应的 Vector 实现。
   * 用于反序列化路径，将 {@code readField} 读取的元素组装回 Vector 对象。
   *
   * @param values      元素值列表
   * @param elementType 元素的 {@link OdpsType}
   * @return 对应类型的 Vector 实例
   * @throws IllegalArgumentException 如果 elementType 暂不支持
   */
  static Vector fromList(List<? extends Number> values, OdpsType elementType) {
    int size = values.size();
    switch (elementType) {
      case FLOAT: {
        float[] arr = new float[size];
        for (int i = 0; i < size; i++) {
          arr[i] = values.get(i).floatValue();
        }
        return FloatVector.wrap(arr);
      }
      case DOUBLE: {
        double[] arr = new double[size];
        for (int i = 0; i < size; i++) {
          arr[i] = values.get(i).doubleValue();
        }
        return DoubleVector.wrap(arr);
      }
      default:
        throw new IllegalArgumentException("Unsupported vector element type: " + elementType);
    }
  }
}
