package com.aliyun.odps.data;

import java.io.Serializable;

import com.aliyun.odps.OdpsType;

/**
 * 向量值的公共接口。
 *
 * <p>向量是固定维度的浮点数组，支持 FLOAT 和 DOUBLE 两种元素类型。
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
   * @return {@link OdpsType#FLOAT} 或 {@link OdpsType#DOUBLE}
   */
  OdpsType elementType();

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
}
