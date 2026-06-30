package com.aliyun.odps.type;

import com.aliyun.odps.OdpsType;

/**
 * Vector 类型接口，表示向量类型的类型信息。
 *
 * <p>向量类型的格式为 {@code VECTOR(element_type,dimension)}，例如：
 * <ul>
 *   <li>{@code VECTOR(FLOAT,1536)}</li>
 *   <li>{@code VECTOR(DOUBLE,768)}</li>
 * </ul>
 *
 * <p>支持的元素类型: {@link OdpsType#FLOAT} 和 {@link OdpsType#DOUBLE}。
 */
public interface VectorTypeInfo extends TypeInfo {

  /**
   * 获取向量的元素类型信息
   *
   * @return 元素类型信息，为 FLOAT 或 DOUBLE
   */
  TypeInfo getElementTypeInfo();

  /**
   * 获取向量的维度
   *
   * @return 正整数维度值
   */
  int getDimension();
}
