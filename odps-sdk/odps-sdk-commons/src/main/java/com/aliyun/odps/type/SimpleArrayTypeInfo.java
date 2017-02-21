package com.aliyun.odps.type;

import com.aliyun.odps.OdpsType;

/**
 * Odps array 类型
 *
 * Created by zhenhong.gzh on 16/7/8.
 */
class SimpleArrayTypeInfo implements ArrayTypeInfo {

  private TypeInfo valueType;

  /**
   * 创建 odps array 类型对象
   *
   * @param typeInfo
   *     array 中元素的类型
   */
  SimpleArrayTypeInfo(TypeInfo typeInfo) {
    if (typeInfo == null) {
      throw new IllegalArgumentException("Invalid element type.");
    }

    valueType = typeInfo;
  }

  @Override
  public String getTypeName() {
    return getOdpsType().name() + "<" + valueType.getTypeName() + ">";
  }

  /**
   * 获取 Array 里元素类型信息
   *
   * @return Array 里的元素类型
   */
  @Override
  public TypeInfo getElementTypeInfo() {
    return valueType;
  }

  @Override
  public OdpsType getOdpsType() {
    return OdpsType.ARRAY;
  }

  @Override
  public String toString() {
    return getTypeName();
  }
}
