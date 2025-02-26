package com.aliyun.odps.type;

import java.util.Objects;

import com.aliyun.odps.OdpsType;

/**
 * Odps array 类型
 *
 * Created by zhenhong.gzh on 16/7/8.
 */
class SimpleArrayTypeInfo implements ArrayTypeInfo {
  private static final long serialVersionUID = 1L;

  private final TypeInfo valueType;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SimpleArrayTypeInfo that = (SimpleArrayTypeInfo) o;
    return Objects.equals(valueType, that.valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueType);
  }

  @Override
  public String getTypeName() {
    return getTypeName(false);
  }
  @Override
  public String getTypeName(boolean quote) {
    if (quote && valueType instanceof NestedTypeInfo) {
      return getOdpsType().name() + "<" + ((NestedTypeInfo) valueType).getTypeName(true) + ">";
    }
    return getOdpsType().name() + "<" + valueType.getTypeName() + ">";
  }
}
