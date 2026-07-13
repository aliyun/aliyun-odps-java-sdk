package com.aliyun.odps.type;

import java.util.Objects;

import com.aliyun.odps.OdpsType;

/**
 * Vector 类型的实现类。
 *
 * <p>向量类型表示固定维度的浮点数组。
 * 类型名称格式为 {@code VECTOR(FLOAT,1536)} 或 {@code VECTOR(DOUBLE,768)}。
 */
class SimpleVectorTypeInfo implements VectorTypeInfo {

  private static final long serialVersionUID = 1L;

  private final TypeInfo elementType;
  private final int dimension;

  /**
   * 创建向量类型信息
   *
   * @param elementType 元素类型
   * @param dimension   向量维度，必须大于 0
   */
  SimpleVectorTypeInfo(TypeInfo elementType, int dimension) {
    if (elementType == null) {
      throw new IllegalArgumentException("Vector element type cannot be null.");
    }

    if (dimension <= 0) {
      throw new IllegalArgumentException("Vector dimension must be > 0, but got: " + dimension);
    }

    this.elementType = elementType;
    this.dimension = dimension;
  }

  @Override
  public TypeInfo getElementTypeInfo() {
    return elementType;
  }

  @Override
  public int getDimension() {
    return dimension;
  }

  @Override
  public OdpsType getOdpsType() {
    return OdpsType.VECTOR;
  }

  @Override
  public String getTypeName() {
    return "VECTOR(" + elementType.getTypeName() + "," + dimension + ")";
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
    SimpleVectorTypeInfo that = (SimpleVectorTypeInfo) o;
    return dimension == that.dimension && Objects.equals(elementType, that.elementType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(elementType, dimension);
  }
}
