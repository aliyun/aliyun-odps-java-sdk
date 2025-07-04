package com.aliyun.odps.type;

import java.util.Objects;

import com.aliyun.odps.OdpsType;

/**
 * Odps map 类型
 *
 * Created by zhenhong.gzh on 16/7/8.
 */
class SimpleMapTypeInfo implements MapTypeInfo {
  private static final long serialVersionUID = 1L;

  private final TypeInfo keyType;
  private final TypeInfo valueType;

  /**
   * 创建 odps map 类型
   *
   * @param keyType
   *     map 中 key 的类型
   * @param valueType
   *     map 中 value 的类型
   */
  SimpleMapTypeInfo(TypeInfo keyType, TypeInfo valueType) {
    if (keyType == null || valueType == null) {
      throw new IllegalArgumentException("Invalid key or value type for map.");
    }

    this.keyType = keyType;
    this.valueType = valueType;
  }

  /**
   * 获取键的类型
   *
   * @return 键的类型
   */
  @Override
  public TypeInfo getKeyTypeInfo() {
    return keyType;
  }

  /**
   * 获取值的类型
   *
   * @return 值的类型
   */
  @Override
  public TypeInfo getValueTypeInfo() {
    return valueType;
  }

  @Override
  public OdpsType getOdpsType() {
    return OdpsType.MAP;
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
    SimpleMapTypeInfo that = (SimpleMapTypeInfo) o;
    return Objects.equals(keyType, that.keyType) && Objects.equals(valueType, that.valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyType, valueType);
  }

  @Override
  public String getTypeName() {
    return getTypeName(false);
  }

  @Override
  public String getTypeName(boolean quote) {
    String baseType = getOdpsType().name();

    if (!quote) {
      return String.format("%s<%s,%s>",
                           baseType,
                           keyType.getTypeName(),
                           valueType.getTypeName()
      );
    }

    StringBuilder sb = new StringBuilder(baseType).append("<");
    appendNestedType(sb, keyType);
    sb.append(",");
    appendNestedType(sb, valueType);
    return sb.append(">").toString();
  }

  private void appendNestedType(StringBuilder sb, TypeInfo type) {
    if (type instanceof NestedTypeInfo) {
      sb.append(((NestedTypeInfo) type).getTypeName(true));
    } else {
      sb.append(type.getTypeName());
    }
  }
}
