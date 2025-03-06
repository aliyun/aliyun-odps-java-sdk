package com.aliyun.odps.data;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.StringUtils;

/**
 * 一个简单 {@link Struct} 接口的实现
 *
 * Created by zhenhong.gzh on 16/8/22.
 */
public class SimpleStruct implements Struct {
  protected StructTypeInfo typeInfo;
  protected List<Object>  values;

  /**
   * A simple implements of {@link Struct}
   *
   * @param type
   *       type of the struct
   * @param values
   *       values of the struct
   *       be careful: the struct value list is a reference of this param
   */
  public SimpleStruct(StructTypeInfo type, List<Object> values) {
    if (type == null || values == null || values.size() != type.getFieldCount()) {
      throw new IllegalArgumentException("Illegal arguments for StructObject.");
    }

    this.typeInfo = type;
    this.values = values;
  }

  @Override
  public int getFieldCount() {
    return values.size();
  }

  @Override
  public String getFieldName(int index) {
    return typeInfo.getFieldNames().get(index);
  }

  @Override
  public TypeInfo getFieldTypeInfo(int index) {
    return typeInfo.getFieldTypeInfos().get(index);
  }

  @Override
  public Object getFieldValue(int index) {
    return values.get(index);
  }

  @Override
  public TypeInfo getFieldTypeInfo(String fieldName) {
    for (int i = 0; i < typeInfo.getFieldCount(); ++i) {
      if (typeInfo.getFieldNames().get(i).equalsIgnoreCase(fieldName)) {
        return typeInfo.getFieldTypeInfos().get(i);
      }
    }
    return null;
  }

  @Override
  public Object getFieldValue(String fieldName) {
    for (int i = 0; i < typeInfo.getFieldCount(); ++i) {
      if (typeInfo.getFieldNames().get(i).equalsIgnoreCase(fieldName)) {
        return values.get(i);
      }
    }
    return null;
  }

  @Override
  public TypeInfo getTypeInfo() {
    return typeInfo;
  }

  @Override
  public List<Object> getFieldValues() {
    return values;
  }

  @Override
  public String toString() {
    String valueStr = "{";
    int colCount = getFieldCount();
    for (int i = 0; i < colCount; ++i) {
      valueStr += getFieldName(i) + ":" + getFieldValue(i);
      if (i != colCount - 1) {
        valueStr += ", ";
      }
    }
    valueStr += "}";
    return valueStr;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SimpleStruct that = (SimpleStruct) o;

    if (!StringUtils.equalsIgnoreCase(this.typeInfo.getFieldNames(), that.typeInfo.getFieldNames())) {
      return false;
    }

    return Objects.equals(this.getFieldValues(), that.getFieldValues());
  }

  @Override
  public int hashCode() {
    return Objects.hash(StringUtils.toLowerCase(typeInfo.getFieldNames()), values);
  }

}
