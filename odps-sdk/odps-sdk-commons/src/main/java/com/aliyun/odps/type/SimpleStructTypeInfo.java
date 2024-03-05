package com.aliyun.odps.type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.utils.StringUtils;

/**
 * Odps struct 类型
 *
 * Created by zhenhong.gzh on 16/7/8.
 */
class SimpleStructTypeInfo implements StructTypeInfo {

  private List<String> fieldNames;
  private List<TypeInfo> fieldTypeInfos;


  /**
   * 创建 odps struct 类型
   *
   * @param names
   *     struct 中字段的名称列表
   * @param typeInfos
   *     struct 中字段的类型列表
   */
  SimpleStructTypeInfo(List<String> names, List<TypeInfo> typeInfos) {
    validateParameters(names, typeInfos);

    this.fieldNames = StringUtils.toLowerCase(names);
    this.fieldTypeInfos = new ArrayList<TypeInfo>(typeInfos);
  }

  private void validateParameters(List<String> names, List<TypeInfo> typeInfos) {
    if (names == null || typeInfos == null || names.isEmpty() || typeInfos.isEmpty()) {
      throw new IllegalArgumentException("Invalid name or element type for struct.");
    }

    if (names.size() != typeInfos.size()) {
      throw new IllegalArgumentException(
          "The amount of field names must be equal to the amount of field types.");
    }
  }

  @Override
  public String getTypeName() {
    StringBuilder stringBuilder = new StringBuilder(getOdpsType().name());
    stringBuilder.append("<");

    for (int i = 0; i < fieldNames.size(); ++i) {
      if (i > 0) {
        stringBuilder.append(",");
      }
      stringBuilder.append(fieldNames.get(i));
      stringBuilder.append(":");
      stringBuilder.append(fieldTypeInfos.get(i).getTypeName());
    }

    stringBuilder.append(">");

    return stringBuilder.toString();
  }

  /**
   * 获取字段的名称列表
   *
   * @return 字段名称列表
   */
  @Override
  public List<String> getFieldNames() {
    return fieldNames;
  }

  /**
   * 获取字段的类型列表
   *
   * @return 字段类型列表
   */
  @Override
  public List<TypeInfo> getFieldTypeInfos() {
    return fieldTypeInfos;
  }

  /**
   * 获取字段数量
   *
   * @return 字段数量
   */
  @Override
  public int getFieldCount() {
    return fieldNames.size();
  }

  @Override
  public OdpsType getOdpsType() {
    return OdpsType.STRUCT;
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

    SimpleStructTypeInfo that = (SimpleStructTypeInfo) o;
    return StringUtils.equalsIgnoreCase(fieldNames, that.fieldNames)
           && Objects.equals(fieldTypeInfos, that.fieldTypeInfos);
  }

  @Override
  public int hashCode() {
    return Objects.hash(StringUtils.toLowerCase(fieldNames), fieldTypeInfos);
  }
}
