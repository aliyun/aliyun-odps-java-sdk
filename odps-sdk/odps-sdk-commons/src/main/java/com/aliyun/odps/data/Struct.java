package com.aliyun.odps.data;

import java.io.Serializable;
import java.util.List;

import com.aliyun.odps.type.TypeInfo;

/**
 * Struct type 的数据类型接口
 *
 * Created by zhenhong.gzh on 16/8/22.
 */
public interface Struct extends Serializable {
  int getFieldCount();
  String getFieldName(int index);
  TypeInfo getFieldTypeInfo(int index);
  Object getFieldValue(int index);
  TypeInfo getFieldTypeInfo(String fieldName);
  Object getFieldValue(String fieldName);
  TypeInfo getTypeInfo();
  List<Object> getFieldValues();
  @Override
  String toString();
}
