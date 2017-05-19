package com.aliyun.odps.udf.utils;

import com.aliyun.odps.OdpsType;

import java.util.HashMap;

public class TypeUtils{
  private static HashMap<String, OdpsType> OdpsTypesMap = new HashMap<String, com.aliyun.odps.OdpsType>();
  static {
    OdpsTypesMap.put("bigint", com.aliyun.odps.OdpsType.BIGINT);
    OdpsTypesMap.put("double", com.aliyun.odps.OdpsType.DOUBLE);
    OdpsTypesMap.put("boolean", com.aliyun.odps.OdpsType.BOOLEAN);
    OdpsTypesMap.put("string", com.aliyun.odps.OdpsType.STRING);
    OdpsTypesMap.put("datetime", com.aliyun.odps.OdpsType.DATETIME);
    OdpsTypesMap.put("decimal", com.aliyun.odps.OdpsType.DECIMAL);
    OdpsTypesMap.put("binary", com.aliyun.odps.OdpsType.BINARY);
  }
  // Note: We have two versions of OdpsType, we are getting com.aliyun.odps.OdpsType here
  public static com.aliyun.odps.OdpsType getOdpsType(String typeStr) {
    return OdpsTypesMap.get(typeStr.toLowerCase());
  }
}