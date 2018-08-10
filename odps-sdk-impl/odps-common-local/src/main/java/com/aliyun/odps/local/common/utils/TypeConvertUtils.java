package com.aliyun.odps.local.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.IntervalDayTime;
import com.aliyun.odps.data.IntervalYearMonth;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TypeConvertUtils {

  private static Charset UTF8 = Charset.forName("UTF-8");

  public static String toString(Object value, TypeInfo typeInfo, boolean isBinary) {
    Object javaVal = transOdpsToJava(value, typeInfo, isBinary);
    if (javaVal == null) {
      return Constants.NULL_TOKEN;
    }
    String rawVal;
    switch (typeInfo.getOdpsType()) {
      case BIGINT:
      case BOOLEAN:
      case DOUBLE:
      case DECIMAL:
      case TINYINT:
      case SMALLINT:
      case INT:
      case FLOAT:
      case CHAR:
      case VARCHAR:
      case DATE:
      case TIMESTAMP:
      case STRING:
      case DATETIME:
      case BINARY:
        rawVal = javaVal.toString();
        break;
      case INTERVAL_DAY_TIME:
      case INTERVAL_YEAR_MONTH:
      case STRUCT:
      case MAP:
      case ARRAY:
        rawVal = JSON.toJSONString(javaVal);
        break;
      default:
        throw new RuntimeException(" Unknown column type: " + typeInfo.getOdpsType());
    }
    //Encode:replace \N with "\N", exception column is null
    return rawVal.replaceAll("\\\\N", "\"\\\\N\"");
  }

  public static Object fromString(TypeInfo typeInfo, String token, boolean isBinary) {
    if (token == null || Constants.NULL_TOKEN.equals(token)) {
      return null;
    }
    switch (typeInfo.getOdpsType()) {
      case BIGINT:
        return Long.parseLong(token);
      case DOUBLE:
        return Double.parseDouble(token);
      case BOOLEAN:
        return Boolean.parseBoolean(token);
      case DATETIME:
        try {
          return LocalRunUtils.getDateFormat(Constants.DATE_FORMAT_2).parse(token);
        } catch (ParseException e) {
          throw new RuntimeException(" parse date failed:" + token, e);
        }
      case STRING:
        try {
          byte[] v = LocalRunUtils.fromReadableString(token);
          return isBinary ? v : new String(v, UTF8);
        } catch (Exception e) {
          throw new RuntimeException(" from readable string failed!", e);
        }
      case DECIMAL:
        return new BigDecimal(token);
      case TINYINT:
        return Byte.parseByte(token);
      case SMALLINT:
        return Short.parseShort(token);
      case INT:
        return Integer.parseInt(token);
      case FLOAT:
        return Float.parseFloat(token);
      case CHAR:
        return new Char(token, ((CharTypeInfo)typeInfo).getLength());
      case VARCHAR:
        return new Varchar(token, ((VarcharTypeInfo)typeInfo).getLength());
      case DATE:
        return java.sql.Date.valueOf(token);
      case TIMESTAMP:
        return Timestamp.valueOf(token);
      case BINARY:
        try {
          return new Binary(LocalRunUtils.fromReadableString(token));
        } catch (Exception e) {
          throw new RuntimeException(" from readable string failed!" + e);
        }
      case INTERVAL_DAY_TIME: {
        JSONObject json = JSON.parseObject(token);
        return new IntervalDayTime(json.getInteger("totalSeconds"), json.getInteger("nanos"));
      }
      case INTERVAL_YEAR_MONTH: {
        JSONObject json = JSON.parseObject(token);
        return new IntervalYearMonth(json.getInteger("years"), json.getInteger("months"));
      }
      case STRUCT: {
        JSONObject json = JSON.parseObject(token);
        return parseStruct(json, (StructTypeInfo) typeInfo, isBinary);
      }
      case MAP: {
        JSONObject json = JSON.parseObject(token);
        return parseMap(json, (MapTypeInfo) typeInfo, isBinary);
      }
      case ARRAY: {
        JSONArray json = JSON.parseArray(token);
        return parseArray(json, (ArrayTypeInfo) typeInfo, isBinary);
      }
      default:
        throw new RuntimeException("Unknown column type: " + typeInfo.getOdpsType());
    }
  }

  private static Object transOdpsToJava(Object value, TypeInfo typeInfo, boolean isBinary) {
    if (value == null) {
      return null;
    }
    switch (typeInfo.getOdpsType()) {
      case BIGINT:
      case BOOLEAN:
      case DOUBLE:
      case DECIMAL:
      case TINYINT:
      case SMALLINT:
      case INT:
      case FLOAT:
      case DATE:
      case TIMESTAMP:
        return value;
      case CHAR:
      case VARCHAR:
        return value.toString();
      case STRING:
        try {
          if (value instanceof byte[]) {
            return isBinary ? LocalRunUtils.toReadableString((byte[])value) : new String((byte[])value, UTF8);
          } else {
            return value;
          }
        } catch (Exception e) {
          throw new RuntimeException(" to readable string failed!", e);
        }
      case DATETIME:
        return LocalRunUtils.getDateFormat(Constants.DATE_FORMAT_2).format((Date) value);
      case INTERVAL_DAY_TIME:
        return transIntervalDayTimeToJavaMap((IntervalDayTime)value);
      case INTERVAL_YEAR_MONTH:
        return transIntervalYearMonthToJavaMap((IntervalYearMonth)value);
      case BINARY:
        try {
          return LocalRunUtils.toReadableString(((Binary)value).data());
        } catch (Exception e) {
          throw new RuntimeException(" to readable string failed!", e);
        }
      case STRUCT:
        return transOdpsStructToJavaMap((Struct) value);
      case MAP:
        return transOdpsMapToJavaMap((Map)value, (MapTypeInfo) typeInfo);
      case ARRAY:
        return transOdpsArrayToJavaList((List) value, (ArrayTypeInfo) typeInfo);
      default:
        throw new RuntimeException(" Unknown column type: " + typeInfo.getOdpsType());
    }
  }

  private static Map transIntervalDayTimeToJavaMap(IntervalDayTime dayTime) {
    Map<String, Long> result = new HashMap();
    result.put("totalSeconds", dayTime.getTotalSeconds());
    result.put("nanos", (long)dayTime.getNanos());
    return result;
  }

  private static Map transIntervalYearMonthToJavaMap(IntervalYearMonth yearMonth) {
    Map <String, Integer> result = new HashMap();
    result.put("years", yearMonth.getYears());
    result.put("months", yearMonth.getMonths());
    return result;
  }

  private static Map transOdpsStructToJavaMap(Struct odpsStruct) {
    Map result = new HashMap();
    for (int i = 0; i < odpsStruct.getFieldCount(); i++) {
      String fieldName = odpsStruct.getFieldName(i);
      Object fieldValue = odpsStruct.getFieldValue(i);
      TypeInfo fieldType = odpsStruct.getFieldTypeInfo(i);
      Object javaVal = transOdpsToJava(fieldValue, fieldType, false);
      result.put(fieldName, javaVal);
    }
    return result;
  }

  private static Map transOdpsMapToJavaMap(Map odpsMap, MapTypeInfo typeInfo) {
    Map result = new HashMap();
    TypeInfo keyType = typeInfo.getKeyTypeInfo();
    TypeInfo valueType = typeInfo.getValueTypeInfo();
    Set<Map.Entry> entrySet = odpsMap.entrySet();
    for (Map.Entry entry : entrySet) {
      Object key = transOdpsToJava(entry.getKey(), keyType, false);
      Object value = transOdpsToJava(entry.getValue(), valueType, false);
      result.put(key, value);
    }
    return result;
  }

  private static List transOdpsArrayToJavaList(List odpsArray, ArrayTypeInfo typeInfo) {
    List result = new ArrayList();
    TypeInfo eleType = typeInfo.getElementTypeInfo();
    for (Object item : odpsArray) {
      Object javaVal = transOdpsToJava(item, eleType, false);
      result.add(javaVal);
    }
    return result;
  }

  private static Struct parseStruct(JSONObject json, StructTypeInfo typeInfo, boolean isBinary) {
    List<String> fieldNames = typeInfo.getFieldNames();
    List<TypeInfo> typeInfos = typeInfo.getFieldTypeInfos();
    List<Object> structValues = new ArrayList<Object>();
    for (int i = 0; i < fieldNames.size(); i++) {
      String fieldName = fieldNames.get(i);
      TypeInfo fieldType = typeInfos.get(i);
      structValues.add(fromString(fieldType, json.getString(fieldName), isBinary));
    }
    return new SimpleStruct(typeInfo, structValues);
  }

  private static Map parseMap(JSONObject json, MapTypeInfo typeInfo, boolean isBinary) {
    Map result = new HashMap();
    Set<String> keys = json.keySet();
    TypeInfo keyType = typeInfo.getKeyTypeInfo();
    TypeInfo valueType = typeInfo.getValueTypeInfo();
    for (String item : keys) {
      Object key = fromString(keyType, item, isBinary);
      Object value = fromString(valueType, json.getString(item), isBinary);
      result.put(key, value);
    }
    return result;
  }

  private static List parseArray(JSONArray jsonArray, ArrayTypeInfo arrayTypeInfo, boolean isBinary) {
    List result = new ArrayList();
    TypeInfo eleType = arrayTypeInfo.getElementTypeInfo();
    for (int i = 0; i  < jsonArray.size(); i++) {
      Object value = fromString(eleType, jsonArray.getString(i), isBinary);
      result.add(value);
    }
    return result;
  }

}
