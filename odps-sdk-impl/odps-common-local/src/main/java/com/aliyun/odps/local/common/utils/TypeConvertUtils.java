package com.aliyun.odps.local.common.utils;

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
import com.google.gson.*;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.*;

public class TypeConvertUtils {

  public static final Charset UTF8 = Charset.forName("UTF-8");
  public static DateFormat DATE_FORMAT = LocalRunUtils.getDateFormat(Constants.DATE_FORMAT_2);
  public static boolean ENCODE_STRING = false;

  public static String toString(Object value, TypeInfo typeInfo) {
    Object javaVal = transOdpsToJava(value, typeInfo);
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
        rawVal = new GsonBuilder().disableHtmlEscaping().create().toJson(javaVal);
        break;
      default:
        throw new RuntimeException(" Unknown column type: " + typeInfo.getOdpsType());
    }
    //Encode:replace \N with "\N", exception column is null
    return rawVal.replaceAll("\\\\N", "\"\\\\N\"");
  }

  public static Object fromString(TypeInfo typeInfo, String token, boolean toBinary) {
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
          return DATE_FORMAT.parse(token);
        } catch (ParseException e) {
          throw new RuntimeException(" parse date failed:" + token, e);
        }
      case STRING:
        try {
          if (ENCODE_STRING) {
            byte[] bytes = LocalRunUtils.fromReadableString(token);
            return toBinary ? bytes : new String(bytes, UTF8);
          } else {
            return toBinary ? token.getBytes(UTF8) : token;
          }
        } catch (Exception e) {
          throw new RuntimeException(" from string failed!", e);
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
        JsonObject json = new JsonParser().parse(token).getAsJsonObject();
        return new IntervalDayTime(json.get("totalSeconds").getAsInt(), json.get("nanos").getAsInt());
      }
      case INTERVAL_YEAR_MONTH: {
        JsonObject json = new JsonParser().parse(token).getAsJsonObject();
        return new IntervalYearMonth(json.get("years").getAsInt(), json.get("months").getAsInt());
      }
      case STRUCT: {
        JsonObject json = new JsonParser().parse(token).getAsJsonObject();
        return parseStruct(json, (StructTypeInfo) typeInfo);
      }
      case MAP: {
        JsonObject json = new JsonParser().parse(token).getAsJsonObject();
        return parseMap(json, (MapTypeInfo) typeInfo);
      }
      case ARRAY: {
        JsonArray json = new JsonParser().parse(token).getAsJsonArray();
        return parseArray(json, (ArrayTypeInfo) typeInfo);
      }
      default:
        throw new RuntimeException("Unknown column type: " + typeInfo.getOdpsType());
    }
  }

  public static Class getOdpsJavaType(TypeInfo typeInfo) {
    switch (typeInfo.getOdpsType()) {
      case BIGINT:
        return Long.class;
      case DOUBLE:
        return Double.class;
      case BOOLEAN:
        return Boolean.class;
      case DATETIME:
        return Date.class;
      case STRING:
        return String.class;
      case DECIMAL:
        return BigDecimal.class;
      case TINYINT:
        return Byte.class;
      case SMALLINT:
        return Short.class;
      case INT:
        return Integer.class;
      case FLOAT:
        return Float.class;
      case CHAR:
        return Char.class;
      case VARCHAR:
        return Varchar.class;
      case DATE:
        return java.sql.Date.class;
      case TIMESTAMP:
        return Timestamp.class;
      case BINARY:
        return Binary.class;
      case INTERVAL_DAY_TIME:
        return IntervalDayTime.class;
      case INTERVAL_YEAR_MONTH:
        return IntervalYearMonth.class;
      case STRUCT:
        return Struct.class;
      case MAP:
        return Map.class;
      case ARRAY:
        return List.class;
      default:
        throw new RuntimeException("Unknown column type: " + typeInfo.getOdpsType());
    }
  }

  public static Object transOdpsToJava(Object value, TypeInfo typeInfo) {
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
            return ENCODE_STRING ? LocalRunUtils.toReadableString((byte[])value) : new String((byte[])value, UTF8);
          } else {
            return value;
          }
        } catch (Exception e) {
          throw new RuntimeException(" to readable string failed!", e);
        }
      case DATETIME:
        return DATE_FORMAT.format((Date) value);
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
      Object javaVal = transOdpsToJava(fieldValue, fieldType);
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
      Object key = transOdpsToJava(entry.getKey(), keyType);
      Object value = transOdpsToJava(entry.getValue(), valueType);
      result.put(key, value);
    }
    return result;
  }

  private static List transOdpsArrayToJavaList(List odpsArray, ArrayTypeInfo typeInfo) {
    List result = new ArrayList();
    TypeInfo eleType = typeInfo.getElementTypeInfo();
    for (Object item : odpsArray) {
      Object javaVal = transOdpsToJava(item, eleType);
      result.add(javaVal);
    }
    return result;
  }

  private static Struct parseStruct(JsonObject json, StructTypeInfo typeInfo) {
    List<String> fieldNames = typeInfo.getFieldNames();
    List<TypeInfo> typeInfos = typeInfo.getFieldTypeInfos();
    List<Object> structValues = new ArrayList<Object>();
    for (int i = 0; i < fieldNames.size(); i++) {
      String fieldName = fieldNames.get(i);
      TypeInfo fieldType = typeInfos.get(i);
      structValues.add(fromString(fieldType, asString(json.get(fieldName)), false));
    }
    return new SimpleStruct(typeInfo, structValues);
  }

  private static Map parseMap(JsonObject json, MapTypeInfo typeInfo) {
    Map result = new HashMap();

    TypeInfo keyType = typeInfo.getKeyTypeInfo();
    TypeInfo valueType = typeInfo.getValueTypeInfo();
    for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
      Object key = fromString(keyType, entry.getKey(), false);
      Object value = fromString(valueType, asString(entry.getValue()), false);
      result.put(key, value);
    }
    return result;
  }

  private static List parseArray(JsonArray jsonArray, ArrayTypeInfo arrayTypeInfo) {
    List result = new ArrayList();
    TypeInfo eleType = arrayTypeInfo.getElementTypeInfo();
    for (int i = 0; i  < jsonArray.size(); i++) {
      Object value = fromString(eleType, asString(jsonArray.get(i)), false);
      result.add(value);
    }
    return result;
  }

  private static String asString(JsonElement jsonElement) {
    try {
      return jsonElement.getAsString();
    } catch (Exception e) {
      return jsonElement.toString();
    }
  }

}