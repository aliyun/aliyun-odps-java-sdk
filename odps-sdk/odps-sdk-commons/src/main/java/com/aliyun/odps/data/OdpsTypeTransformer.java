package com.aliyun.odps.data;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;

/**
 * Created by zhenhong.gzh on 16/12/13.
 */
public class OdpsTypeTransformer {
  private static final int STRING_MAX_LENGTH = 8 * 1024 * 1024;

  // 9999-12-31 23:59:59
  private static final long DATETIME_MAX_TICKS = 253402271999000L;
  // 0001-01-01 00:00:00
  private static final long DATETIME_MIN_TICKS = -62135798400000L;

  private static Map<OdpsType, Class> ODPS_TYPE_MAPPER = new HashMap<OdpsType, Class>() ;

  OdpsTypeTransformer() {

  }

  static {
    ODPS_TYPE_MAPPER.put(OdpsType.BIGINT, Long.class);
    ODPS_TYPE_MAPPER.put(OdpsType.STRING, String.class);
    ODPS_TYPE_MAPPER.put(OdpsType.DATETIME, java.util.Date.class);
    ODPS_TYPE_MAPPER.put(OdpsType.DOUBLE, Double.class);
    ODPS_TYPE_MAPPER.put(OdpsType.BOOLEAN, Boolean.class);
    ODPS_TYPE_MAPPER.put(OdpsType.DECIMAL, BigDecimal.class);
    ODPS_TYPE_MAPPER.put(OdpsType.ARRAY, List.class);
    ODPS_TYPE_MAPPER.put(OdpsType.MAP, Map.class);
    ODPS_TYPE_MAPPER.put(OdpsType.STRUCT, Struct.class);
    ODPS_TYPE_MAPPER.put(OdpsType.INT, Integer.class);
    ODPS_TYPE_MAPPER.put(OdpsType.TINYINT, Byte.class);
    ODPS_TYPE_MAPPER.put(OdpsType.SMALLINT, Short.class);
    ODPS_TYPE_MAPPER.put(OdpsType.DATE, java.sql.Date.class);
    ODPS_TYPE_MAPPER.put(OdpsType.TIMESTAMP, java.sql.Timestamp.class);
    ODPS_TYPE_MAPPER.put(OdpsType.FLOAT, Float.class);
    ODPS_TYPE_MAPPER.put(OdpsType.CHAR, Char.class);
    ODPS_TYPE_MAPPER.put(OdpsType.BINARY, Binary.class);
    ODPS_TYPE_MAPPER.put(OdpsType.VARCHAR, Varchar.class);
    ODPS_TYPE_MAPPER.put(OdpsType.INTERVAL_YEAR_MONTH, IntervalYearMonth.class);
    ODPS_TYPE_MAPPER.put(OdpsType.INTERVAL_DAY_TIME, IntervalDayTime.class);
  }

  public static Class odpsTypeToJavaType(OdpsType type) {
    if (ODPS_TYPE_MAPPER.containsKey(type)) {
      return ODPS_TYPE_MAPPER.get(type);
    }

    throw new IllegalArgumentException("Cannot get Java type for Odps type: " + type);
  }

  private static void validateString(String value) {
    try {
      if ((value.length() * 6 > STRING_MAX_LENGTH) && (value.getBytes("utf-8").length
          > STRING_MAX_LENGTH)) {
        throw new IllegalArgumentException("InvalidData: The string's length is more than "
            + STRING_MAX_LENGTH / 1024 / 1024 + "M.");
      }
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  private static void validateChar(Char value, CharTypeInfo typeInfo) {
    if (value.length() > typeInfo.getLength()) {
      throw new IllegalArgumentException(String.format(
          "InvalidData: %s data is overflow, pls check data length: %s.", typeInfo.getTypeName(),
          (value).length()));
    }
  }

  private static void validateVarChar(Varchar value, VarcharTypeInfo typeInfo) {
    if (value.length() > typeInfo.getLength()) {
      throw new IllegalArgumentException(String.format(
          "InvalidData: %s data is overflow, pls check data length: %s.", typeInfo.getTypeName(),
          (value).length()));
    }
  }

  private static void validateBigint(Long value) {
    if (value == Long.MIN_VALUE) {
      throw new IllegalArgumentException("InvalidData: Bigint out of range.");
    }
  }

  private static void validateDateTime(java.util.Date value) {
    if ((value.getTime() > DATETIME_MAX_TICKS || value.getTime() < DATETIME_MIN_TICKS)) {
      throw new IllegalArgumentException("InvalidData: Datetime out of range.");
    }
  }

  private static void validateDecimal(BigDecimal value, DecimalTypeInfo typeInfo) {
    BigDecimal tmpValue = value.setScale(typeInfo.getScale(), RoundingMode.HALF_UP);
    int intLength = tmpValue.precision() - tmpValue.scale();
    if (intLength > (typeInfo.getPrecision() - typeInfo.getScale())) {
      throw new IllegalArgumentException(
          String.format("InvalidData: decimal value %s overflow, max integer digit number is %s.",
                        value, (typeInfo.getPrecision() - typeInfo.getScale())));
    }

  }

  private static List transformArray(List value, ArrayTypeInfo typeInfo) {
    List<Object> newList = new ArrayList<Object>(value.size());

    TypeInfo elementTypeInfo = typeInfo.getElementTypeInfo();

    for (Object obj : value) {
      newList.add(transform(obj, elementTypeInfo));
    }

    return newList;
  }

  private static Map transformMap(Map value, MapTypeInfo typeInfo) {
    TypeInfo keyTypeInfo = typeInfo.getKeyTypeInfo();
    TypeInfo valTypeInfo = typeInfo.getValueTypeInfo();

    Map newMap = new HashMap(value.size(), 1.0f);
    Iterator iter = value.entrySet().iterator();

    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();

      Object entryKey = transform(entry.getKey(), keyTypeInfo);
      Object entryValue = transform(entry.getValue(), valTypeInfo);

      newMap.put(entryKey, entryValue);
    }

    return newMap;
  }

  private static Struct transformStruct(Struct value, StructTypeInfo typeInfo) {
    List<Object> elements = new ArrayList<Object>();

    for (int i = 0; i < typeInfo.getFieldCount(); ++i) {
      TypeInfo fieldTypeInfo = value.getFieldTypeInfo(i);
      elements.add(transform(value.getFieldValue(i), fieldTypeInfo));
    }

    return new SimpleStruct(typeInfo, elements);
  }

  static Object transform(Object value, TypeInfo typeInfo) {
    return transform(value, typeInfo, true);
  }

  static Object transform(Object value, TypeInfo typeInfo, boolean strict) {
    if (value == null) {
      return null;
    }

    switch (typeInfo.getOdpsType()) {
      case STRING:
        if (value instanceof byte []) {
          // this convert would happened on embedded STRING in MAP/ARRAY/STRUCT
          // raw STRING would not convert byte array to string, it handles in ArrayRecord.set
          value = ArrayRecord.bytesToString((byte []) value);
        }
        if (strict) {
          validateString((String) value);
        }
        break;
      case BIGINT:
        validateBigint((Long) value);
        break;
      case DATETIME:
        if (strict) {
          validateDateTime((java.util.Date) value);
        }
        break;
      case DECIMAL:
        validateDecimal((BigDecimal) value, (DecimalTypeInfo) typeInfo);
        break;
      case CHAR:
        validateChar((Char) value, (CharTypeInfo) typeInfo);
        break;
      case VARCHAR:
        validateVarChar((Varchar) value, (VarcharTypeInfo) typeInfo);
        break;
      case ARRAY:
        return transformArray((List) value, (ArrayTypeInfo) typeInfo);
      case MAP:
        return transformMap((Map) value, (MapTypeInfo) typeInfo);
      case STRUCT:
        return transformStruct((Struct) value, (StructTypeInfo) typeInfo);
      default:
    }

    return odpsTypeToJavaType(typeInfo.getOdpsType()).cast(value);
  }
}
