package com.aliyun.odps.utils;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsCommonUtils {

  public static String quoteRef(String str) {
    return "`" + str + "`";
  }

  public static String quoteStr(String str) {
    str = str.replace("'", "\\'");
    return "'" + str + "'";
  }

  /**
   * Infer the Odps type of object based on its Java Type
   * <p>
   * It should be noted that for char, varchar, and decimal,
   * they are inferred based on the precision, length of the obj and are not necessarily the same type as in the table.
   * <p>
   * For complex types (list, map, struct), since the information of the subtypes is currently unknown, it is not supported yet.
   */
  public static TypeInfo indicateTypeFromClass(Object obj) {
    if (obj == null) {
      return TypeInfoFactory.VOID;
    }
    if (obj instanceof Byte) {
      return TypeInfoFactory.TINYINT;
    } else if (obj instanceof Short) {
      return TypeInfoFactory.SMALLINT;
    } else if (obj instanceof Integer) {
      return TypeInfoFactory.INT;
    } else if (obj instanceof Long) {
      return TypeInfoFactory.BIGINT;
    } else if (obj instanceof Binary) {
      return TypeInfoFactory.BINARY;
    } else if (obj instanceof Float) {
      return TypeInfoFactory.FLOAT;
    } else if (obj instanceof Double) {
      return TypeInfoFactory.DOUBLE;
    } else if (obj instanceof BigDecimal) {
      return TypeInfoFactory.DECIMAL;
    } else if (obj instanceof Varchar) {
      return TypeInfoFactory.getVarcharTypeInfo(((Varchar) obj).length());
    } else if (obj instanceof Char) {
      return TypeInfoFactory.getCharTypeInfo(((Char) obj).length());
    } else if (obj instanceof String || obj instanceof byte[]) {
      return TypeInfoFactory.STRING;
    } else if (obj instanceof java.sql.Date || obj instanceof LocalDate) {
      return TypeInfoFactory.DATE;
    } else if (obj instanceof java.sql.Timestamp || obj instanceof Instant) {
      return TypeInfoFactory.TIMESTAMP;
    } else if (obj instanceof java.util.Date || obj instanceof ZonedDateTime) {
      return TypeInfoFactory.DATETIME;
    } else if (obj instanceof LocalDateTime) {
      return TypeInfoFactory.TIMESTAMP_NTZ;
    } else if (obj instanceof Boolean) {
      return TypeInfoFactory.BOOLEAN;
    } else {
      return TypeInfoFactory.UNKNOWN;
    }
  }
}
