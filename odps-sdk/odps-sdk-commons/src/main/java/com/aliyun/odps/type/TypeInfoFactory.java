package com.aliyun.odps.type;

import java.util.HashMap;
import java.util.List;

import com.aliyun.odps.OdpsType;

/**
 * Odps type 工厂类
 *
 * Created by zhenhong.gzh on 16/7/11.
 */
public class TypeInfoFactory {
  private TypeInfoFactory() {
  }

  public static final PrimitiveTypeInfo
      BOOLEAN = new SimplePrimitiveTypeInfo(OdpsType.BOOLEAN);

  public static final PrimitiveTypeInfo
      VOID = new SimplePrimitiveTypeInfo(OdpsType.VOID);

  public static final PrimitiveTypeInfo
      TINYINT = new SimplePrimitiveTypeInfo(OdpsType.TINYINT);
  public static final PrimitiveTypeInfo
      SMALLINT = new SimplePrimitiveTypeInfo(OdpsType.SMALLINT);
  public static final PrimitiveTypeInfo INT = new SimplePrimitiveTypeInfo(OdpsType.INT);
  public static final PrimitiveTypeInfo
      BIGINT = new SimplePrimitiveTypeInfo(OdpsType.BIGINT);
  public static final PrimitiveTypeInfo
      FLOAT = new SimplePrimitiveTypeInfo(OdpsType.FLOAT);
  public static final PrimitiveTypeInfo
      DOUBLE = new SimplePrimitiveTypeInfo(OdpsType.DOUBLE);

  public static final PrimitiveTypeInfo
      STRING = new SimplePrimitiveTypeInfo(OdpsType.STRING);

  public static final PrimitiveTypeInfo
      DATE = new SimplePrimitiveTypeInfo(OdpsType.DATE);
  public static final PrimitiveTypeInfo
      DATETIME = new SimplePrimitiveTypeInfo(OdpsType.DATETIME);
  public static final PrimitiveTypeInfo
      TIMESTAMP = new SimplePrimitiveTypeInfo(OdpsType.TIMESTAMP);
  public static final PrimitiveTypeInfo
      TIMESTAMP_NTZ = new SimplePrimitiveTypeInfo(OdpsType.TIMESTAMP_NTZ);

  public static final PrimitiveTypeInfo
      BINARY = new SimplePrimitiveTypeInfo(OdpsType.BINARY);

  public static final PrimitiveTypeInfo
      INTERVAL_DAY_TIME = new SimplePrimitiveTypeInfo(OdpsType.INTERVAL_DAY_TIME);

  public static final PrimitiveTypeInfo
      INTERVAL_YEAR_MONTH = new SimplePrimitiveTypeInfo(OdpsType.INTERVAL_YEAR_MONTH);

  public static final PrimitiveTypeInfo
      UNKNOWN = new SimplePrimitiveTypeInfo(OdpsType.UNKNOWN);

  public static final DecimalTypeInfo
      DECIMAL = new DecimalTypeInfo();

  public static final PrimitiveTypeInfo
      JSON = new SimplePrimitiveTypeInfo(OdpsType.JSON);

  private static HashMap<OdpsType, PrimitiveTypeInfo> typeInfoMap = new HashMap<OdpsType, PrimitiveTypeInfo>();

  static {
    typeInfoMap.put(BOOLEAN.getOdpsType(), BOOLEAN);
    typeInfoMap.put(VOID.getOdpsType(), VOID);
    typeInfoMap.put(TINYINT.getOdpsType(), TINYINT);
    typeInfoMap.put(SMALLINT.getOdpsType(), SMALLINT);
    typeInfoMap.put(INT.getOdpsType(), INT);
    typeInfoMap.put(BIGINT.getOdpsType(), BIGINT);
    typeInfoMap.put(FLOAT.getOdpsType(), FLOAT);
    typeInfoMap.put(DOUBLE.getOdpsType(), DOUBLE);
    typeInfoMap.put(STRING.getOdpsType(), STRING);
    typeInfoMap.put(DATE.getOdpsType(), DATE);
    typeInfoMap.put(DATETIME.getOdpsType(), DATETIME);
    typeInfoMap.put(TIMESTAMP.getOdpsType(), TIMESTAMP);
    typeInfoMap.put(TIMESTAMP_NTZ.getOdpsType(), TIMESTAMP_NTZ);
    typeInfoMap.put(BINARY.getOdpsType(), BINARY);
    typeInfoMap.put(INTERVAL_DAY_TIME.getOdpsType(), INTERVAL_DAY_TIME);
    typeInfoMap.put(INTERVAL_YEAR_MONTH.getOdpsType(), INTERVAL_YEAR_MONTH);
    typeInfoMap.put(UNKNOWN.getOdpsType(), UNKNOWN);

    typeInfoMap.put(DECIMAL.getOdpsType(), DECIMAL);
    typeInfoMap.put(JSON.getOdpsType(), JSON);
  }

  public static PrimitiveTypeInfo getPrimitiveTypeInfo(OdpsType odpsType) {
    PrimitiveTypeInfo typeInfo = typeInfoMap.get(odpsType);
    if (typeInfo != null) {
      return typeInfo;
    } else {
      // Not found in the cache. Must be parameterized types.
      throw new IllegalArgumentException("Error get PrimitiveTypeInfo instance for: " + odpsType);
    }
  }

  public static ArrayTypeInfo getArrayTypeInfo(TypeInfo valueType) {
    // do not check key, value type here.
    // compiler will do the check.
    return new SimpleArrayTypeInfo(valueType);
  }

  public static MapTypeInfo getMapTypeInfo(TypeInfo keyType, TypeInfo valueType) {
    // do not check key, value type here.
    return new SimpleMapTypeInfo(keyType, valueType);
  }

  public static CharTypeInfo getCharTypeInfo(int length) {
    return new CharTypeInfo(length);
  }

  public static VarcharTypeInfo getVarcharTypeInfo(int length) {
    return new VarcharTypeInfo(length);
  }

  public static DecimalTypeInfo getDecimalTypeInfo(int precision, int scale) {
    if ((precision == DecimalTypeInfo.DEFAULT_PRECISION) && (scale == DecimalTypeInfo.DEFAULT_SCALE)) {
      return DECIMAL;
    }
    return new DecimalTypeInfo(precision, scale);
  }

  public static StructTypeInfo getStructTypeInfo(List<String> names, List<TypeInfo> typeInfos) {
    return new SimpleStructTypeInfo(names, typeInfos);
  }
}
