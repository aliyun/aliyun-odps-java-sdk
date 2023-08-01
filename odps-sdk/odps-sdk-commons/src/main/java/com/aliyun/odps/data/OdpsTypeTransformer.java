package com.aliyun.odps.data;

import static com.aliyun.odps.data.ArrayRecord.DEFAULT_CALENDAR;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
  private static final int UTF8_ENCODED_CHAR_MAX_SIZE = 6;

  /**
   * The datetime range limited by storage, from -001-12-31 00:00:00 +0000 to
   * 10000-01-02 00:00:00 +0000.
   */
  private static final long DATETIME_MAX_TICKS = 253402387200000L;
  private static final long DATETIME_MIN_TICKS = -62167305600000L;

  static final Map<OdpsType, Class> ODPS_TYPE_MAPPER = new HashMap<>();
  static final Map<OdpsType, Class> ODPS_TYPE_MAPPER_V2 = new HashMap<>();


  OdpsTypeTransformer() {}

  static {
    ODPS_TYPE_MAPPER.put(OdpsType.BIGINT, Long.class);
    ODPS_TYPE_MAPPER.put(OdpsType.STRING, String.class);
    ODPS_TYPE_MAPPER.put(OdpsType.DATETIME, Date.class);
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
    ODPS_TYPE_MAPPER.put(OdpsType.TIMESTAMP, Timestamp.class);
    ODPS_TYPE_MAPPER.put(OdpsType.FLOAT, Float.class);
    ODPS_TYPE_MAPPER.put(OdpsType.CHAR, Char.class);
    ODPS_TYPE_MAPPER.put(OdpsType.BINARY, Binary.class);
    ODPS_TYPE_MAPPER.put(OdpsType.VARCHAR, Varchar.class);
    ODPS_TYPE_MAPPER.put(OdpsType.INTERVAL_YEAR_MONTH, IntervalYearMonth.class);
    ODPS_TYPE_MAPPER.put(OdpsType.INTERVAL_DAY_TIME, IntervalDayTime.class);
    ODPS_TYPE_MAPPER.put(OdpsType.JSON, JsonValue.class);

    // fix date type mapping
    ODPS_TYPE_MAPPER_V2.putAll(ODPS_TYPE_MAPPER);
    ODPS_TYPE_MAPPER_V2.put(OdpsType.DATE, LocalDate.class);
    ODPS_TYPE_MAPPER_V2.put(OdpsType.TIMESTAMP, Instant.class);
    ODPS_TYPE_MAPPER_V2.put(OdpsType.DATETIME, ZonedDateTime.class);
  }

  @Deprecated
  public static Class odpsTypeToJavaType(OdpsType type) {
    return odpsTypeToJavaType(ODPS_TYPE_MAPPER_V2, type);
  }

  public static Class odpsTypeToJavaType(Map<OdpsType, Class> mapper, OdpsType type) {
    if (mapper.containsKey(type)) {
      return mapper.get(type);
    }

    throw new IllegalArgumentException("Cannot get Java type for Odps type: " + type);
  }

  private static void validateString(String value, long limit) {
    try {
      if (value.length() * UTF8_ENCODED_CHAR_MAX_SIZE > limit
          && value.getBytes("utf-8").length > limit) {
        throw new IllegalArgumentException(
            "InvalidData: The string's length is more than " + limit + " bytes.");
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
    validateDateTime(value.getTime());
  }

  private static void validateDateTime(ZonedDateTime value) {
    validateDateTime(value.toInstant().toEpochMilli());
  }

  private static void validateDateTime(long epochMilli) {
    if (epochMilli < DATETIME_MIN_TICKS || epochMilli > DATETIME_MAX_TICKS) {
      throw new IllegalArgumentException(String.format("InvalidData: Datetime(%s) out of range.", epochMilli));
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

  static <T> T getCompatibleType(Object value, TypeInfo typeInfo) {
    return getCompatibleType(value, typeInfo, null);
  }

  static <T> T getCompatibleType(Object value, TypeInfo typeInfo, Calendar calendar) {
    return transform(value, typeInfo, calendar, false, true, false, -1);
  }

  static <T> T getOriginalType(Object value, TypeInfo typeInfo) {
    return transform(value, typeInfo, null, false, false, false, -1);
  }

  static <T> T transformAndValidate(Object value, TypeInfo typeInfo, Calendar calendar, boolean strict, long fieldMaxSize) {
    // allow byte [] to set on STRING column, ugly
    if (typeInfo.getOdpsType() == OdpsType.STRING && (value instanceof byte[])) {
      return (T) value;
    }

    return transform(value, typeInfo, calendar, true, false, strict, fieldMaxSize);
  }

  /**
   * get/set transform and validate method
   *
   * @param value       value need to be transform
   * @param typeInfo    value's typeinfo
   * @param calendar    for getDate(idx, calendar)
   * @param setData     boolean
   *                    true  - use by setXXX
   *                          1. need validate
   *                          2. transform byte[] => string in complex type
   *                          3. transform Date+Calender => LocalDate
   *                    false - use by getXXX
   * @param compatible
   *        transform new type(LocalDate/ZonedDatetime/Instant) to old type(java.sql.Date/Date/Timestamp) use by getXXX
   * @param strict
   *          1. validate string size < fieldMaxSize
   *          2. validate datetime range [] (//todo why timestamp and date not validate?)
   * @param fieldMaxSize max string size
   *
   *        Transform Table:         (use in all set/setType method)
   *        original                 typeInfo                 compatible    calendar      result
   *        ------------------------------------------------------------------------------------------
   *        String                   STRING                   false         -             String
   *        byte[]                   STRING in complex type   false         -             String
   *        *byte[]*                 STRING                   false         -             byte[]
   *        java.util.Date           DATETIME                 false         -             java.util.Date
   *        ZonedDatetime            DATETIME                 false         -             ZonedDatetime
   *        java.sql.Date + null     DATE                     false         -             java.sql.Date
   *        java.sql.Date + calender DATE                     false         -             LocalDate
   *        LocalDate                DATE                     false         -             LocalDate
   *        Timestamp                Timestamp                false         -             Timestamp
   *        Instant                  Timestamp                false         -             Instant
   *
   *
   *        get => return object directly(no transform)
   *
   *        getXXX Transform Table:(use in getDate/getDatetime/getTimestamp/getArray/getMap/getStruct)
   *        original         typeInfo                   result
   *        -----------------------------------------------------------
   *        ZonedDatetime    DATETIME                   java.util.Date
   *        LocalDate        DATE                       java.sql.Date
   *        Instant          Timestamp                  Timestamp
   *
   *
   * @return transformed result
   */
  static <T> T transform(Object value,
                         TypeInfo typeInfo,
                         Calendar calendar,
                         boolean setData,
                         boolean compatible,
                         boolean strict,
                         long fieldMaxSize) {
    if (value == null) {
      return null;
    }

    // 1. transform
    Object transformedResult = value;
    Map<OdpsType, Class> transformMapper = ODPS_TYPE_MAPPER;
    switch (typeInfo.getOdpsType()) {
      case JSON:
        if (setData) {
          if (strict) {
            validateString(transformedResult.toString(), fieldMaxSize);
          }
        }
        break;
      case STRING:
        if (setData) {
          if (value instanceof byte[]) {
            // this convert would happen on embedded STRING in MAP/ARRAY/STRUCT
            // raw STRING would not convert byte array to string, it handles in ArrayRecord.set
            transformedResult = ArrayRecord.bytesToString((byte[]) value);
          }
          if (strict) {
            validateString((String) transformedResult, fieldMaxSize);
          }
        }
        break;
      case BIGINT:
        if (setData) {
          validateBigint((Long) transformedResult);
        }
        break;
      case DECIMAL:
        if (setData) {
          validateDecimal((BigDecimal) transformedResult, (DecimalTypeInfo) typeInfo);
        }
        break;
      case CHAR:
        if (setData) {
          validateChar((Char) transformedResult, (CharTypeInfo) typeInfo);
        }
        break;
      case VARCHAR:
        if (setData) {
          validateVarChar((Varchar) transformedResult, (VarcharTypeInfo) typeInfo);
        }
        break;
      case DATETIME:
        if (setData) {
          if (transformedResult instanceof ZonedDateTime) {
            transformMapper = ODPS_TYPE_MAPPER_V2;
            validateDateTime((ZonedDateTime) transformedResult);
          } else {
            validateDateTime((Date) transformedResult);
          }
        } else {
          if (compatible) {
            if (value instanceof ZonedDateTime) {
              transformedResult = Date.from(((ZonedDateTime) value).toInstant());
            }
          } else {
            // 返回原始数据
            if (value instanceof ZonedDateTime) {
              transformMapper = ODPS_TYPE_MAPPER_V2;
            }
          }
        }
        break;
      case DATE:
        if (setData) {
          if (value instanceof LocalDate) {
            transformMapper = ODPS_TYPE_MAPPER_V2;
          } else {
            if (calendar != null) {
              // setDate(date, calendar) => LocalDate
              // date + calendar => LocalDate
              transformMapper = ODPS_TYPE_MAPPER_V2;
              transformedResult = dateToLocalDate((java.sql.Date) value, calendar);
            }
            // else setDate
          }
        } else {
          if (compatible) {
            if (value instanceof LocalDate) {
              transformedResult = localDateToDate((LocalDate) value, calendar);
            }
          } else {
            if (value instanceof LocalDate) {
              transformMapper = ODPS_TYPE_MAPPER_V2;
            }
          }
        }
        break;
      case TIMESTAMP:
        if (setData) {
          if (value instanceof Instant) {
            transformMapper = ODPS_TYPE_MAPPER_V2;
          }
        } else {
          if (compatible) {
            if (value instanceof Instant) {
              transformedResult = Timestamp.from((Instant) value);
            }
          } else {
            if (value instanceof Instant) {
              transformMapper = ODPS_TYPE_MAPPER_V2;
            }
          }
        }
        break;
      case ARRAY:
        List arrayValue = (List) value;
        TypeInfo elementTypeInfo = ((ArrayTypeInfo)typeInfo).getElementTypeInfo();

        List<Object> newList = new ArrayList<>(arrayValue.size());
        for (Object obj : arrayValue) {
          newList.add(transform(obj, elementTypeInfo, calendar, setData, compatible, strict, fieldMaxSize));
        }
        transformedResult = newList;
        break;
      case MAP:
        TypeInfo keyTypeInfo = ((MapTypeInfo)typeInfo).getKeyTypeInfo();
        TypeInfo valTypeInfo = ((MapTypeInfo)typeInfo).getValueTypeInfo();
        Map map = (Map) value;

        Map newMap = new HashMap(map.size(), 1.0f);
        Iterator iter = map.entrySet().iterator();

        while (iter.hasNext()) {
          Map.Entry entry = (Map.Entry) iter.next();

          Object entryKey = transform(entry.getKey(), keyTypeInfo, calendar, setData, compatible, strict, fieldMaxSize);
          Object entryValue = transform(entry.getValue(), valTypeInfo, calendar, setData, compatible, strict, fieldMaxSize);

          newMap.put(entryKey, entryValue);
        }
        transformedResult = newMap;
        break;
      case STRUCT:
        Struct struct = (Struct) value;
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<Object> elements = new ArrayList<>();

        for (int i = 0; i < structTypeInfo.getFieldCount(); ++i) {
          TypeInfo fieldTypeInfo = struct.getFieldTypeInfo(i);
          elements.add(transform(struct.getFieldValue(i), fieldTypeInfo, calendar, setData, compatible, strict, fieldMaxSize));
        }
        transformedResult = new SimpleStruct(structTypeInfo, elements);
        break;
      default:

    }
    return (T) odpsTypeToJavaType(transformMapper, typeInfo.getOdpsType()).cast(transformedResult);
  }

  // 转换为 calendar 时区下的 LocalDate
  // default calendar use GMT,
  //
  // 不通过 ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), calendar.getTimeZone().toZoneId()).toLocalDate();
  // 这种方式转，这样转时间戳相同，但是 Date 可能因为 1900 前的 bug 导致日期读出来不一样
  @Deprecated
  public static LocalDate dateToLocalDate(java.sql.Date date, Calendar calendar) {
    if (calendar == null) {
      calendar = DEFAULT_CALENDAR;
    }

    calendar = (Calendar) calendar.clone();
    calendar.clear();
    calendar.setLenient(true);
    calendar.setTime(date);
    return IsoChronology.INSTANCE.date(
            //TODO remove this
            IsoChronology.INSTANCE.eraOf(calendar.get(Calendar.ERA)),
            calendar.get(Calendar.YEAR),
            calendar.get(Calendar.MONTH) + 1,
            calendar.get(Calendar.DAY_OF_MONTH));


  }

  // 转换为只有在 calender 的时区下才保证正确的 Date
  // default calendar use GMT
  //
  // 不能用保留时间戳的方式转换, 1900 前会有 5:43 的偏差（offset 不一致导致）
  // return java.util.Date.from(
  //         localDate.atStartOfDay(calendar.getTimeZone().toZoneId())
  //                 .toInstant());
  static java.sql.Date localDateToDate(LocalDate localDate, Calendar calendar) {
    if (calendar == null) {
      calendar = DEFAULT_CALENDAR;
    }
    //
    Calendar c = (Calendar) calendar.clone();
    c.clear();
    c.set(Calendar.YEAR, localDate.getYear());
    c.set(Calendar.MONTH, localDate.getMonthValue() - 1);
    c.set(Calendar.DAY_OF_MONTH, localDate.getDayOfMonth());
    return new java.sql.Date(c.getTime().getTime());
  }
}
