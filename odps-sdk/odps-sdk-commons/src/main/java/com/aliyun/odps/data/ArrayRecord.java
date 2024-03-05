/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.data;

import static com.aliyun.odps.data.OdpsTypeTransformer.getCompatibleType;
import static com.aliyun.odps.data.OdpsTypeTransformer.transformAndValidate;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * 基于数组的{@link Record}实现
 *
 * @see Record
 */
public class ArrayRecord implements Record {
  private static final Long DEFAULT_FIELD_MAX_SIZE = 8 * 1024 * 1024L;
  private static final String DEFAULT_CHARSET = "utf-8";

  public static final Calendar DEFAULT_CALENDAR = new Calendar.Builder()
      .setCalendarType("iso8601")
      .setLenient(true)
      .setTimeZone(TimeZone.getTimeZone("GMT"))
      .build();

  private Column[] columns;
  private final Object[] values;
  private HashMap<String, Integer> nameMap = new HashMap<>();

  /**
   * When strictTypeValidation is enabled, max length of string and range of datetime are
   * restricted.
   */
  private boolean strictTypeValidation;
  /**
   * Field max size allowed.
   */
  private Long fieldMaxSize = DEFAULT_FIELD_MAX_SIZE;

  public ArrayRecord(Column[] columns) {
    this(columns, true);
  }

  public ArrayRecord(Column[] columns, boolean strictTypeValidation) {
    this(columns, strictTypeValidation, DEFAULT_FIELD_MAX_SIZE);
  }

  public ArrayRecord(Column[] columns, boolean strictTypeValidation, Long fieldMaxSize) {
    if (columns == null) {
      throw new IllegalArgumentException();
    }

    this.columns = columns;
    this.strictTypeValidation = strictTypeValidation;
    if (fieldMaxSize != null) {
      this.fieldMaxSize = fieldMaxSize;
    }

    values = new Object[columns.length];

    for (int i = 0; i < columns.length; i++) {
      nameMap.put(columns[i].getName(), i);
    }
  }

  public ArrayRecord(Column[] columns, Object[] values) {
    this(columns, values, true);
  }

  public ArrayRecord(Column[] columns, Object[] values, boolean strictTypeValidation){
    this(columns, strictTypeValidation);
    if (values.length != columns.length) {
      throw new IllegalArgumentException("Lengths of schema and column values of the Record mismatches.");
    }
    for (int i = 0 ; i < columns.length; i++){
      set(i, values[i]);
    }
  }

  public ArrayRecord(TableSchema schema) {
    this(schema, true);
  }

  public ArrayRecord(TableSchema schema, boolean strictTypeValidation) {
    this(schema.getColumns().toArray(new Column[0]), strictTypeValidation);
  }

  @Override
  public int getColumnCount() {
    return values.length;
  }

  @Override
  public Column[] getColumns() {
    return columns;
  }

  @Override
  public void set(int idx, Object value) {
    set(idx, value, null);
  }

  private void set(int idx, Object value, Calendar calendar) {
    values[idx] = transformAndValidate(
            value,
            columns[idx].getTypeInfo(),
            calendar,
            strictTypeValidation,
            fieldMaxSize);
  }

  @Override
  public Object get(int idx) {
    return values[idx];
  }

  @Override
  public void set(String columnName, Object value) {
    set(getColumnIndex(columnName), value);
  }

  @Override
  public Object get(String columnName) {
    return values[getColumnIndex(columnName)];
  }

  @Override
  public void setBigint(int idx, Long value) {
    set(idx, value);
  }

  @Override
  public Long getBigint(int idx) {
    return getInternal(idx);
  }

  @Override
  public void setBigint(String columnName, Long value) {
    setBigint(getColumnIndex(columnName), value);
  }

  @Override
  public Long getBigint(String columnName) {
    return getBigint(getColumnIndex(columnName));
  }

  @Override
  public void setDouble(int idx, Double value) {
    set(idx, value);
  }

  @Override
  public Double getDouble(int idx) {
    return getInternal(idx);
  }

  @Override
  public void setDouble(String columnName, Double value) {
    setDouble(getColumnIndex(columnName), value);
  }

  @Override
  public Double getDouble(String columnName) {
    return getDouble(getColumnIndex(columnName));
  }

  @Override
  public void setBoolean(int idx, Boolean value) {
    set(idx, value);
  }

  @Override
  public Boolean getBoolean(int idx) {
    return getInternal(idx);
  }

  @Override
  public void setBoolean(String columnName, Boolean value) {
    setBoolean(getColumnIndex(columnName), value);
  }

  @Override
  public Boolean getBoolean(String columnName) {
    return getBoolean(getColumnIndex(columnName));
  }

  public void setDatetime(int idx, Date value) {
    set(idx, value);
  }

  @Deprecated
  public Date getDatetime(int idx) {
    return OdpsTypeTransformer.getCompatibleType(getInternal(idx), TypeInfoFactory.DATETIME);
  }

  public ZonedDateTime getDatetimeAsZonedDateTime(int idx) {
    return getInternal(idx);
  }

  public ZonedDateTime getDatetimeAsZonedDateTime(String columnName) {
    return getInternal(getColumnIndex(columnName));
  }

  @Override
  public void setDatetime(String columnName, Date value) {
    setDatetime(getColumnIndex(columnName), value);
  }

  /**
   * Set datetime column, using a java.time.ZonedDateTime value.
   *
   * See the static factory methods of {@link ZonedDateTime} to learn how to construct a
   * {@link ZonedDateTime} object.
   *
   * <pre>
   * {@code
   *     DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(
   *         ZoneId.systemDefault());
   *     ZonedDateTime zonedDateTime = ZonedDateTime.parse(date, formatter);
   * }
   * </pre>
   *
   * @param columnName Column name
   * @param value A {@link ZonedDateTime} object. Could be null.
   */
  public void setDatetimeAsZonedDateTime(String columnName, ZonedDateTime value) {
    set(getColumnIndex(columnName), value);
  }

  /**
   * Set datetime column, using a java.time.ZonedDateTime value.
   *
   * See the static factory methods of {@link ZonedDateTime} to learn how to construct a
   * {@link ZonedDateTime} object.
   *
   * @param idx Column index
   * @param value A {@link ZonedDateTime} object. Could be null.
   */
  public void setDatetimeAsZonedDateTime(int idx, ZonedDateTime value) {
    set(idx,  value);
  }

  @Deprecated
  public Date getDatetime(String columnName) {
    return getDatetime(getColumnIndex(columnName));
  }

  @Override
  public void setDecimal(int idx, BigDecimal value) {
    set(idx, value);
  }

  @Override
  public BigDecimal getDecimal(int idx) {
    return getInternal(idx);
  }

  @Override
  public void setDecimal(String columnName, BigDecimal value) {
    setDecimal(getColumnIndex(columnName), value);
  }

  @Override
  public BigDecimal getDecimal(String columnName) {
    return getDecimal(getColumnIndex(columnName));
  }

  @Override
  public void setString(int idx, String value) {
    set(idx, value);
  }

  @Override
  public String getString(int idx) {
    Object obj = values[idx];

    if (obj == null) {
      return null;
    }

    if (obj instanceof byte []) {
      return bytesToString((byte []) obj);
    }

    return getInternal(idx);
  }

  @Override
  public void setString(String columnName, String value) {
    setString(getColumnIndex(columnName), value);
  }

  @Override
  public String getString(String columnName) {
    return getString(getColumnIndex(columnName));
  }

  @Override
  public void setString(int idx, byte[] value) {
    set(idx, value);
  }

  @Override
  public void setString(String columnName, byte[] value) {
    setString(getColumnIndex(columnName), value);
  }

  @Override
  public byte[] getBytes(int idx) {
    Object obj = values[idx];

    if (obj == null) {
      return null;
    }
    if (obj instanceof  byte[]) {
      return (byte[]) obj;
    } else if (obj instanceof String) {
      return stringToBytes((String) obj);
    } else if (obj instanceof Binary) {
      return ((Binary) obj).data();
    } else if (obj instanceof AbstractChar) {
        return stringToBytes(((AbstractChar)obj).getValue());
    }
    else {
      throw new RuntimeException("Does not support getBytes for type other than String/Binary/Char/VarChar, sees "
              + obj.getClass());
    }
  }

  @Override
  public byte[] getBytes(String columnName) {
    return getBytes(getColumnIndex(columnName));
  }

  public void setBinary(int idx, Binary value) {
    set(idx, value);
  }

  public void setBinary(String columnName, Binary value) {
    setBinary(getColumnIndex(columnName), value);
  }

  public Binary getBinary(int idx) {
    return getInternal(idx);
  }

  public Binary getBinary(String columnName) {
    return getBinary(getColumnIndex(columnName));
  }

  @Override
  public void set(Object[] values) {
    if (values == null || columns.length != values.length) {
      throw new IllegalArgumentException();
    }
    for (int i = 0; i < values.length; ++i) {
      set(i, values[i]);
    }
  }

  @Override
  public Object[] toArray() {
    return values;
  }

  private int getColumnIndex(String name) {
    Integer idx = nameMap.get(name);
    if (idx == null) {
      throw new IllegalArgumentException("No such column:" + name);
    }
    return idx;
  }

  public void clear() {
    for (int i = 0; i < values.length; i++) {
      values[i] = null;
    }
  }

  public void setArray(String columnName, List list) {
    setArray(getColumnIndex(columnName), list);
  }

  /**
   * 设置对应索引列的值，该列是 Array 类型
   *
   * @param idx
   *     列索引
   * @param list 注意，String 将被转换为 byte[] 进行存储，转换过程使用 UTF-8 编码
   *     列值
   */

  public void setArray(int idx, List list) {
    set(idx, list);
  }

  /**
   * 设置对应索引列的值，该列是 Map 类型
   *
   * @param idx
   *     列索引
   * @param map Map 中 String 类型的 key/value 将被转换为 byte[] 存储，转换过程使用 UTF-8 编码
   *     列值
   */
  public void setMap(int idx, Map map) {
    set(idx, map);
  }

  public void setMap(String columnName, Map map) {
    setMap(getColumnIndex(columnName), map);
  }

  public <T> List<T> getArray(Class<T> className, String columnName) {
    return getArray(className, getColumnIndex(columnName));
  }

  /**
   * 获取对应索引列的值，该列是 Array 类型
   *
   * @param <T>
   *     返回 List 的元素类型，当类型为 String 时，内部存储的 byte[] 将使用 UTF-8 编码为 String 并返回
   * @param className
   *     返回 List 的元素类型名称
   * @param idx
   *     列索引值
   * @return Array 类型的列值
   */
  public <T> List<T> getArray(Class<T> className, int idx) {
    List list;
    if (isNewDatetimeType(className)) {
      list = OdpsTypeTransformer.getOriginalType(getInternal(idx), columns[idx].getTypeInfo());
    } else {
      list = getArray(idx);
    }

    if (list == null || list.isEmpty()) {
      return list;
    }

    List<T> newList = new ArrayList<T>(list.size());
    for (Object obj : list) {
      obj = transformUserCustomType(className, obj);
      newList.add(className.cast(obj));
    }
    return newList;
  }

  public List getArray(String columnName) {
    return getArray(getColumnIndex(columnName));
  }

  public List getArray(int idx) {
    return OdpsTypeTransformer.getCompatibleType(getInternal(idx), columns[idx].getTypeInfo());
  }

  public <k, v> Map<k, v> getMap(Class<k> keyClass, Class<v> valueClass, String columnName) {
    return getMap(keyClass, valueClass, getColumnIndex(columnName));
  }

  /**
   * 获取对应索引列的值，该列是 Map 类型
   *
   * @param <k>
   *     待获取的 map 中 key 的类型，当类型为 String 时，内部存储的 byte[] 将使用 UTF-8 编码为 String 并返回
   * @param <v>
   *     待获取的 map 中 value 的类型，当类型为 String 时，内部存储的 byte[] 将使用 UTF-8 编码为 String 并返回
   * @param keyClass
   *     待获取的 map 中 key 的类型名称
   * @param valueClass
   *     待获取的 map 中 value 的类型名称
   * @param idx
   *     列索引值
   * @return Map 类型的列值
   */
  public <k, v> Map<k, v> getMap(Class<k> keyClass, Class<v> valueClass, int idx) {
    Map map;
    if (isNewDatetimeType(keyClass) && isOldDatetimeType(valueClass)) {
      throw new IllegalArgumentException("key type and value type conflict");
    } else if (isOldDatetimeType(keyClass) && isNewDatetimeType(valueClass)) {
      throw new IllegalArgumentException("key type and value type conflict");
    } else if (isNewDatetimeType(keyClass) && isNewDatetimeType(valueClass)) {
      map = OdpsTypeTransformer.getOriginalType(getInternal(idx), columns[idx].getTypeInfo());
    } else {
      map = getMap(idx);
    }

    if (map == null || map.isEmpty()) {
      return map;
    }

    Map<k, v> newMap = new HashMap<k, v>(map.size(), 1.0f);
    Iterator iter = map.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      Object key = entry.getKey();
      Object value = entry.getValue();

      key = transformUserCustomType(keyClass, key);
      value = transformUserCustomType(valueClass, value);

      newMap.put(keyClass.cast(key), valueClass.cast(value));
    }

    return newMap;
  }

  private boolean isNewDatetimeType(Class c) {
    return c == ZonedDateTime.class || c == LocalDate.class || c == Instant.class;
  }

  private boolean isOldDatetimeType(Class c) {
    return c == Date.class || c == java.sql.Date.class || c == Timestamp.class;
  }

  private <T> Object transformUserCustomType(Class<T> className, Object o) {
    if (className == byte[].class && o instanceof String) {
      return stringToBytes((String) o);
    } else {
      return o;
    }
  }

  public Map getMap(String columnName) {
    return getMap(getColumnIndex(columnName));
  }

  public Map getMap(int idx) {
    return OdpsTypeTransformer.getCompatibleType(getInternal(idx), columns[idx].getTypeInfo());
  }

  public void setChar(int idx, Char value) {
    set(idx, value);
  }

  public void setChar(String columnName, Char value) {
    setChar(getColumnIndex(columnName), value);
  }

  public Char getChar(int idx) {
    return getInternal(idx);
  }

  public Char getChar(String columnName) {
    return getChar(getColumnIndex(columnName));
  }

  public void setVarchar(int idx, Varchar value) {
    set(idx, value);
  }

  public void setVarchar(String columnName, Varchar value) {
    setVarchar(getColumnIndex(columnName), value);
  }

  public Varchar getVarchar(int idx) {
    return getInternal(idx);
  }

  public Varchar getVarchar(String columnName) {
    return getVarchar(getColumnIndex(columnName));
  }

  /**
   * This method is error-prone and deprecated. See {@link #setDate(int, java.sql.Date, Calendar)}
   * for an alternative.
   *
   * Set the value of the designated column to the given {@link java.sql.Date} object.
   *
   * <b>IMPORTANT: </b>The {@link java.sql.Date} object must be constructed using the Greenwich
   * Mean Time. And here is the recommended way to construct a valid java.sql.Date object:
   *
   * <pre>
   * {@code
   * Calendar calendar = new Calendar.Builder()
   *       .setCalendarType("iso8601")
   *       .setLenient(true)
   *       .setTimeZone(TimeZone.getTimeZone("GMT"))
   *       .build();
   * calendar.set(year, month, dayOfMonth);
   * java.sql.Date date = new java.sql.Date(calendar.getTimeInMillis());
   * }
   * </pre>
   *
   * @param idx Column index.
   * @param value Date.
   */
  @Deprecated
  public void setDate(int idx, java.sql.Date value) {
    setDate(idx, value, null);
  }

  /**
   * This method is error-prone and deprecated. See {@link #getDate(int, Calendar)}
   * for an alternative.
   *
   * Get the value of the designated column as a {@link java.sql.Date} object.
   *
   * <b>IMPORTANT: </b>The {@link java.sql.Date} object is constructed using
   * the Greenwich Mean Time. And here is the recommended way to get the year, month and date from
   * the returned value:
   *
   * <pre>
   * {@code
   * java.sql.Date date = record.getDate(columnIdx);
   * Calendar calendar = new Calendar.Builder()
   *       .setCalendarType("iso8601")
   *       .setLenient(true)
   *       .setTimeZone(TimeZone.getTimeZone("GMT"))
   *       .build();
   * calendar.setTime(date);
   * int year = calendar.get(Calendar.YEAR);
   * int month = calendar.get(Calendar.MONTH);
   * int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
   * }
   * </pre>
   *
   * @param idx Column index.
   * @return Date.
   */
  @Deprecated
  public java.sql.Date getDate(int idx) {
    return getDate(idx, null);
  }

  /**
   * This method is error-prone and deprecated. See {@link #setDate(String, java.sql.Date, Calendar)}
   * for an alternative.
   *
   * <b>IMPORTANT: </b> The {@link java.sql.Date} object must be constructed using the Greenwich
   * Mean Time. And here is the recommended way to construct a valid java.sql.Date object:
   *
   * <pre>
   * {@code
   * Calendar calendar = new Calendar.Builder()
   *       .setCalendarType("iso8601")
   *       .setLenient(true)
   *       .setTimeZone(TimeZone.getTimeZone("GMT"))
   *       .build();
   * calendar.set(year, month, dayOfMonth);
   * java.sql.Date date = new java.sql.Date(calendar.getTimeInMillis());
   * }
   * </pre>
   *
   * @param columnName Column name.
   * @param value A {@link java.sql.Date} object. Could be null.
   */
  @Deprecated
  public void setDate(String columnName, java.sql.Date value) {
    setDate(getColumnIndex(columnName), value);
  }

  /**
   * This method is error-prone and deprecated. See {@link #getDate(String, Calendar)}
   * for an alternative.
   *
   * Get the value of the designated column as a {@link java.sql.Date} object.
   *
   * <b>IMPORTANT: </b>The {@link java.sql.Date} object is constructed using
   * the Greenwich Mean Time. And here is the recommended way to get the year, month and date from
   * the returned value:
   *
   * <pre>
   * {@code
   * java.sql.Date date = record.getDate(columnIdx);
   * Calendar calendar = new Calendar.Builder()
   *       .setCalendarType("iso8601")
   *       .setLenient(true)
   *       .setTimeZone(TimeZone.getTimeZone("GMT"))
   *       .build();
   * calendar.setTime(date);
   * int year = calendar.get(Calendar.YEAR);
   * int month = calendar.get(Calendar.MONTH);
   * int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
   * }
   * </pre>
   *
   * @param columnName Column name.
   * @return A {@link java.sql.Date} object. If the value is SQL <code>NULL</code>, the value
   *         returned is <code>null</code>.
   */
  @Deprecated
  public java.sql.Date getDate(String columnName) {
    return getDate(getColumnIndex(columnName));
  }

  /**
   * Set the value of the designated column to the given {@link java.sql.Date} object, using the
   * given {@link Calendar} object. With the {@link Calendar} object, the date can be calculated
   * taking into account a custom timezone. If no {@link Calendar} is specified, Greenwich Mean
   * Time is used as the default timezone.
   *
   * Here is an example of constructing a calendar:
   * <pre>
   * {@code
   * Calendar calendar = new Calendar.Builder()
   *       .setCalendarType("iso8601")
   *       .setLenient(true)
   *       .setTimeZone(TimeZone.getTimeZone("GMT"))
   *       .build();
   * }
   * </pre>
   *
   *
   * @param columnName Column name.
   * @param value Date.
   * @param calendar The calendar to use in constructing the final {@link java.sql.Date} object.
   */
  @Deprecated
  public void setDate(String columnName, java.sql.Date value, Calendar calendar) {
    setDate(getColumnIndex(columnName), value, calendar);
  }

  /**
   * Get the value of the designated column as a {@link java.sql.Date} object. This method uses the
   * given calendar to construct an appropriate {@link java.sql.Date} object taking into account a
   * custom timezone. If no {@link Calendar} is specified, Greenwich Mean Time is used as the
   * default timezone.
   *
   * <b>IMPORTANT: </b>The timestamp in the returned {@link java.sql.Date} may be different from
   * the original one, but the year, the month and the day of month are guaranteed to be the same.
   *
   * Here is an example of constructing a calendar:
   * <pre>
   * {@code
   * Calendar calendar = new Calendar.Builder()
   *       .setCalendarType("iso8601")
   *       .setLenient(true)
   *       .setTimeZone(TimeZone.getTimeZone("GMT"))
   *       .build();
   * }
   * </pre>
   *
   * @param columnName Column name.
   * @param calendar The calendar to use in constructing the return value.
   * @return
   */
  @Deprecated
  public java.sql.Date getDate(String columnName, Calendar calendar) {
    return getDate(getColumnIndex(columnName), calendar);
  }

  /**
   * Set the value of the designated column to the given {@link java.sql.Date} object, using the
   * given {@link Calendar} object. With the {@link Calendar} object, the date can be calculated
   * taking into account a custom timezone. If no {@link Calendar} is specified, Greenwich Mean
   * Time is used as the default timezone.
   *
   * Here is an example of constructing a calendar:
   * <pre>
   * {@code
   * Calendar calendar = new Calendar.Builder()
   *       .setCalendarType("iso8601")
   *       .setLenient(true)
   *       .setTimeZone(TimeZone.getTimeZone("GMT"))
   *       .build();
   * }
   * </pre>
   *
   * @param idx Column index.
   * @param value Date.
   * @param calendar The calendar to use in constructing the final {@link java.sql.Date} object.
   */
  @Deprecated
  public void setDate(int idx, java.sql.Date value, Calendar calendar) {
    set(idx, value, calendar);
  }

  /**
   * Get the value of the designated column as a {@link java.sql.Date} object. This method uses the
   * given calendar to construct an appropriate {@link java.sql.Date} object taking into account a
   * custom timezone. If no {@link Calendar} is specified, Greenwich Mean Time is used as the
   * default timezone.
   *
   * <b>IMPORTANT: </b>The timestamp in the returned {@link java.sql.Date} may be different from
   * the original one, but the year, the month and the day of month are guaranteed to be the same.
   *
   * Here is an example of constructing a calendar:
   * <pre>
   * {@code
   * Calendar calendar = new Calendar.Builder()
   *       .setCalendarType("iso8601")
   *       .setLenient(true)
   *       .setTimeZone(TimeZone.getTimeZone("GMT"))
   *       .build();
   * }
   * </pre>
   *
   * @param idx Column index.
   * @param calendar The calendar to use in constructing the return value.
   * @return
   */
  @Deprecated
  public java.sql.Date getDate(int idx, Calendar calendar) {
    return getCompatibleType(getInternal(idx), TypeInfoFactory.DATE, calendar);
  }

  /**
   * Set the value of the designated column as a {@link LocalDate} object. The object will be
   * converted to a SQL <code>DATE</code> value when it is sent to MaxCompute.
   *
   * See the static factory methods of {@link LocalDate} to learn how to construct a
   * {@link LocalDate} object.
   *
   * @param columnIdx Column index.
   * @param localDate A {@link LocalDate} object. Could be null.
   */
  public void setDateAsLocalDate(int columnIdx, LocalDate localDate) {
    set(columnIdx, localDate);
  }

  /**
   * Set the value of the designated column as a {@link LocalDate} object. The object will be
   * converted to a SQL <code>DATE</code> value when it is sent to MaxCompute.
   *
   * See the static factory methods of {@link LocalDate} to learn how to construct a
   * {@link LocalDate} object.
   *
   * @param columnName Column name.
   * @param localDate A {@link LocalDate} object. Could be null.
   */
  public void setDateAsLocalDate(String columnName, LocalDate localDate) {
    setDateAsLocalDate(getColumnIndex(columnName), localDate);
  }

  /**
   * Get the value of the designated column as a {@link LocalDate} object.
   *
   * @param columnIdx Column index.
   * @return A {@link LocalDate} object. If the value is SQL <code>NULL</code>, the value returned
   *         is <code>null</code>.
   */
  public LocalDate getDateAsLocalDate(int columnIdx) {
    return getInternal(columnIdx);
  }

  /**
   * Get the value of the designated column as a {@link LocalDate} object.
   *
   * @param columnName Column name.
   * @return A {@link LocalDate} object. If the value is SQL <code>NULL</code>, the value returned
   *         is <code>null</code>.
   */
  public LocalDate getDateAsLocalDate(String columnName) {
    return getDateAsLocalDate(getColumnIndex(columnName));
  }

  public void setTimestamp(int idx, Timestamp value) {
    set(idx, value);
  }

  /**
   * Set timestamp data, using a java.time.Instant value.
   *
   * See the static factory methods of {@link Instant} to learn how to construct a
   * {@link Instant} object.
   *
   * <pre>
   * {@code
   *     DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(
   *         ZoneId.systemDefault());
   *     Instant instant = ZonedDateTime.parse(date, formatter).toInstant();
   * }
   * </pre>
   *
   * @param idx Column index
   * @param instant A {@link Instant} object. Could be null.
   */
  public void setTimestampAsInstant(int idx, Instant instant) {
    set(idx, instant);
  }

  /**
   * Set timestamp data, using a java.time.Instant value.
   *
   * See the static factory methods of {@link Instant} to learn how to construct a
   * {@link Instant} object.
   *
   * @param columnName Column name
   * @param instant A {@link Instant} object. Could be null.
   */
  public void setTimestampAsInstant(String columnName, Instant instant) {
    setTimestampAsInstant(getColumnIndex(columnName), instant);
  }

  public Instant getTimestampAsInstant(int idx) {
    return getInternal(idx);
  }

  public Instant getTimestampAsInstant(String columnName) {
    return getInternal(getColumnIndex(columnName));
  }

  public Timestamp getTimestamp(int idx) {
    return OdpsTypeTransformer.getCompatibleType(getInternal(idx), TypeInfoFactory.TIMESTAMP);
  }

  public void setTimestamp(String columnName, Timestamp value) {
    setTimestamp(getColumnIndex(columnName), value);
  }

  public Timestamp getTimestamp(String columnName) {
    return getTimestamp(getColumnIndex(columnName));
  }

  /**
   * Set timestamp_ntz data, using a {@link java.time.LocalDateTime} value.
   *
   * @param idx Column index
   * @param localDateTime A {@link LocalDateTime} object. Could be null.
   */
  public void setTimestampNtz(int idx, LocalDateTime localDateTime) {
    set(idx, localDateTime);
  }

  /**
   * Set timestamp_ntz data, using a {@link java.time.LocalDateTime} value.
   *
   * @param columnName Column name
   * @param localDateTime A {@link LocalDateTime} object. Could be null.
   */
  public void setTimestampNtz(String columnName, LocalDateTime localDateTime) {
    set(getColumnIndex(columnName), localDateTime);
  }

  /**
   * Get the value of the designated column as a {@link LocalDateTime} object.
   *
   * @param idx Column index.
   * @return A {@link LocalDateTime} object. If the value is SQL <code>NULL</code>, the value returned
   *         is <code>null</code>.
   */
  public LocalDateTime getTimestampNtz(int idx) {
    return getInternal(idx);
  }

  /**
   * Get the value of the designated column as a {@link LocalDateTime} object.
   *
   * @param columnName Column name.
   * @return A {@link LocalDateTime} object. If the value is SQL <code>NULL</code>, the value returned
   *         is <code>null</code>.
   */
  public LocalDateTime getTimestampNtz(String columnName) {
    return getInternal(getColumnIndex(columnName));
  }

  public void setFloat(int idx, Float value) {
    set(idx, value);
  }

  public void setFloat(String columnName, Float value) {
    setFloat(getColumnIndex(columnName), value);
  }

  public Float getFloat(int idx) {
    return getInternal(idx);
  }

  public Float getFloat(String columnName) {
    return getFloat(getColumnIndex(columnName));
  }

  public void setInt(int idx, Integer value) {
    set(idx, value);
  }

  public void setInt(String columnName, Integer value) {
    setInt(getColumnIndex(columnName), value);
  }

  public Integer getInt(int idx) {
    return getInternal(idx);
  }

  public Integer getInt(String columnName) {
    return getInt(getColumnIndex(columnName));
  }

  public void setTinyint(int idx, Byte value) {
    set(idx, value);
  }

  public void setTinyint(String columnName, Byte value) {
    setTinyint(getColumnIndex(columnName), value);
  }

  public Byte getTinyint(int idx) {
    return getInternal(idx);
  }

  public Byte getTinyint(String columnName) {
    return getTinyint(getColumnIndex(columnName));
  }

  public void setSmallint(int idx, Short value) {
    set(idx, value);
  }

  public void setSmallint(String columnName, Short value) {
    setSmallint(getColumnIndex(columnName), value);
  }

  public Short getSmallint(int idx) {
    return getInternal(idx);
  }

  public Short getSmallint(String columnName) {
    return getSmallint(getColumnIndex(columnName));
  }

  public void setStruct(int idx, Struct value) {
    set(idx, value);
  }

  public void setStruct(String columnName, Struct value) {
    setStruct(getColumnIndex(columnName), value);
  }

  public Struct getStruct(int idx) {
    return OdpsTypeTransformer.getCompatibleType(getInternal(idx), columns[idx].getTypeInfo());
  }

  public Struct getStruct(String columnName) {
    return getStruct(getColumnIndex(columnName));
  }

  public void setJsonValue(int idx, JsonValue value) {
    set(idx, value);
  }

  public void setJsonValue(String columnName, JsonValue value) {
    setJsonValue(getColumnIndex(columnName), value);
  }

  public JsonValue getJsonValue(int idx) {
    return OdpsTypeTransformer.getCompatibleType(getInternal(idx), columns[idx].getTypeInfo());
  }

  public JsonValue getJsonValue(String columnName) {
    return getJsonValue(getColumnIndex(columnName));
  }

  public IntervalYearMonth getIntervalYearMonth(int idx) {
    return getInternal(idx);
  }

  public IntervalYearMonth getIntervalYearMonth(String columnName) {
    return getIntervalYearMonth(getColumnIndex(columnName));
  }

  public void setIntervalYearMonth(int idx, IntervalYearMonth value) {
    set(idx, value);
  }

  public void setIntervalYearMonth(String columnName, IntervalYearMonth value) {
    setIntervalYearMonth(getColumnIndex(columnName), value);
  }

  public IntervalDayTime getIntervalDayTime(int idx) {
    return getInternal(idx);
  }

  public IntervalDayTime getIntervalDayTime(String columnName) {
    return getIntervalDayTime(getColumnIndex(columnName));
  }

  public void setIntervalDayTime(int idx, IntervalDayTime value) {
    set(idx, value);
  }

  public void setIntervalDayTime(String columnName, IntervalDayTime value) {
    setIntervalDayTime(getColumnIndex(columnName), value);
  }

  @Override
  public boolean isNull(int idx) {
    return values[idx] == null;
  }

  @Override
  public boolean isNull(String columnName) {
    return isNull(getColumnIndex(columnName));
  }

  @Override
  public Record clone() {
    ArrayRecord record = new ArrayRecord(getColumns());
    record.set(values);
    return record;
  }

  @SuppressWarnings({"unchecked"})
  protected <T> T getInternal(int idx) {
    if (values[idx] == null) {
      return null;
    }

    return (T) values[idx];
  }

  @SuppressWarnings({"unchecked"})
  /**
   * set record column value directly without type validation
   * unsafe: caller must ensure correct type and do validation check externally when necessary
   */
  public void setWithoutValidation(int idx, Object o){
    this.values[idx] = o;
  }

  /**
   * 使用默认 charset，将 byte array 转成 string
   * @param bytes
   *     byte array
   * @return 字符串
   */
  protected static String bytesToString(byte[] bytes) {
    try {
      return new String(bytes, DEFAULT_CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  /**
   * 将 string 转成 byte array，使用默认 charset
   * @param string
   *     字符串
   * @return byte array
   */
  protected static byte[] stringToBytes(String string) {
    try {
      return string.getBytes(DEFAULT_CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

}

