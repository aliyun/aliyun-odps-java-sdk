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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.chrono.IsoChronology;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;

/**
 * 基于数组的{@link Record}实现
 *
 * @see Record
 */
public class ArrayRecord implements Record {
  private static final Long DEFAULT_FIELD_MAX_SIZE = 8 * 1024 * 1024L;
  private static final String DEFAULT_CHARSET = "utf-8";

  static final Calendar DEFAULT_CALENDAR = new Calendar.Builder()
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
    // allow byte [] to set on STRING column, ugly
    if (columns[idx].getTypeInfo().getOdpsType() == OdpsType.STRING && (value instanceof byte[])) {
      values[idx] = value;
      return;
    }

    values[idx] = OdpsTypeTransformer.transform(
        value,
        columns[idx].getTypeInfo(),
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

  @Override
  public void setDatetime(int idx, Date value) {
    set(idx, value);
  }

  @Override
  public Date getDatetime(int idx) {
    return getInternal(idx);
  }

  @Override
  public void setDatetime(String columnName, Date value) {
    setDatetime(getColumnIndex(columnName), value);
  }

  @Override
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
    List list = getArray(idx);

    if (list == null || list.isEmpty()) {
      return list;
    }

    List<T> newList = new ArrayList<T>(list.size());
    for (Object obj : list) {
      if ((obj instanceof String) && (className == byte[].class)) {
        newList.add((T) stringToBytes((String) obj));
      } else {
        newList.add(className.cast(obj));
      }
    }
    return newList;
  }

  public List getArray(String columnName) {
    return getArray(getColumnIndex(columnName));
  }

  public List getArray(int idx) {
    return toCompatibleArray((ArrayTypeInfo) columns[idx].getTypeInfo(), getInternal(idx));
  }

  private List toCompatibleArray(ArrayTypeInfo typeInfo, List list) {
    if (list == null) {
      return null;
    }

    // ArrayRecord used java.sql.Date as inner object type of DATE. But java.sql.Date is error-prone
    // so it was replaced by LocalDate. However, the inner object type is actually exposed to users
    // by this method. So, we do a conversion to keep the compatibility.
    TypeInfo elementTypeInfo = typeInfo.getElementTypeInfo();
    List<Object> ret = new ArrayList<>(list.size());
    for (Object o : list) {
      if (o == null) {
        ret.add(null);
        continue;
      }
      switch (elementTypeInfo.getOdpsType()) {
        case DATE:
          ret.add(new java.sql.Date(localDateToDate((LocalDate) o, DEFAULT_CALENDAR).getTime()));
          break;
        case ARRAY:
          ret.add(toCompatibleArray((ArrayTypeInfo) elementTypeInfo, (List) o));
          break;
        case MAP:
          ret.add(toCompatibleMap((MapTypeInfo) elementTypeInfo, (Map) o));
          break;
        case STRUCT:
          ret.add(toCompatibleStruct((StructTypeInfo) elementTypeInfo, (Struct) o));
          break;
        default:
          ret.add(o);
      }
    }

    return ret;
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
    Map map = getMap(idx);

    if (map == null || map.isEmpty()) {
      return map;
    }

    Map<k, v> newMap = new HashMap<k, v>(map.size(), 1.0f);
    Iterator iter = map.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      Object key = entry.getKey();
      Object value = entry.getValue();

      if (keyClass == byte [].class && key != null && key instanceof String) {
        key = stringToBytes((String) key);
      }

      if (valueClass == byte [].class && value != null && value instanceof String) {
        value = stringToBytes((String) value);
      }

      newMap.put(keyClass.cast(key), valueClass.cast(value));
    }

    return newMap;
  }

  public Map getMap(String columnName) {
    return getMap(getColumnIndex(columnName));
  }

  public Map getMap(int idx) {
    return toCompatibleMap((MapTypeInfo) columns[idx].getTypeInfo(), getInternal(idx));
  }

  private Map toCompatibleMap(MapTypeInfo typeInfo, Map map) {
    if (map == null) {
      return null;
    }

    // ArrayRecord used java.sql.Date as inner object type of DATE. But java.sql.Date is error-prone
    // so it was replaced by LocalDate. However, the inner object type is actually exposed to users
    // by this method. So, we do a conversion to keep the compatibility.
    Map<Object, Object> ret = new HashMap<>(map.size());
    for (Object key : map.keySet()) {
      Object value = map.get(key);
      if (value != null) {
        TypeInfo valueTypeInfo = typeInfo.getValueTypeInfo();
        switch (typeInfo.getValueTypeInfo().getOdpsType()) {
          case DATE:
            value =
                new java.sql.Date(localDateToDate((LocalDate) value, DEFAULT_CALENDAR).getTime());
            break;
          case ARRAY:
            value = toCompatibleArray((ArrayTypeInfo) valueTypeInfo, (List) value);
            break;
          case MAP:
            value = toCompatibleMap((MapTypeInfo) valueTypeInfo, (Map) value);
            break;
          case STRUCT:
            value = toCompatibleStruct((StructTypeInfo) valueTypeInfo, (Struct) value);
          default:
        }
      }

      if (key != null && OdpsType.DATE.equals(typeInfo.getKeyTypeInfo().getOdpsType())) {
        key = new java.sql.Date(localDateToDate((LocalDate) key, DEFAULT_CALENDAR).getTime());
      }

      ret.put(key, value);
    }

    return ret;
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
  public void setDate(int idx, java.sql.Date value,  Calendar calendar) {
    if (value != null) {
      set(idx, dateToLocalDate(value, Optional.ofNullable(calendar).orElse(DEFAULT_CALENDAR)));
    } else {
      set(idx, null);
    }
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
  public java.sql.Date getDate(int idx, Calendar calendar) {
    java.util.Date date = localDateToDate(
        getInternal(idx), Optional.ofNullable(calendar).orElse(DEFAULT_CALENDAR));

    if (date == null) {
      return null;
    }

    return new java.sql.Date(date.getTime());
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
  // TODO: make this method public
  private void setDateAsLocalDate(int columnIdx, LocalDate localDate) {
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
  // TODO: make this method public
  private void setDateAsLocalDate(String columnName, LocalDate localDate) {
    setDateAsLocalDate(getColumnIndex(columnName), localDate);
  }

  /**
   * Get the value of the designated column as a {@link LocalDate} object.
   *
   * @param columnIdx Column index.
   * @return A {@link LocalDate} object. If the value is SQL <code>NULL</code>, the value returned
   *         is <code>null</code>.
   */
  // TODO: make this method public
  private LocalDate getDateAsLocalDate(int columnIdx) {
    return getInternal(columnIdx);
  }

  /**
   * Get the value of the designated column as a {@link LocalDate} object.
   *
   * @param columnName Column name.
   * @return A {@link LocalDate} object. If the value is SQL <code>NULL</code>, the value returned
   *         is <code>null</code>.
   */
  // TODO: make this method public
  private LocalDate getDateAsLocalDate(String columnName) {
    return getDateAsLocalDate(getColumnIndex(columnName));
  }

  public void setTimestamp(int idx, Timestamp value) {
    set(idx, value);
  }

  public Timestamp getTimestamp(int idx) {
    return getInternal(idx);
  }

  public void setTimestamp(String columnName, Timestamp value) {
    setTimestamp(getColumnIndex(columnName), value);
  }

  public Timestamp getTimestamp(String columnName) {
    return getTimestamp(getColumnIndex(columnName));
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
    return toCompatibleStruct((StructTypeInfo) columns[idx].getTypeInfo(), getInternal(idx));
  }

  private Struct toCompatibleStruct(StructTypeInfo typeInfo, Struct struct) {
    if (struct == null) {
      return null;
    }

    // ArrayRecord used java.sql.Date as inner object type of DATE. But java.sql.Date is error-prone
    // so it was replaced by LocalDate. However, the inner object type is actually exposed to users
    // by this method. So, we do a conversion to keep the compatibility.
    List<Object> values = new ArrayList<>(struct.getFieldCount());
    List<TypeInfo> fieldTypeInfos = typeInfo.getFieldTypeInfos();
    for (int i = 0; i < typeInfo.getFieldCount(); ++i) {
      TypeInfo fieldTypeInfo = fieldTypeInfos.get(i);
      Object o = struct.getFieldValue(i);

      if (o == null) {
        values.add(null);
        continue;
      }

      switch (fieldTypeInfo.getOdpsType()) {
        case DATE:
          o = new java.sql.Date(localDateToDate((LocalDate) o, DEFAULT_CALENDAR).getTime());
          break;
        case ARRAY:
          o = toCompatibleArray((ArrayTypeInfo) fieldTypeInfo, (List) o);
          break;
        case MAP:
          o = toCompatibleMap((MapTypeInfo) fieldTypeInfo, (Map) o);
          break;
        case STRUCT:
          o = toCompatibleStruct((StructTypeInfo) fieldTypeInfo, (Struct) o);
          break;
        default:
      }
      values.add(o);
    }
    return new SimpleStruct(typeInfo, values);
  }


  public Struct getStruct(String columnName) {
    return getStruct(getColumnIndex(columnName));
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
  private <T> T getInternal(int idx) {
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
  static String bytesToString(byte[] bytes) {
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
  static byte[] stringToBytes(String string) {
    try {
      return string.getBytes(DEFAULT_CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  /**
   * Convert a {@link LocalDate} object to a {@link java.util.Date} object using GMT time.
   *
   * Since some users use the {@link java.util.Date} to represent the {@link OdpsType#DATE},
   * we use {@link java.util.Date} here to keep compatibility.
   *
   * @param localDate A {@link LocalDate} object. Could be null.
   * @param calendar A {@link Calendar} object. Cannot be null.
   * @return A {@link java.util.Date} object, or null if the input is null.
   */
  static java.util.Date localDateToDate(LocalDate localDate, Calendar calendar) {
    if (localDate != null) {
      calendar = (Calendar) calendar.clone();
      calendar.clear();
      calendar.set(
          localDate.getYear(),
          // Starts from 0
          localDate.getMonth().getValue() - 1,
          localDate.getDayOfMonth());
      return calendar.getTime();
    }

    return null;
  }

  /**
   * Convert a {@link java.util.Date} object to a {@link LocalDate} object. The
   * {@link java.util.Date} object must be constructed using the Greenwich Mean Time.
   *
   * Since some users use the {@link java.util.Date} to represent the {@link OdpsType#DATE},
   * we use {@link java.util.Date} here to keep compatibility.
   *
   * @param date A {@link java.util.Date} object. Could be null.
   * @param calendar A {@link Calendar} object. Cannot be null.
   * @return A {@link LocalDate} object, or null if the input is null.
   */
  static LocalDate dateToLocalDate(java.util.Date date, Calendar calendar) {
    if (date != null) {
      calendar = (Calendar) calendar.clone();
      calendar.clear();
      calendar.setLenient(true);
      calendar.setTime(date);
      return IsoChronology.INSTANCE.date(
          IsoChronology.INSTANCE.eraOf(calendar.get(Calendar.ERA)),
          calendar.get(Calendar.YEAR),
          // Starts from 1
          calendar.get(Calendar.MONTH) + 1,
          calendar.get(Calendar.DAY_OF_MONTH));
    }

    return null;
  }
}

