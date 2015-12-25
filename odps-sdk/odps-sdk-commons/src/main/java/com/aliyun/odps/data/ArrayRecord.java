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
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;

/**
 * 基于数组的{@link Record}实现
 *
 * @see Record
 */
public class ArrayRecord implements Record {

  private static final int STRING_MAX_LENTH = 8 * 1024 * 1024;
  private static final int DECIMAL_MAX_INTLENGTH = 36;
  private static final int DECIMAL_MAX_SCALE = 18;

  // 9999-12-31 23:59:59
  private static final long DATETIME_MAX_TICKS = 253402271999000L;
  // 0001-01-01 00:00:00
  private static final long DATETIME_MIN_TICKS = -62135798400000L;

  private Column[] columns;
  private final Object[] values;

  private static final String STRING_CHARSET = "UTF-8";

  private HashMap<String, Integer> nameMap = new HashMap<String, Integer>();

  public ArrayRecord(Column[] columns) {

    if (columns == null) {
      throw new IllegalArgumentException();
    }

    this.columns = columns;

    values = new Object[columns.length];

    for (int i = 0; i < columns.length; i++) {
      nameMap.put(columns[i].getName(), i);
    }

  }

  public ArrayRecord(TableSchema schema) {
    this(schema.getColumns().toArray(new Column[0]));
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
    switch (columns[idx].getType()) {
      case STRING:
        if (value instanceof String) {
          setString(idx, (String) value);
        } else {
          setString(idx, (byte[]) value);
        }
        break;
      case BIGINT:
        setBigint(idx, (Long) value);
        break;
      case BOOLEAN:
        setBoolean(idx, (Boolean) value);
        break;
      case DOUBLE:
        setDouble(idx, (Double) value);
        break;
      case DATETIME:
        setDatetime(idx, (Date) value);
        break;
      case DECIMAL:
        setDecimal(idx, (BigDecimal) value);
        break;
      case ARRAY:
        setArray(idx, (List) value);
        break;
      case MAP:
        setMap(idx, (Map) value);
    }
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
    if (value != null && (value > Long.MAX_VALUE || value <= Long.MIN_VALUE)) {
      throw new IllegalArgumentException("InvalidData: Bigint out of range.");
    }
    values[idx] = value;
  }

  @Override
  public Long getBigint(int idx) {
    return (Long) get(idx);
  }

  @Override
  public void setBigint(String columnName, Long value) {
    setBigint(getColumnIndex(columnName), value);
  }

  @Override
  public Long getBigint(String columnName) {
    return (Long) get(columnName);
  }

  @Override
  public void setDouble(int idx, Double value) {
    values[idx] = value;
  }

  @Override
  public Double getDouble(int idx) {
    return (Double) get(idx);
  }

  @Override
  public void setDouble(String columnName, Double value) {
    setDouble(getColumnIndex(columnName), value);
  }

  @Override
  public Double getDouble(String columnName) {
    return (Double) get(columnName);
  }

  @Override
  public void setBoolean(int idx, Boolean value) {
    values[idx] = value;
  }

  @Override
  public Boolean getBoolean(int idx) {
    return (Boolean) get(idx);
  }

  @Override
  public void setBoolean(String columnName, Boolean value) {
    setBoolean(getColumnIndex(columnName), value);
  }

  @Override
  public Boolean getBoolean(String columnName) {
    return (Boolean) get(columnName);
  }

  @Override
  public void setDatetime(int idx, Date value) {
    if (value != null
        && (value.getTime() > DATETIME_MAX_TICKS || value.getTime() < DATETIME_MIN_TICKS)) {
      throw new IllegalArgumentException("InvalidData: Datetime out of range.");
    }
    values[idx] = value;
  }

  @Override
  public Date getDatetime(int idx) {
    return (Date) get(idx);
  }

  @Override
  public void setDatetime(String columnName, Date value) {
    setDatetime(getColumnIndex(columnName), value);
  }

  @Override
  public Date getDatetime(String columnName) {
    return (Date) get(columnName);
  }

  @Override
  public void setDecimal(int idx, BigDecimal value) {
    if (value != null) {
      BigDecimal tmpValue = value.setScale(DECIMAL_MAX_SCALE, RoundingMode.HALF_UP);
      int intLength = tmpValue.precision() - tmpValue.scale();
      if (intLength > DECIMAL_MAX_INTLENGTH) {
        throw new IllegalArgumentException(
            String.format("decimal value %s overflow, max integer digit number is %s.",
                          value, DECIMAL_MAX_INTLENGTH));
      }
    }
    values[idx] = value;

  }

  @Override
  public BigDecimal getDecimal(int idx) {
    return (BigDecimal) get(idx);
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
    if (value == null) {
      values[idx] = value;
      return;
    }
    try {
      if (value.length() * 6 > STRING_MAX_LENTH
          && value.getBytes(STRING_CHARSET).length > STRING_MAX_LENTH) {
        throw new IllegalArgumentException("InvalidData: The string's length is more than "
                                           + STRING_MAX_LENTH / 1024 / 1024 + "M.");
      }
      values[idx] = value;
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  @Override
  public String getString(int idx) {
    Object o = get(idx);
    if (o == null) {
      return null;
    }
    if (!(o instanceof String)) {
      return bytesToString((byte[]) o);
    } else {
      return (String) get(idx);
    }
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
    if (value == null) {
      values[idx] = value;
      return;
    }
    if (value.length > STRING_MAX_LENTH) {
      throw new IllegalArgumentException("InvalidData: The string's length is more than "
                                         + STRING_MAX_LENTH / 1024 / 1024 + "M.");
    }
    values[idx] = value;
  }

  @Override
  public void setString(String columnName, byte[] value) {
    setString(getColumnIndex(columnName), value);
  }

  @Override
  public byte[] getBytes(int idx) {
    Object o = get(idx);
    if (o == null) {
      return null;
    }
    if (o instanceof String) {
      return stringToBytes((String) o);
    } else {
      return (byte[]) get(idx);
    }
  }

  @Override
  public byte[] getBytes(String columnName) {
    return getBytes(getColumnIndex(columnName));
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

  private byte[] stringToBytes(String string) {
    try {
      return String.valueOf(string).getBytes(STRING_CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  private String bytesToString(byte[] bytes) {
    try {
      return new String(bytes, STRING_CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }

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
    if (list == null) {
      values[idx] = list;
      return;
    }

    List<Object> newList = new ArrayList<Object>(list.size());

    for (Object obj : list) {
      if (obj instanceof String) {
        newList.add(stringToBytes((String) obj));
      } else {
        newList.add(obj);
      }
    }

    values[idx] = newList;
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
    if (map == null) {
      values[idx] = map;
      return;
    }

    Map newMap = new HashMap(map.size(), 1.0f);
    Iterator iter = map.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();

      Object key = entry.getKey();
      Object value = entry.getValue();

      if (key instanceof String) {
        key = stringToBytes((String) key);
      }

      if (value instanceof String) {
        value = stringToBytes((String) value);
      }

      newMap.put(key, value);
    }

    values[idx] = newMap;
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
    if (list == null) {
      return null;
    }

    if (className == String.class) {
      List newList = new ArrayList<String>(list.size());
      for (Object obj : list) {
        if (obj instanceof byte[]) {
          newList.add(bytesToString((byte[]) obj));
        } else {
          newList.add((String) obj);
        }
      }
      return newList;
    } else {
      return (List<T>) list;
    }
  }

  public List getArray(String columnName) {
    return getArray(getColumnIndex(columnName));
  }

  public List getArray(int idx) {
    Object obj = get(idx);
    if (obj == null) {
      return null;
    }

    if (obj instanceof List) {
      return (List) obj;
    } else {
      throw new IllegalArgumentException("Column " + idx + " is not an array column");
    }
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
    if (map == null) {
      return null;
    }

    if (keyClass != String.class && valueClass != String.class) {
      return (Map<k, v>) map;
    }

    Map<k, v> newMap = new HashMap<k, v>(map.size(), 1.0f);

    Iterator iter = map.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      Object key = entry.getKey();
      Object value = entry.getValue();

      if (keyClass == String.class && key != null) {
        key = bytesToString((byte[]) key);
      }

      if (valueClass == String.class && value != null) {
        value = bytesToString((byte[]) value);
      }

      newMap.put((k) key, (v) value);
    }

    return newMap;
  }

  public Map getMap(String columnName) {
    return getMap(getColumnIndex(columnName));
  }

  public Map getMap(int idx) {
    Object obj = get(idx);
    if (obj == null) {
      return null;
    }

    if (obj instanceof Map) {
      return (Map) obj;
    } else {
      throw new IllegalArgumentException("Column " + idx + " is not a map column");
    }
  }
}

