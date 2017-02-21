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
  private static final String DEFAULT_CHARSET = "utf-8";

  private Column[] columns;
  private final Object[] values;

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
    values[idx] = OdpsTypeTransformer.transform(value, columns[idx].getTypeInfo());
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
    return getInternal(idx);
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
    return getInternal(idx);
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

  public void setDate(int idx, java.sql.Date value) {
    set(idx, value);
  }

  public java.sql.Date getDate(int idx) {
    return getInternal(idx);
  }

  public void setDate(String columnName, java.sql.Date value) {
    setDate(getColumnIndex(columnName), value);
  }

  public java.sql.Date getDate(String columnName) {
    return getDate(getColumnIndex(columnName));
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
    return getInternal(idx);
  }

  public Struct getStruct(String columnName) {
    return getStruct(getColumnIndex(columnName));
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
}

