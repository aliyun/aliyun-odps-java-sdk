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
import java.util.Date;
import java.util.HashMap;

import com.aliyun.odps.Column;

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
      int intLength =  tmpValue.precision() - tmpValue.scale();
      if (intLength > DECIMAL_MAX_INTLENGTH) {
        throw new IllegalArgumentException(String.format("decimal value %s overflow, max integer digit number is %s.",
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
      try {
        return new String((byte[]) o, STRING_CHARSET);
      } catch (UnsupportedEncodingException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
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
      try {
        return ((String) o).getBytes(STRING_CHARSET);
      } catch (UnsupportedEncodingException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
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
}
