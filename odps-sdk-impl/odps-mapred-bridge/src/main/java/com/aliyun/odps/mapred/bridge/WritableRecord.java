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

package com.aliyun.odps.mapred.bridge;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import com.aliyun.odps.Column;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.BigDecimalWritable;
import com.aliyun.odps.io.BooleanWritable;
import com.aliyun.odps.io.DatetimeWritable;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableUtils;

/**
 * 基于数组的{@link Record}实现
 *
 * @see Record
 */
public class WritableRecord implements Record {

  private Column[] columns;
  private final Writable[] values;
  // for columns access statistics
  private boolean[] accessMarks;
  private boolean enableColumnAccessStat;

  private static final String STRING_CHARSET = "UTF-8";

  private HashMap<String, Integer> nameMap = new HashMap<String, Integer>();

  public WritableRecord(Column[] columns) {

    if (columns == null) {
      throw new IllegalArgumentException();
    }

    this.columns = columns;

    values = new Writable[columns.length];
    accessMarks = new boolean[columns.length];

    for (int i = 0; i < columns.length; i++) {
      String colName = (columns[i].getName() == null) ? null : columns[i].getName().toLowerCase();
      nameMap.put(colName, i);
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
    if (value == null || value.equals(NullWritable.get())) {
      values[idx] = null;
      return;
    }
    if (value instanceof Writable) {
      values[idx] = (Writable) value;
      return;
    }

    switch (columns[idx].getType()) {
      case STRING:
        if (value instanceof String) {
          setString(idx, (String) value);
        } else {
          setString(idx, (byte[]) value);
        }
        break;
      case BIGINT:
        if (value instanceof Long) {
          setBigint(idx, (Long) value);
        } else if (value instanceof Integer) {
          setBigint(idx, new Long((Integer) value));
        } else {
          throw new RuntimeException("Unsupported value type " + value.getClass().getName() +
              " for column type " + columns[idx].getType());
        }
        break;
      case BOOLEAN:
        setBoolean(idx, (Boolean) value);
        break;
      case DOUBLE:
        if (value instanceof Double) {
          setDouble(idx, (Double) value);
        } else if (value instanceof Float) {
          setDouble(idx, new Double((Float) value));
        } else {
          throw new RuntimeException("Unsupported value type " + value.getClass().getName() +
              " for column type " + columns[idx].getType());
        }
        break;
      case DATETIME:
        setDatetime(idx, (Date) value);
        break;
      case DECIMAL:
        if (value instanceof Long) {
          setDecimal(idx, new BigDecimal( (Long) value));
        } else if (value instanceof Integer) {
          setDecimal(idx, new BigDecimal( (Integer) value));
        } else if (value instanceof Double) {
          setDecimal(idx, new BigDecimal( (Double) value));
        } else if (value instanceof Float) {
          setDecimal(idx, new BigDecimal( (Float) value));
        } else if (value instanceof BigDecimal) {
          setDecimal(idx, (BigDecimal) value);
        } else {
          setDecimal(idx, new BigDecimal( (String) value));
        }
        break;
      case MAP:
      case ARRAY:
        values[idx] = (Writable)value;
      default:
        throw new RuntimeException("Unsupported type " + columns[idx].getType());
    }
  }


  @Override
  public Object get(int idx) {
    switch (columns[idx].getType()) {
      case STRING:
        return getString(idx);
      case BIGINT:
        return getBigint(idx);
      case BOOLEAN:
        return getBoolean(idx);
      case DOUBLE:
        return getDouble(idx);
      case DATETIME:
        return getDatetime(idx);
      case DECIMAL:
        return getDecimal(idx);
      case MAP:
      case ARRAY:
        if (enableColumnAccessStat) {
          columnAccessMark(idx);
        }
        return values[idx];
      default:
        throw new RuntimeException("Unsupported type " + columns[idx].getType());
    }
  }

  @Override
  public void set(String columnName, Object value) {
    set(getColumnIndex(columnName), value);
  }

  @Override
  public Object get(String columnName) {
    return get(getColumnIndex(columnName));
  }

  @Override
  public void setBigint(int idx, Long value) {
    if (value != null && value == Long.MIN_VALUE) {
      throw new IllegalArgumentException("InvalidData: Bigint out of range.");
    }
    values[idx] = value == null ? null : new LongWritable(value);
  }

  @Override
  public Long getBigint(int idx) {
    if (enableColumnAccessStat) {
      columnAccessMark(idx);
    }
    if (values[idx] == null) {
      return null;
    }
    return ((LongWritable) values[idx]).get();
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
    values[idx] = value == null ? null : new DoubleWritable(value);
  }

  @Override
  public Double getDouble(int idx) {
    if (enableColumnAccessStat) {
      columnAccessMark(idx);
    }
    if (values[idx] == null) {
      return null;
    }
    return ((DoubleWritable) values[idx]).get();
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
    values[idx] = value == null ? null : new BooleanWritable(value);
  }

  @Override
  public Boolean getBoolean(int idx) {
    if (enableColumnAccessStat) {
      columnAccessMark(idx);
    }
    if (values[idx] == null) {
      return null;
    }
    return ((BooleanWritable) values[idx]).get();
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
    values[idx] = value == null ? null : new DatetimeWritable(value.getTime());
  }

  @Override
  public Date getDatetime(int idx) {
    if (enableColumnAccessStat) {
      columnAccessMark(idx);
    }
    if (values[idx] == null) {
      return null;
    }
    return new Date(((DatetimeWritable) values[idx]).get());
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
  public void setString(int idx, String value) {
    try {
      setString(idx, value == null ? null : value.getBytes(STRING_CHARSET));
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  @Override
  public String getString(int idx) {
    if (enableColumnAccessStat) {
      columnAccessMark(idx);
    }
    if (values[idx] == null) {
      return null;
    }
    return ((Text) values[idx]).toString();
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
    values[idx] = value == null ? null : new Text(value);
  }

  @Override
  public void setString(String columnName, byte[] value) {
    setString(getColumnIndex(columnName), value);
  }

  @Override
  public byte[] getBytes(int idx) {
    if (enableColumnAccessStat) {
      columnAccessMark(idx);
    }
    Text o = (Text) values[idx];
    if (o == null) {
      return null;
    }
    return Arrays.copyOfRange(o.getBytes(), 0, o.getLength());
  }

  @Override
  public byte[] getBytes(String columnName) {
    return getBytes(getColumnIndex(columnName));
  }

  @Override
  public void set(Object[] values) {
    if (values == null || columns.length != values.length) {
      throw new IllegalArgumentException("InvalidData: the parameter is null or length not match.");
    }
    for (int i = 0; i < values.length; ++i) {
      set(i, values[i]);
    }
  }

  @Override
  public Object[] toArray() {
    Object[] rt = new Object[values.length];
    for (int i = 0; i < values.length; ++i) {
      rt[i] = get(i);
    }
    return rt;
  }

  public Writable[] toWritableArray() {
    if (enableColumnAccessStat) {
      for (int idx = 0; idx < this.getColumnCount(); ++idx) {
        columnAccessMark(idx);
      }
    }
    return values;
  }

  private int getColumnIndex(String name) {
    Integer idx = nameMap.get(name.toLowerCase());
    if (idx == null) {
      throw new IllegalArgumentException("No such column:" + name);
    }
    return idx;
  }

  void setEnableColumnAccessStat(boolean isEnable) {
    enableColumnAccessStat = isEnable;
  }

  void columnAccessMark(int idx) {
    accessMarks[idx] = true;
  }

  int getColumnAccessedNum() {
    int cnt = 0;
    for (int i = 0; i < accessMarks.length; ++i) {
      if (accessMarks[i]) {
        cnt++;
      }
    }
    return cnt;
  }

  @Override
  public void setDecimal(int idx, BigDecimal value) {
    values[idx] = value == null ? null : new BigDecimalWritable(value);
  }

  @Override
  public BigDecimal getDecimal(int idx) {
    if (enableColumnAccessStat) {
      columnAccessMark(idx);
    }
    if (values[idx] == null) {
      return null;
    }
    return ((BigDecimalWritable) values[idx]).get();
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
  public boolean isNull(int idx) {
    if (enableColumnAccessStat) {
      columnAccessMark(idx);
    }
    return values[idx] == null;
  }

  @Override
  public boolean isNull(String columnName) {
    return isNull(getColumnIndex(columnName));
  }

  @Override
  public Record clone() {
    WritableRecord record = new WritableRecord(getColumns());
    for (int i = 0; i < values.length; ++i) {
      // if clone, set all accessed
      if (enableColumnAccessStat) {
        columnAccessMark(i);
      }
      record.values[i] = WritableUtils.clone(values[i], null);
    }
    return record;
  }
}
