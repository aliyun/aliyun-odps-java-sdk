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
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import com.aliyun.odps.Column;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.data.IntervalYearMonth;
import com.aliyun.odps.data.IntervalDayTime;
import com.aliyun.odps.io.BigDecimalWritable;
import com.aliyun.odps.io.BooleanWritable;
import com.aliyun.odps.io.DatetimeWritable;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableUtils;
import com.aliyun.odps.io.CharWritable;
import com.aliyun.odps.io.VarcharWritable;
import com.aliyun.odps.io.StructWritable;
import com.aliyun.odps.io.FloatWritable;
import com.aliyun.odps.io.DateWritable;
import com.aliyun.odps.io.TimestampWritable;
import com.aliyun.odps.io.IntWritable;
import com.aliyun.odps.io.BytesWritable;
import com.aliyun.odps.io.IntervalDayTimeWritable;
import com.aliyun.odps.io.IntervalYearMonthWritable;
import com.aliyun.odps.io.ArrayWritable;
import com.aliyun.odps.io.MapWritable;
import com.aliyun.odps.io.ByteWritable;
import com.aliyun.odps.io.ShortWritable;

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
      case TINYINT:
        setTinyint(idx, (Byte)value);
        break;
      case SMALLINT:
        setSmallint(idx, (Short)value);
        break;
      case INT:
        setInt(idx, (Integer)value);
        break;
      case FLOAT:
        setFloat(idx, (Float)value);
        break;
      case CHAR:
        setChar(idx, (Char) value);
        break;
      case VARCHAR:
        setVarchar(idx, (Varchar) value);
        break;
      case DATE:
        setDate(idx, (java.sql.Date) value);
        break;
      case TIMESTAMP:
        setTimestamp(idx, (java.sql.Timestamp) value);
        break;
      case BINARY:
        setBinary(idx, (Binary) value);
        break;
      case INTERVAL_DAY_TIME:
        setIntervalDayTime(idx, (IntervalDayTime) value);
        break;
      case INTERVAL_YEAR_MONTH:
        setIntervalYearMonth(idx, (IntervalYearMonth) value);
        break;
      case STRUCT:
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
      case TINYINT:
        return getTinyint(idx);
      case SMALLINT:
        return getSmallint(idx);
      case INT:
        return getInt(idx);
      case FLOAT:
        return getFloat(idx);
      case CHAR:
        return getChar(idx);
      case VARCHAR:
        return getVarchar(idx);
      case DATE:
        return getDate(idx);
      case TIMESTAMP:
        return getTimestamp(idx);
      case BINARY:
        return getBinary(idx);
      case INTERVAL_DAY_TIME:
        return getIntervalDayTime(idx);
      case INTERVAL_YEAR_MONTH:
        return getIntervalYearMonth(idx);
      case STRUCT:
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

  @SuppressWarnings({"unchecked"})
  private <T> T getInternal(int idx) {
    if (enableColumnAccessStat) {
      columnAccessMark(idx);
    }
    return values[idx] == null ? null : (T) values[idx];
  }

  public void setChar(int idx, Char value) {
    values[idx] = value == null ? null : new CharWritable(value);
  }

  public void setChar(String columnName, Char value) {
    setChar(getColumnIndex(columnName), value);
  }

  public Char getChar(int idx) {
    CharWritable writable = getInternal(idx);
    return writable == null ? null : writable.get();
  }

  public Char getChar(String columnName) {
    return getChar(getColumnIndex(columnName));
  }

  public void setVarchar(int idx, Varchar value) {
    values[idx] = value == null ? null : new VarcharWritable(value);
  }

  public void setVarchar(String columnName, Varchar value) {
    setVarchar(getColumnIndex(columnName), value);
  }

  public Varchar getVarchar(int idx) {
    VarcharWritable writable = getInternal(idx);
    return writable == null ? null : writable.get();
  }

  public Varchar getVarchar(String columnName) {
    return getVarchar(getColumnIndex(columnName));
  }

  public void setDate(int idx, java.sql.Date value) {
    values[idx] = value == null ? null : new DateWritable(value.getTime());
  }

  public java.sql.Date getDate(int idx) {
    DateWritable writable = getInternal(idx);
    return writable == null ? null : new java.sql.Date(((DateWritable) values[idx]).get());
  }

  public void setDate(String columnName, java.sql.Date value) {
    setDate(getColumnIndex(columnName), value);
  }

  public java.sql.Date getDate(String columnName) {
    return getDate(getColumnIndex(columnName));
  }

  public void setTimestamp(int idx, Timestamp value) {
    values[idx] = value == null ? null : new TimestampWritable(value.getTime());
  }

  public Timestamp getTimestamp(int idx) {
    TimestampWritable writable = getInternal(idx);
    return writable == null ? null : new java.sql.Timestamp(writable.get());
  }

  public void setTimestamp(String columnName, Timestamp value) {
    setTimestamp(getColumnIndex(columnName), value);
  }

  public Timestamp getTimestamp(String columnName) {
    return getTimestamp(getColumnIndex(columnName));
  }

  public void setFloat(int idx, Float value) {
    values[idx] = value == null ? null : new FloatWritable(value);
  }

  public void setFloat(String columnName, Float value) {
    setFloat(getColumnIndex(columnName), value);
  }

  public Float getFloat(int idx) {
    FloatWritable writable = getInternal(idx);
    return writable == null ? null : writable.get();
  }

  public Float getFloat(String columnName) {
    return getFloat(getColumnIndex(columnName));
  }

  public void setInt(int idx, Integer value) {
    values[idx] = value == null ? null : new IntWritable(value);
  }

  public void setInt(String columnName, Integer value) {
    setInt(getColumnIndex(columnName), value);
  }

  public Integer getInt(int idx) {
    IntWritable writable = getInternal(idx);
    return writable == null ? null : writable.get();
  }

  public Integer getInt(String columnName) {
    return getInt(getColumnIndex(columnName));
  }

  public void setTinyint(int idx, Byte value) {
    values[idx] = value == null ? null : new ByteWritable(value);
  }

  public void setTinyint(String columnName, Byte value) {
    setTinyint(getColumnIndex(columnName), value);
  }

  public Byte getTinyint(int idx) {
    ByteWritable writable = getInternal(idx);
    return writable == null ? null : writable.get();
  }

  public Byte getTinyint(String columnName) {
    return getTinyint(getColumnIndex(columnName));
  }

  public void setSmallint(int idx, Short value) {
    values[idx] = value == null ? null : new ShortWritable(value);
  }

  public void setSmallint(String columnName, Short value) {
    setSmallint(getColumnIndex(columnName), value);
  }

  public Short getSmallint(int idx) {
    ShortWritable writable = getInternal(idx);
    return writable == null ? null : writable.get();
  }

  public Short getSmallint(String columnName) {
    return getSmallint(getColumnIndex(columnName));
  }

  public void setBinary(int idx, Binary value) {
    values[idx] = value == null ? null : new BytesWritable(value.data());
  }

  public void setBinary(String columnName, Binary value) {
    setBinary(getColumnIndex(columnName), value);
  }

  public Binary getBinary(int idx) {
    BytesWritable writable = getInternal(idx);
    return writable == null ? null : new Binary(writable.getBytes());
  }

  public Binary getBinary(String columnName) {
    return getBinary(getColumnIndex(columnName));
  }

  public IntervalYearMonth getIntervalYearMonth(int idx) {
    IntervalYearMonthWritable writable = getInternal(idx);
    return writable == null ? null : writable.get();
  }

  public IntervalYearMonth getIntervalYearMonth(String columnName) {
    return getIntervalYearMonth(getColumnIndex(columnName));
  }

  public void setIntervalYearMonth(int idx, IntervalYearMonth value) {
    values[idx] = value == null ? null : new IntervalYearMonthWritable(value);
  }

  public void setIntervalYearMonth(String columnName, IntervalYearMonth value) {
    setIntervalYearMonth(getColumnIndex(columnName), value);
  }

  public IntervalDayTime getIntervalDayTime(int idx) {
    IntervalDayTimeWritable writable = getInternal(idx);
    return writable == null ? null : writable.get();
  }

  public IntervalDayTime getIntervalDayTime(String columnName) {
    return getIntervalDayTime(getColumnIndex(columnName));
  }

  public void setIntervalDayTime(int idx, IntervalDayTime value) {
    values[idx] = value == null ? null : new IntervalDayTimeWritable(value);
  }

  public void setIntervalDayTime(String columnName, IntervalDayTime value) {
    setIntervalDayTime(getColumnIndex(columnName), value);
  }

  public void setArray(String columnName, Writable value) {
    setArray(getColumnIndex(columnName), value);
  }

  public void setArray(int idx, Writable value) {
    set(idx, value);
  }

  public void setMap(int idx, Writable value) {
    set(idx, value);
  }

  public void setMap(String columnName, Writable value) {
    setMap(getColumnIndex(columnName), value);
  }

  public void setStruct(int idx, Writable value) {
    set(idx, value);
  }

  public void setStruct(String columnName, Writable value) {
    setStruct(getColumnIndex(columnName), value);
  }

  public ArrayWritable getArray(String columnName) {
    return getArray(getColumnIndex(columnName));
  }

  public ArrayWritable getArray(int idx) {
    return getInternal(idx);
  }

  public MapWritable getMap(String columnName) {
    return getMap(getColumnIndex(columnName));
  }

  public MapWritable getMap(int idx) {
    return getInternal(idx);
  }

  public StructWritable getStruct(int idx) {
    return getInternal(idx);
  }

  public StructWritable getStruct(String columnName) {
    return getStruct(getColumnIndex(columnName));
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
