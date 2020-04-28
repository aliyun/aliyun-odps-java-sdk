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

package com.aliyun.odps.local.common.utils;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.IntervalDayTime;
import com.aliyun.odps.data.IntervalYearMonth;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.io.ArrayWritable;
import com.aliyun.odps.io.BigDecimalWritable;
import com.aliyun.odps.io.ByteWritable;
import com.aliyun.odps.io.BytesWritable;
import com.aliyun.odps.io.CharWritable;
import com.aliyun.odps.io.DateWritable;
import com.aliyun.odps.io.DatetimeWritable;
import com.aliyun.odps.io.FloatWritable;
import com.aliyun.odps.io.IntervalDayTimeWritable;
import com.aliyun.odps.io.IntervalYearMonthWritable;
import com.aliyun.odps.io.MapWritable;
import com.aliyun.odps.io.ShortWritable;
import com.aliyun.odps.io.StructWritable;
import com.aliyun.odps.io.TimestampWritable;
import com.aliyun.odps.io.VarcharWritable;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.type.VarcharTypeInfo;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

import com.aliyun.odps.io.BooleanWritable;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.IntWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalWritableUtils {

  public static Writable[] convert(Object[] args, List<TypeInfo> types) {
    if (args == null) {
      return null;
    }
    Writable[] result = new Writable[args.length];
    for (int i = 0; i < args.length; i++) {
      TypeInfo typeInfo = types != null ? types.get(i) : null;
      result[i] = convert(args[i], typeInfo);
    }
    return result;
  }

  public static Writable convert(Object arg0, TypeInfo typeInfo) {
    if (arg0 == null) {
      return null;
    }
    if (typeInfo == TypeInfoFactory.STRING || arg0 instanceof String) {
      if (arg0 instanceof String) {
        return new Text((String) arg0);
      } else {
        byte[] bytes = (byte[]) arg0;
        return new Text(bytes);
      }
    }
    if (typeInfo == TypeInfoFactory.TINYINT || arg0 instanceof Byte) {
      return new ByteWritable((Byte) arg0);
    }
    if (typeInfo == TypeInfoFactory.SMALLINT || arg0 instanceof Short) {
      return new ShortWritable((Short) arg0);
    }
    if (typeInfo == TypeInfoFactory.INT || arg0 instanceof Integer) {
      return new IntWritable((Integer) arg0);
    }
    if (typeInfo == TypeInfoFactory.BIGINT || arg0 instanceof Long) {
      return new LongWritable((Long) arg0);
    }
    if (typeInfo == TypeInfoFactory.BOOLEAN || arg0 instanceof Boolean) {
      return new BooleanWritable((Boolean) arg0);
    }
    if (typeInfo == TypeInfoFactory.FLOAT || arg0 instanceof Float) {
      return new FloatWritable((Float) arg0);
    }
    if (typeInfo == TypeInfoFactory.DOUBLE || arg0 instanceof Double) {
      return new DoubleWritable((Double) arg0);
    }
    if (typeInfo == TypeInfoFactory.DECIMAL || arg0 instanceof BigDecimal) {
      return new BigDecimalWritable((BigDecimal) arg0);
    }
    if (typeInfo == TypeInfoFactory.DATETIME || arg0 instanceof java.util.Date) {
      return new DatetimeWritable(((java.util.Date)arg0).getTime());
    }
    if (typeInfo == TypeInfoFactory.TIMESTAMP || arg0 instanceof Timestamp) {
      TimestampWritable temp = new TimestampWritable();
      temp.setTimestamp((Timestamp) arg0);
      return temp;
    }
    if (typeInfo == TypeInfoFactory.DATE || arg0 instanceof java.sql.Date) {
      return new DateWritable(((java.sql.Date) arg0).getTime()/1000);
    }
    if (typeInfo instanceof CharTypeInfo || arg0 instanceof Char) {
      return new CharWritable((Char) arg0);
    }
    if (typeInfo instanceof VarcharTypeInfo || arg0 instanceof Varchar) {
      return new VarcharWritable((Varchar) arg0);
    }
    if (typeInfo == TypeInfoFactory.BINARY || arg0 instanceof Binary) {
      return new BytesWritable(((Binary) arg0).data());
    }
    if (typeInfo == TypeInfoFactory.INTERVAL_DAY_TIME || arg0 instanceof IntervalDayTime) {
      return new IntervalDayTimeWritable((IntervalDayTime) arg0);
    }
    if (typeInfo == TypeInfoFactory.INTERVAL_YEAR_MONTH || arg0 instanceof IntervalYearMonth) {
      return new IntervalYearMonthWritable((IntervalYearMonth) arg0);
    }
    if (typeInfo instanceof ArrayTypeInfo) {
      TypeInfo subType = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();
      List list = (List) arg0;
      Writable[] writables = new Writable[list.size()];
      for (int i = 0; i < writables.length; i++) {
        Object ele = list.get(i);
        writables[i] = convert(ele, subType);
      }
      Class subClazz = getWritableClass(subType);
      return new ArrayWritable(subClazz, writables);
    }
    if (typeInfo instanceof MapTypeInfo) {
      TypeInfo keyType = ((MapTypeInfo) typeInfo).getKeyTypeInfo();
      TypeInfo valueType = ((MapTypeInfo) typeInfo).getValueTypeInfo();
      Map map = (Map) arg0;
      MapWritable result = new MapWritable();
      for (Object entry : map.entrySet()) {
        Map.Entry mapEntry = (Map.Entry) entry;
        Writable key = convert(mapEntry.getKey(), keyType);
        Writable value = null;
        if (mapEntry.getValue() != null) {
          value = convert(mapEntry.getValue(), valueType);
        }
        result.put(key, value);
      }
      return result;
    }
    if (typeInfo instanceof StructTypeInfo) {
      List<TypeInfo> fieldTypes = ((StructTypeInfo)typeInfo).getFieldTypeInfos();
      List<Object> fieldValues = ((SimpleStruct) arg0).getFieldValues();
      assert fieldTypes.size() == fieldValues.size();
      List<Writable> writables = new ArrayList<>();
      for (int i = 0; i < fieldValues.size(); i++) {
        Object val = fieldValues.get(i);
        TypeInfo type = fieldTypes.get(i);
        writables.add(convert(val, type));
      }
      return new StructWritable(writables);
    }

    throw new IllegalArgumentException("unsupported data type:" + arg0.getClass().getName());
  }

  public static Object convert(Writable arg0, TypeInfo typeInfo) {
    if (arg0 == null) {
      return null;
    }
    if (typeInfo == TypeInfoFactory.STRING || arg0 instanceof Text) {
      Text text = (Text) arg0;
      return new String(text.getBytes(), 0, text.getLength(), TypeConvertUtils.UTF8);
    }
    if (typeInfo == TypeInfoFactory.TINYINT || arg0 instanceof ByteWritable) {
      return ((ByteWritable) arg0).get();
    }
    if (typeInfo == TypeInfoFactory.SMALLINT || arg0 instanceof ShortWritable) {
      return ((ShortWritable) arg0).get();
    }
    if (typeInfo == TypeInfoFactory.INT || arg0 instanceof IntWritable) {
      return ((IntWritable) arg0).get();
    }
    if (typeInfo == TypeInfoFactory.BIGINT || arg0 instanceof LongWritable) {
      return ((LongWritable) arg0).get();
    }
    if (typeInfo == TypeInfoFactory.BOOLEAN || arg0 instanceof BooleanWritable) {
      return ((BooleanWritable) arg0).get();
    }
    if (typeInfo == TypeInfoFactory.FLOAT || arg0 instanceof FloatWritable) {
      return ((FloatWritable) arg0).get();
    }
    if (typeInfo == TypeInfoFactory.DOUBLE || arg0 instanceof DoubleWritable) {
      return ((DoubleWritable) arg0).get();
    }
    if (typeInfo == TypeInfoFactory.DECIMAL || arg0 instanceof BigDecimalWritable) {
      return ((BigDecimalWritable) arg0).get();
    }
    if (typeInfo == TypeInfoFactory.DATETIME || arg0 instanceof DatetimeWritable) {
      return ((DatetimeWritable) arg0).getDatetime();
    }
    if (typeInfo == TypeInfoFactory.TIMESTAMP || arg0 instanceof TimestampWritable) {
      return ((TimestampWritable)arg0).getTimestamp();
    }
    if (typeInfo == TypeInfoFactory.DATE || arg0 instanceof DateWritable) {
      return ((DateWritable)arg0).getDate();
    }
    if (typeInfo instanceof CharTypeInfo || arg0 instanceof CharWritable) {
      return ((CharWritable) arg0).get();
    }
    if (typeInfo instanceof VarcharTypeInfo || arg0 instanceof VarcharWritable) {
      return ((VarcharWritable) arg0).get();
    }
    if (typeInfo == TypeInfoFactory.BINARY || arg0 instanceof BytesWritable) {
      BytesWritable tmp = (BytesWritable) arg0;
      byte[] bytes = Arrays.copyOfRange(tmp.getBytes(), 0, tmp.getLength());
      return new Binary(bytes);
    }
    if (typeInfo == TypeInfoFactory.INTERVAL_DAY_TIME || arg0 instanceof IntervalDayTimeWritable) {
      return ((IntervalDayTimeWritable) arg0).get();
    }
    if (typeInfo == TypeInfoFactory.INTERVAL_YEAR_MONTH || arg0 instanceof IntervalYearMonthWritable) {
      return ((IntervalYearMonthWritable) arg0).get();
    }
    if (typeInfo instanceof ArrayTypeInfo) {
      TypeInfo subType = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();
      List<Object> res = new ArrayList<>();
      for (Writable writable : ((ArrayWritable) arg0).get()) {
        res.add(convert(writable, subType));
      }
      return res;
    }
    if (typeInfo instanceof MapTypeInfo) {
      TypeInfo keyType = ((MapTypeInfo) typeInfo).getKeyTypeInfo();
      TypeInfo valueType = ((MapTypeInfo) typeInfo).getValueTypeInfo();
      Map map = new HashMap();
      for (Map.Entry<Writable, Writable> entry : ((MapWritable) arg0).entrySet()) {
        Object key = convert(entry.getKey(), keyType);
        Object value = convert(entry.getValue(), valueType);
        map.put(key, value);
      }
      return map;
    }
    if (typeInfo instanceof StructTypeInfo) {
      List<TypeInfo> fieldTypes = ((StructTypeInfo)typeInfo).getFieldTypeInfos();
      List<Writable> writables = ((StructWritable) arg0).get();
      assert fieldTypes.size() == writables.size();
      List<Object> elements = new ArrayList<>(writables.size());
      for (int i = 0; i < writables.size(); i++) {
        Writable val = writables.get(i);
        TypeInfo type = fieldTypes.get(i);
        elements.add(convert(val, type));
      }
      return new SimpleStruct((StructTypeInfo)typeInfo, elements);
    }

    throw new IllegalArgumentException("unsupported data type:" + arg0.getClass().getName());
  }

  public static Class getWritableClass(TypeInfo typeInfo) {
    if (typeInfo == TypeInfoFactory.STRING) {
      return Text.class;
    }
    if (typeInfo == TypeInfoFactory.TINYINT) {
      return ByteWritable.class;
    }
    if (typeInfo == TypeInfoFactory.SMALLINT) {
      return ShortWritable.class;
    }
    if (typeInfo == TypeInfoFactory.INT) {
      return IntWritable.class;
    }
    if (typeInfo == TypeInfoFactory.BIGINT) {
      return LongWritable.class;
    }
    if (typeInfo == TypeInfoFactory.BOOLEAN) {
      return BooleanWritable.class;
    }
    if (typeInfo == TypeInfoFactory.FLOAT) {
      return FloatWritable.class;
    }
    if (typeInfo == TypeInfoFactory.DOUBLE) {
      return DoubleWritable.class;
    }
    if (typeInfo == TypeInfoFactory.DECIMAL) {
      return BigDecimalWritable.class;
    }
    if (typeInfo == TypeInfoFactory.DATETIME) {
      return DatetimeWritable.class;
    }
    if (typeInfo == TypeInfoFactory.TIMESTAMP) {
      return TimestampWritable.class;
    }
    if (typeInfo == TypeInfoFactory.DATE) {
      return DateWritable.class;
    }
    if (typeInfo instanceof CharTypeInfo) {
      return CharWritable.class;
    }
    if (typeInfo instanceof VarcharTypeInfo) {
      return VarcharWritable.class;
    }
    if (typeInfo == TypeInfoFactory.BINARY) {
      return BytesWritable.class;
    }
    if (typeInfo == TypeInfoFactory.INTERVAL_DAY_TIME) {
      return IntervalDayTimeWritable.class;
    }
    if (typeInfo == TypeInfoFactory.INTERVAL_YEAR_MONTH) {
      return IntervalYearMonth.class;
    }
    if (typeInfo instanceof ArrayTypeInfo) {
      return ArrayWritable.class;
    }
    if (typeInfo instanceof MapTypeInfo) {
      return MapWritable.class;
    }
    if (typeInfo instanceof StructTypeInfo) {
      return StructWritable.class;
    }
    throw new IllegalArgumentException("unsupported data type:" + typeInfo.getTypeName());
  }

}
