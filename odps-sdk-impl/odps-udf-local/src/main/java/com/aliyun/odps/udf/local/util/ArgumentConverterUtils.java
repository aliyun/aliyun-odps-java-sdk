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

package com.aliyun.odps.udf.local.util;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.IntervalDayTime;
import com.aliyun.odps.data.IntervalYearMonth;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArgumentConverterUtils {

  public static HashMap<String, ArgumentConverter>
      validSigType =
      new HashMap<String, ArgumentConverter>();

  static {
    validSigType.put("string", new StringConverter());
    validSigType.put("bigint", new LongConverter());
    validSigType.put("boolean", new BooleanConverter());
    validSigType.put("double", new DoubleConverter());
    validSigType.put("datetime", new DatetimeConverter());
    validSigType.put("decimal", new DecimalConverter());
    validSigType.put("tinyint", new TinyintConverter());
    validSigType.put("smallint", new SmallintConverter());
    validSigType.put("int", new IntConverter());
    validSigType.put("float", new FloatConverter());
    validSigType.put("char", new CharConverter());
    validSigType.put("varchar", new VarcharConverter());
    validSigType.put("date", new DateConverter());
    validSigType.put("timestamp", new TimeStampConverter());
    validSigType.put("binary", new BinaryConverter());
    validSigType.put("interval_day_time", new IntervalDayTimeConverter());
    validSigType.put("interval_year_month", new IntervalYearMonthConverter());
    validSigType.put("map", new MapConverter());
    validSigType.put("array", new ArrayConverter());
    validSigType.put("struct", new StructConverter());
    validSigType.put("void", new VoidConverter());
  }

  public static String getSigType(TypeInfo typeInfo) {
    if (typeInfo instanceof ArrayTypeInfo) {
      return "array";
    } else if (typeInfo instanceof MapTypeInfo) {
      return "map";
    } else if (typeInfo instanceof StructTypeInfo) {
      return "struct";
    } else {
      return typeInfo.getTypeName().toLowerCase();
    }
  }

  public abstract static class ArgumentConverter {
    public abstract void check(Object o);
  }

  public static class StringConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof String)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
                                   + "' can't cast to '" + String.class.getName() + "'");
      }
    }
  }

  public static class LongConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Long)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
                                   + "' can't cast to '" + Long.class.getName() + "'");
      }
    }
  }

  public static class DoubleConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Double)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
                                   + "' can't cast to '" + Double.class.getName() + "'");
      }
    }
  }

  public static class BooleanConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Boolean)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
                                   + "' can't cast to '" + Boolean.class.getName() + "'");
      }
    }
  }

  public static class DatetimeConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Long)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
                                   + "' can't cast to '" + Long.class.getName() + "'");
      }
    }
  }

  public static class DecimalConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof BigDecimal)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + BigDecimal.class.getName() + "'");
      }
    }
  }

  public static class TinyintConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Byte)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Byte.class.getName() + "'");
      }
    }
  }

  public static class SmallintConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Short)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Short.class.getName() + "'");
      }
    }
  }

  public static class IntConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Integer)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Integer.class.getName() + "'");
      }
    }
  }
  public static class FloatConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Float)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Float.class.getName() + "'");
      }
    }
  }
  public static class CharConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Char)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Char.class.getName() + "'");
      }
    }
  }
  public static class VarcharConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Varchar)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Varchar.class.getName() + "'");
      }
    }
  }
  public static class DateConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Date)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Date.class.getName() + "'");
      }
    }
  }
  public static class TimeStampConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Timestamp)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Timestamp.class.getName() + "'");
      }
    }
  }
  public static class BinaryConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Binary)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Binary.class.getName() + "'");
      }
    }
  }
  public static class IntervalDayTimeConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof IntervalDayTime)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + IntervalDayTime.class.getName() + "'");
      }
    }
  }

  public static class IntervalYearMonthConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof IntervalYearMonth)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + IntervalYearMonth.class.getName() + "'");
      }
    }
  }
  public static class MapConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Map)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Map.class.getName() + "'");
      }
    }
  }
  public static class ArrayConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof List)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + List.class.getName() + "'");
      }
    }
  }

  public static class StructConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Struct)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Struct.class.getName() + "'");
      }
    }
  }
  public static class VoidConverter extends ArgumentConverter {
    @Override
    public void check(Object o) {
      if (o != null && !(o instanceof Void)) {
        throw new RuntimeException("Input data type '" + o.getClass().getName()
          + "' can't cast to '" + Void.class.getName() + "'");
      }
    }
  }
}
