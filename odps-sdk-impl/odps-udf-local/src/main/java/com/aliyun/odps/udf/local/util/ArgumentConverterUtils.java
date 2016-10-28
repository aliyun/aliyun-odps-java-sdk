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

import java.util.Date;
import java.util.HashMap;

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
}
