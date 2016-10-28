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

import java.io.UnsupportedEncodingException;
import java.util.Date;

import com.aliyun.odps.io.BooleanWritable;
import com.aliyun.odps.io.DatetimeWritable;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.IntWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.local.LocalRunException;

public class LocalWritableUtils {

  public static Writable[] convert(Object[] arg0) throws LocalRunException {
    if (arg0 == null) {
      return null;
    }
    Writable[] result = new Writable[arg0.length];
    for (int i = 0; i < arg0.length; i++) {
      result[i] = convert(arg0[i]);
    }
    return result;
  }

  public static Writable convert(Object arg0) throws LocalRunException {
    if (arg0 == null) {
      return NullWritable.get();
    }
    if (arg0 instanceof String) {
      return new Text((String) arg0);
    }
    if (arg0 instanceof Integer) {
      return new IntWritable((Integer) arg0);
    }
    if (arg0 instanceof Long) {
      return new LongWritable((Long) arg0);
    }
    if (arg0 instanceof Boolean) {
      return new BooleanWritable((Boolean) arg0);
    }
    if (arg0 instanceof Double) {
      return new DoubleWritable((Double) arg0);
    }

    if (arg0 instanceof Date) {
      return new DatetimeWritable(((Date) arg0).getTime());
    }

    throw new LocalRunException("unsupported data type:" + arg0.getClass().getName());
  }

  public static Object convert(Writable arg0) throws LocalRunException {
    if (arg0 instanceof NullWritable) {
      return null;
    }
    if (arg0 instanceof Text) {
      try {
        return new String(((Text) arg0).getBytes(), "utf-8");
      } catch (UnsupportedEncodingException e) {
        throw new LocalRunException(e);
      }
    }
    if (arg0 instanceof IntWritable) {
      return ((IntWritable) arg0).get();
    }

    if (arg0 instanceof LongWritable) {
      return ((LongWritable) arg0).get();
    }

    if (arg0 instanceof BooleanWritable) {
      return ((BooleanWritable) arg0).get();
    }

    if (arg0 instanceof DoubleWritable) {
      return ((DoubleWritable) arg0).get();
    }

    if (arg0 instanceof DatetimeWritable) {
      return new Date(((DatetimeWritable) arg0).get());
    }

    throw new LocalRunException("unsupported data type:" + arg0.getClass().getName());
  }

}
