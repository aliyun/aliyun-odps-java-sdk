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

package com.aliyun.odps.mapred.bridge.type;

import java.util.Comparator;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.io.BooleanWritable;
import com.aliyun.odps.io.DatetimeWritable;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;

/**
 * 自然顺序的比较器定义
 */
public class ComparatorFactory {

  @SuppressWarnings("rawtypes")
  public static Comparator getComparator(OdpsType type) {
    switch (type) {
      case STRING:
        return new StringComparator();
      case DOUBLE:
        return new DoubleComparator();
      case BIGINT:
        return new BigintComparator();
      case BOOLEAN:
        return new BooleanComparator();
      case DATETIME:
        return new DatetimeComparator();
      default:
        throw new RuntimeException("Unsupported type in compare:" + type);
    }
  }

  /**
   * 比较双方为String类型的比较器
   */
  public static class StringComparator implements Comparator<Text> {

    @Override
    public int compare(Text o1, Text o2) {
      if (o1 == null) {
        if (o2 == null) {
          return 0;
        }
        return -1;
      }
      if (o2 == null) {
        return 1;
      }
      return o1.compareTo(o2);
    }
  }

  /**
   * 比较双方为Double类型的比较器
   */
  public static class DoubleComparator implements Comparator<DoubleWritable> {

    @Override
    public int compare(DoubleWritable o1, DoubleWritable o2) {
      if (o1 == null) {
        if (o2 == null) {
          return 0;
        }
        return -1;
      }
      if (o2 == null) {
        return 1;
      }
      return o1.compareTo(o2);
    }
  }

  /**
   * 比较双方为Long类型的比较器
   */
  public static class BigintComparator implements Comparator<LongWritable> {

    @Override
    public int compare(LongWritable o1, LongWritable o2) {
      if (o1 == null) {
        if (o2 == null) {
          return 0;
        }
        return -1;
      }
      if (o2 == null) {
        return 1;
      }
      return o1.compareTo(o2);
    }
  }

  /**
   * 比较双方为Boolean类型的比较器
   */
  public static class BooleanComparator implements Comparator<BooleanWritable> {

    @Override
    public int compare(BooleanWritable o1, BooleanWritable o2) {
      if (o1 == null) {
        if (o2 == null) {
          return 0;
        }
        return -1;
      }
      if (o2 == null) {
        return 1;
      }
      return o1.compareTo(o2);
    }
  }

  /**
   * 比较双方为Datetime类型的比较器
   */
  public static class DatetimeComparator implements Comparator<DatetimeWritable> {

    @Override
    public int compare(DatetimeWritable o1, DatetimeWritable o2) {
      if (o1 == null) {
        if (o2 == null) {
          return 0;
        }
        return -1;
      }
      if (o2 == null) {
        return 1;
      }
      return o1.compareTo(o2);
    }
  }

}
