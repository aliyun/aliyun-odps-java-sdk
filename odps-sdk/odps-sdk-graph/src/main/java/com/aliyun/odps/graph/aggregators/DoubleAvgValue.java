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

package com.aliyun.odps.graph.aggregators;

import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 计算double类型平均值的aggregator使用的聚合数据类型
 */
public class DoubleAvgValue implements Writable {

  DoubleWritable sum;
  LongWritable count;
  DoubleWritable avg;

  public DoubleAvgValue() {
    sum = new DoubleWritable();
    count = new LongWritable();
    avg = new DoubleWritable();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    sum.write(out);
    count.write(out);
    avg.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    sum.readFields(in);
    count.readFields(in);
    avg.readFields(in);
  }

  /**
   * 获取总和
   *
   * @return 总和
   */
  public DoubleWritable getSum() {
    return sum;
  }

  /**
   * 获取计数
   *
   * @return 计数
   */
  public LongWritable getCount() {
    return count;
  }

  /**
   * 获取平均值
   *
   * @return 平均值
   */
  public DoubleWritable getAvg() {
    return avg;
  }

  @Override
  public String toString() {
    return "sum = " + sum + ", count = " + count + ", avg = " + avg;
  }

}