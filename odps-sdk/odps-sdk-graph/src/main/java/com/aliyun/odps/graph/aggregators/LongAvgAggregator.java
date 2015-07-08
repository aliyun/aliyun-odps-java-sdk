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

import java.io.IOException;

import com.aliyun.odps.graph.Aggregator;
import com.aliyun.odps.graph.WorkerContext;
import com.aliyun.odps.io.LongWritable;

/**
 * 计算long类型平均值的aggregator
 */
public class LongAvgAggregator extends Aggregator<LongAvgValue> {

  @SuppressWarnings("rawtypes")
  @Override
  public LongAvgValue createInitialValue(WorkerContext context)
      throws IOException {
    return new LongAvgValue();
  }

  @Override
  public void aggregate(LongAvgValue value, Object lw) {
    value.sum.set(value.sum.get() + ((LongWritable) lw).get());
    value.count.set(value.count.get() + 1);
  }

  @Override
  public void merge(LongAvgValue value, LongAvgValue partial) {
    value.sum.set(value.sum.get() + partial.sum.get());
    value.count.set(value.count.get() + partial.count.get());
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean terminate(WorkerContext context, LongAvgValue value)
      throws IOException {
    if (value.count.get() > 0) {
      value.avg.set(value.sum.get() / value.count.get());
    }
    return false;
  }
}
