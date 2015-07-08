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
 * 获取long类型计数的aggregator
 */
public class LongCountAggregator extends Aggregator<LongWritable> {

  @SuppressWarnings("rawtypes")
  @Override
  public LongWritable createInitialValue(WorkerContext context)
      throws IOException {
    return new LongWritable(0);
  }

  @Override
  public void aggregate(LongWritable value, Object item) throws IOException {
    value.set(value.get() + 1);
  }

  @Override
  public void merge(LongWritable value, LongWritable partial)
      throws IOException {
    value.set(value.get() + partial.get());
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean terminate(WorkerContext context, LongWritable value)
      throws IOException {
    return false;
  }
}
