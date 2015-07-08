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

package com.aliyun.odps.graph;

import java.io.IOException;

import com.aliyun.odps.io.Writable;

/**
 * aggregate 和 merge 逻辑相同的 Aggregator 可以继承 BasicAggregator，简化实现.
 *
 * <p>
 * 对于某些 Aggregator 如 {@link com.aliyun.odps.graph.aggregators.LongMinAggregator}、{@link
 * com.aliyun.odps.graph.aggregators.LongSumAggregator}，可以继承该类简化实现，只需实现
 * {@link #createInitialValue(WorkerContext)} 和
 * {@link #aggregate(Writable, Object)} 两个方法，对于
 * {@link com.aliyun.odps.graph.aggregators.LongAvgAggregator} 则不适合，根据具体情况选择继承
 * {@link Aggregator} 还是 {@link BasicAggregator}。<br/>
 * </p>
 *
 * <p>
 * 其中:
 * <ul>
 * <li>{@link #merge(Writable, Writable)} 方法只简单调用 aggregate(value, partial)，用户可以
 * override；
 * <li>{@link #terminate(WorkerContext, Writable)} 方法只简单返回false，用户可以 override；
 * </ul>
 *
 * @param <VALUE>
 */
public abstract class BasicAggregator<VALUE extends Writable> extends
                                                              Aggregator<VALUE> {

  /*
   * (non-Javadoc)
   * 
   * @see com.aliyun.odps.graph.Aggregator#merge(com.aliyun.odps.io.Writable,
   * com.aliyun.odps.io.Writable)
   */
  @Override
  public void merge(VALUE value, VALUE partial) throws IOException {
    aggregate(value, partial);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.aliyun.odps.graph.Aggregator#terminate(com.aliyun.odps.graph.
   * WorkerContext, com.aliyun.odps.io.Writable)
   */
  @SuppressWarnings("rawtypes")
  @Override
  public boolean terminate(WorkerContext context, VALUE value)
      throws IOException {
    return false;
  }

}
