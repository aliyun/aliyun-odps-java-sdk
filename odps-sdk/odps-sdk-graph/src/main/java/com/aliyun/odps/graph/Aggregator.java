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
 * Aggregator 用于汇总并处理全局信息.
 *
 * <p>
 * 通过 {@link GraphJob} 的 setAggregatorClass(Class ...) 方法提供自定义聚合器，支持提供一个或多个
 * Aggregators，每个聚合器对应一个从0开始的编号.
 * <p>
 *
 * <p>
 * 使用 Aggregator 进行全局计算，例如进行全局信息统计，检查迭代是否收敛等，在每一轮迭代中，Aggregator 的计算流程如下：
 * <ol>
 * <li>迭代开始前，实例化所有的 Aggregators，并调用 {@link Aggregator#createInitialValue(WorkerContext)}
 * 创建初始值；
 * <li>迭代进行中，在 {@link Vertex#compute(ComputeContext, Iterable)} 中
 * <ul>
 * <li>通过 {@link ComputeContext#getLastAggregatedValue(int)} （参数为 Aggregator
 * 编号）可以取得上一轮迭代的聚合结果，对于第一轮迭代（超步为0），该方法返回 null；
 * <li>通过 {@link ComputeContext#aggregate(Object)} 和
 * {@link ComputeContext#aggregate(int, Object)} 进行局部（当前 Worker 内）聚合；
 * </ul>
 * <li>迭代结束时
 * <ul>
 * <li>所有的局部聚合结果会发送给 Master 进行全局合并（使用 {@link Aggregator#merge(Writable, Writable)}；
 * <li>最后调用 {@link Aggregator#terminate(WorkerContext, Writable)} 计算最终结果，并广播给所有的
 * Worker，这样在下一轮迭代可以取得上一轮的聚合计算结果；
 * </ul>
 * </ol>
 * <p>
 *
 * <p>
 * {@link Aggregator#terminate(WorkerContext, Writable)} 是一个比较特殊的函数，如果某个 Aggregator 的
 * terminate 方法返回了 true，迭代会终止，默认返回false，这个特性可以用于收敛判断，可以参考 Kmeans 的实现.
 * <p>
 *
 * @param <VALUE>
 *     Aggregator的值类型
 * @see ComputeContext#aggregate(Object)
 * @see ComputeContext#aggregate(int, Object)
 * @see ComputeContext#getLastAggregatedValue(int)
 */
public abstract class Aggregator<VALUE extends Writable> {

  /**
   * 初始值创建函数（Worker在所有超步开始前调用，只调用一次）.
   *
   * <p>
   * 本函数默认实现只是返回 null，用户可以根据需要 override 此函数.
   * <p>
   *
   * <p>
   * Worker 在启动时会调用 {@link #createStartupValue(WorkerContext)} 创建 Aggregator
   * 初始值，这些初始值可以在 {@link Vertex#setup(WorkerContext)} 或第一轮超步中可以通过
   * {@link ComputeContext#getLastAggregatedValue(int)} 取得；
   * <p>
   *
   * @param context
   *     Worker 上下文
   * @return 此聚合器的初始值
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public VALUE createStartupValue(WorkerContext context) throws IOException {
    return null;
  }

  /**
   * 初始值创建函数（超步开始时调用，每一轮超步都会调用）.
   *
   * <p>
   * 每一轮超步开始前，Worker 会调用 {@link #createInitialValue(WorkerContext)} 方法创建
   * Aggregator 初始值，作为本轮超步的初始值；
   * <p>
   *
   * @param context
   *     Worker 上下文
   * @return 此聚合器的初始值
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public abstract VALUE createInitialValue(WorkerContext context)
      throws IOException;

  /**
   * 聚合函数.
   *
   * <p>
   * 迭代进行中，用户可以在 Vertex 的 Compute方法中调用 {@link ComputeContext#aggregate(Object)}
   * 和 {@link ComputeContext#aggregate(int, Object)} 进行局部聚合，.
   * <p>
   *
   * @param value
   *     此聚合器的值
   * @param item
   *     待聚合的对象
   * @throws IOException
   */
  public abstract void aggregate(VALUE value, Object item) throws IOException;

  /**
   * 合并函数.
   *
   * <p>
   * 迭代结束时，所有的局部聚合结果会发送给 Master，使用本方法进行全局合并,在只有
   * 一个worker的情况下，该方法不会被调用.
   * <p>
   *
   * @param value
   *     此聚合器的值
   * @param partial
   *     局部聚合结果
   * @throws IOException
   */
  public abstract void merge(VALUE value, VALUE partial) throws IOException;

  /**
   * 结束函数.
   *
   * <p>
   * 迭代结束时，Master 收到所有局部聚合结果并完成合并后，会调用
   * {@link #terminate(WorkerContext, Writable)} 计算最终结果，并广播给所有的
   * Worker，这样在下一轮迭代可以取得上一轮的聚合计算结果.
   * <p>
   *
   * <p>
   * terminate 是一个比较特殊的函数，如果某个 Aggregator 的 terminate 方法返回了
   * true，迭代会终止，默认返回false，这个特性可以用于收敛判断，可以参考 Kmeans 的实现.
   * <p>
   *
   * @param context
   *     Worker 上下文
   * @param value
   *     局部聚合结果并完成合并后的值
   * @return 是否收敛，true 表示收敛，迭代终止，false 表示未收敛，迭代继续
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public abstract boolean terminate(WorkerContext context, VALUE value)
      throws IOException;

}
