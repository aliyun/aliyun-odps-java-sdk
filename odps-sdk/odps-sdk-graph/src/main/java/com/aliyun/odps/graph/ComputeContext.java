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
import com.aliyun.odps.io.WritableComparable;

/**
 * ComputeContext 继承自 {@link WorkerContext}，传给
 * {@link Vertex#compute(ComputeContext, Iterable)} 方法的上下文.
 *
 * <p>
 * 在 {@link WorkerContext} 基础上，ComputeContext 提供了一些新功能，这些功能只能在
 * {@link Vertex#compute(ComputeContext, Iterable)} 方法中使用.
 *
 * @param <VERTEX_ID>
 *     Vertex ID 类型
 * @param <VERTEX_VALUE>
 *     Vertex Value 类型
 * @param <EDGE_VALUE>
 *     Edge Value 类型
 * @param <MESSAGE>
 *     Message 类型
 */
@SuppressWarnings("rawtypes")
public abstract class ComputeContext<VERTEX_ID extends WritableComparable, VERTEX_VALUE extends Writable, EDGE_VALUE extends Writable, MESSAGE extends Writable>
    extends WorkerContext<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE, MESSAGE>
    implements MutationContext<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE, MESSAGE> {

  /**
   * 发送消息到目标点，在下一个超步中会传给目标点的 {@link Vertex#compute(ComputeContext, Iterable)} 方法进行处理.
   *
   * @param destVertexID
   *     目标点 ID
   * @param msg
   *     待发送的消息
   * @throws IOException
   */
  public abstract void sendMessage(VERTEX_ID destVertexID, MESSAGE msg)
      throws IOException;

  /**
   * 发送一条消息到多个点。
   * 如果使用{@link GraphJob#setBroadcastMessageEnable(true)}开启广播优化，则msg参数
   * 不能在多次调用中复用。
   *
   * @param destVertexIDs
   *     目标点的集合
   * @param msg
   *     待发送的消息
   * @throws IOException
   */
  public abstract void sendMessage(Iterable<VERTEX_ID> destVertexIDs, MESSAGE msg)
      throws IOException;

  /**
   * 发送消息到给定点的所有邻接点，在下一个超步中会传给这些邻接点的
   * {@link Vertex#compute(ComputeContext, Iterable)} 方法进行处理.
   *
   * @param vertex
   *     目标点
   * @param msg
   *     待发送的消息
   * @throws IOException
   */
  public abstract void sendMessageToNeighbors(
      Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE, MESSAGE> vertex, MESSAGE msg)
      throws IOException;

  /**
   * 聚合函数，item 对象传给所有的 Aggregators 进行聚合，通常在
   * {@link Vertex#compute(ComputeContext, Iterable)} 方法中调用.
   *
   * <p>
   * 如果 item 对象需要传给某一个 Aggregator 进行聚合，请使用 {@link #aggregate(int, Object)}.
   * </p>
   *
   * @param item
   *     待聚合对象
   * @throws IOException
   * @see Aggregator#aggregate(Writable, Object)
   * @see GraphJob#setAggregatorClass(Class...)
   * @see Vertex#compute(ComputeContext, Iterable)
   */
  public abstract void aggregate(Object item) throws IOException;

  /**
   * 聚合函数，item 对象传给给定编号（起始为 0）的 Aggregator 进行聚合，通常在
   * {@link Vertex#compute(ComputeContext, Iterable)} 方法中调用.
   *
   * <p>
   * 如果 item 对象需要传给所有 Aggregators 进行聚合，请使用 {@link #aggregate(Object)}.
   * </p>
   *
   * @param aggregatorIndex
   *     Aggregator编号，起始为 0
   * @param item
   *     待聚合对象
   * @throws IOException
   * @see Aggregator#aggregate(Writable, Object)
   * @see GraphJob#setAggregatorClass(Class...)
   * @see Vertex#compute(ComputeContext, Iterable)
   */
  public abstract void aggregate(int aggregatorIndex, Object item)
      throws IOException;
}
