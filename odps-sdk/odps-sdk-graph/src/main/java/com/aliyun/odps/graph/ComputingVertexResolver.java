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

import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;

/**
 * ComputingVertexResolver 用于自定义迭代计算阶段图拓扑修改及冲突的处理逻辑.
 *
 * <p>
 * ODPS Graph 程序在图迭代计算过程中可以对图拓扑结构进行修改，包括添加、删除点或者边，详见框架提供的
 * {@link MutationContext} 接口说明，对图拓扑结构的修改请求可能引起下面几种类型的<b>“冲突”</b>：
 * <ul>
 * <li>添加重复点：通常使用 {@linkplain MutationContext#addVertexRequest(Vertex)
 * addVertexRequest} 添加点，而相同 ID 的点已经存在。
 * <li>添加重复边：通常使用
 * {@linkplain MutationContext#addEdgeRequest(WritableComparable, Edge)
 * addEdgeRequest} 添加边时，而相同起点和终点的边已经存在；或者使用
 * {@linkplain MutationContext#addVertexRequest(Vertex) addVertexRequest}
 * 添加点时，待添加点中存在重复边。
 * <li>删除不存在的边：通常使用
 * {@linkplain MutationContext#removeEdgeRequest(WritableComparable, WritableComparable)
 * removeEdgeRequest} 删除边时，待删除的边不存在。
 * <li>删除不存在的点：通常使用
 * {@linkplain MutationContext#removeVertexRequest(WritableComparable)
 * removeVertexRequest} 删除点时，待删除的点不存在。
 * <li>发送消息到不存在的点：通常使用
 * {@linkplain ComputeContext#sendMessage(WritableComparable, Writable)
 * sendMessage} 发送消息时，目标点不存在。
 * </ul>
 * 当上述冲突发生时：如果用户提供了自定义的实现类，框架会把同一个点相关的冲突信息封装成 {@link VertexChanges}，调用用户自定义类的
 * {@linkplain #resolveExistVertexMutations(WritableComparable，Vertex, VertexChanges)
 * resolveExistVertexMutations} 或者
 * {@linkplain #resolveNotExistVertexMutations(WritableComparable, VertexChanges, boolean)
 * resolveNotExistVertexMutations}
 * 方法进行处理；如果没有提供冲突处理的实现类，框架默认会抛异常。
 * </p> {@link GraphJob} 提供两个了设置自定义的实现类：
 * <ul>
 * <li>{@linkplain JobConf#setComputingVertexResolverClass(Class)
 * setComputingVertexResolverClass}
 * ，设置迭代计算过程中的图拓扑修改及冲突处理实现类，框架提供一个默认实现
 * {@link DefaultComputingVertexResolver}
 * </ul>
 *
 * @param <I>
 *     Vertex ID 类型
 * @param <V>
 *     Vertex Value 类型
 * @param <E>
 *     Edge Value 类型
 * @param <M>
 *     Message 类型
 * @see MutationContext
 * @see JobConf#setComputingVertexResolverClass(Class)
 * @see DefaultComputingVertexResolver
 */
@SuppressWarnings("rawtypes")
public abstract class ComputingVertexResolver<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
    extends VertexResolver<I, V, E, M> {

  /**
   * 此方法在对象被创建后立即调用，用户可以覆盖（override）本方法进行对象的初始化.
   *
   * @param conf
   *     运行时的JobConf对象
   */
  @Override
  public void configure(Configuration conf) throws IOException {

  }


  /**
   * 图拓扑修改及冲突处理方法，用户需要实现此方法自定义相关的点存在情况下的图拓扑修改及冲突处理逻辑.
   *
   * <p>
   * 在图载入和迭代计算过程中，用户可以调用 {@link MutationContext}
   * 接口对图进行修改，例如增加、删除点或者边，框架会将这些图拓扑变动请求按相关的点 （这里“相关的点”指待增加/删除点的 ID，待增加/删除边的起始点的
   * ID）进行归类，封装成 {@link VertexChanges} 对象，用户可以实现本方法对这些相关的图拓扑变动请求或冲突进行处理。
   * </p>
   *
   * <p>
   * 实现时，建议参考 {@link DefaultComputingVertexResolver} 的源代码，见在线文档。
   * </p>
   *
   * <p>
   * <b>注意：</b>基于性能上的考虑，框架直接使用本方法返回的 {@link Vertex} 对象，用户应保证本方法返回的
   * {@link Vertex} 对象及其成员变量在内部不被复用，否则会导致逻辑错误。
   * </p>
   *
   * @param vertexId
   *     待解决的相关点 ID，总不为 null
   * @param vertex
   *     与 vertexId 对应的原 {@link Vertex} 对象，总不为null
   * @param vertexChanges
   *     与 vertexId 相关的图拓扑变动请求
   * @return 返回的 {@link Vertex} 对象，如果不为 null，则添加到图中并替换原始点；
   * 如果为 null，则从图中删除原始点，应保证该对象及其成员变量不被复用
   */
  public abstract Vertex<I, V, E, M> resolveExistVertexMutations(I vertexId,
                                                                 Vertex<I, V, E, M> vertex,
                                                                 VertexChanges<I, V, E, M> vertexChanges)
      throws IOException;

  /**
   * 图拓扑修改及冲突处理方法，用户需要实现此方法自定义相关的点不存在情况下的图拓扑修改及冲突处理逻辑.
   *
   * <p>
   * 在图载入和迭代计算过程中，用户可以调用 {@link MutationContext}
   * 接口对图进行修改，例如增加、删除点或者边，框架会将这些图拓扑变动请求按相关的点 （这里“相关的点”指待增加/删除点的 ID，待增加/删除边的起始点的
   * ID）进行归类，封装成 {@link VertexChanges} 对象，用户可以实现本方法对这些相关的图拓扑变动请求或冲突进行处理。
   * </p>
   *
   * <p>
   * 实现时，建议参考 {@link DefaultComputingVertexResolver} 的源代码，见在线文档。
   * </p>
   *
   * <p>
   * <b>注意：</b>基于性能上的考虑，框架直接使用本方法返回的 {@link Vertex} 对象，用户应保证本方法返回的
   * {@link Vertex} 对象及其成员变量在内部不被复用，否则会导致逻辑错误。
   * </p>
   *
   * @param vertexId
   *     待解决冲突的点 ID，总不为 null
   * @param vertexChanges
   *     与 vertexId 相关的图拓扑变动请求
   * @param hasMessages
   *     上一轮迭代是否发送消息到此点
   * @return 返回的 {@link Vertex} 对象，如果不为 null，则添加到图中； 如果为 null，则忽略。
   */
  public abstract Vertex<I, V, E, M> resolveNotExistVertexMutations(I vertexId,
                                                                    VertexChanges<I, V, E, M> vertexChanges,
                                                                    boolean hasMessages)
      throws IOException;

  @Override
  final public Vertex<I, V, E, M> resolve(I vertexId, Vertex<I, V, E, M> vertex,
                                          VertexChanges<I, V, E, M> vertexChanges,
                                          boolean hasMessages)
      throws IOException {
    if (vertex == null) {
      return resolveNotExistVertexMutations(vertexId, vertexChanges, hasMessages);
    } else {
      return resolveExistVertexMutations(vertexId, vertex, vertexChanges);
    }
  }

}
