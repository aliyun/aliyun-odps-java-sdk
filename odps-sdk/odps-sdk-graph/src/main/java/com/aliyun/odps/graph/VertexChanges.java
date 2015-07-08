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

import java.util.List;

import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;

/**
 * VertexChanges 表示图拓扑的变动信息，如增加/删除点或者边.
 *
 * <p>
 * 在图载入和迭代计算过程中，用户可以调用 {@link MutationContext}
 * 接口对图进行修改，例如增加、删除点或者边，框架会将这些图拓扑变动请求按相关的点 （这里“相关的点”指待增加/删除点的 ID，待增加/删除边的起始点的
 * ID）进行归类，封装成 VertexChanges 对象，所以，VertexChanges 可以理解为与某个“Vertex
 * ID”相关的图拓扑结构变动请求信息。
 * </p>
 *
 * <p>
 * 框架要求提供 {@link VertexResolver} 实现用于对 VertexChanges 进行处理，具体如何处理
 * VertexChanges，详见 {@link VertexResolver}，这里只对 VertexChanges 的内容进行说明。
 * </p>
 *
 * @param <I>
 *     Vertex ID 类型
 * @param <V>
 *     Vertex Value 类型
 * @param <E>
 *     Edge Value 类型
 * @param <M>
 *     Message 类型
 * @see VertexResolver
 * @see MutationContext
 */
@SuppressWarnings("rawtypes")
public interface VertexChanges<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable> {

  /**
   * 请求添加的点列表，这些点具有相同的标识.
   *
   * <p>
   * 在图加载过程中或者上一轮迭代中，用户可以调用
   * {@linkplain MutationContext#addVertexRequest(Vertex) addVertexRequest}
   * 方法请求添加点，一个或多个相同 ID 的点会组成一个列表放到与该 ID 相关的 {@link VertexChanges}
   * 里，特殊地，如果只有一个点，那么列表中只包含一个点。
   * </p>
   *
   * @return 一个或多个相同 ID 的点组成的列表
   * @see MutationContext#addVertexRequest(Vertex)
   */
  public List<Vertex<I, V, E, M>> getAddedVertexList();

  /**
   * 请求删除该点的次数.
   *
   * <p>
   * 在图加载过程中或者上一轮迭代中，用户可以调用
   * {@linkplain MutationContext#removeVertexRequest(WritableComparable)
   * removeVertexRequest} 方法请求删除点，本方法返回请求删除点的次数。
   * </p>
   *
   * @return 请求删除该点的次数
   */
  public int getRemovedVertexCount();

  /**
   * 请求添加的边列表，这些边具有相同的起始点.
   *
   * <p>
   * 在图加载过程中或者上一轮迭代中，用户可以调用
   * {@linkplain MutationContext#addEdgeRequest(WritableComparable, Edge)
   * addEdgeRequest} 方法添加边，相同起始点的边会组成一个列表放到与该起始点 ID 相关的 {@link VertexChanges} 里。
   * </p>
   *
   * @return 请求添加的相同起始点的边列表
   */
  public List<Edge<I, E>> getAddedEdgeList();

  /**
   * 请求删除的边列表.
   *
   * <p>
   * 在图加载过程中或者上一轮迭代中，用户可以调用
   * {@linkplain MutationContext#removeEdgeRequest(WritableComparable, WritableComparable)
   * removeEdgeRequest} 方法请求删除边，相同起始点的边会组成一个列表放到与该起始点 ID 相关的
   * {@link VertexChanges} 里，本方法返回这些待删除边的终点组成的列表。
   * </p>
   *
   * @return 请求删除的边列表
   */
  public List<I> getRemovedEdgeList();
}
