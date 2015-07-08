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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;

/**
 * RemoveDuplicatesLoadingResolver 是解决 {@link GraphLoader} 载入图数据时引入的点冲突的一种实现.
 *
 * <p>
 * 在图载入阶段，用户可以调用 {@link MutationContext} 的接口向图中添加、删除点或边，由此引入的冲突默认由此类解决， 用户也可以通过
 * {@link GraphJob} 提供的
 * {@linkplain JobConf#setLoadingVertexResolverClass(Class)
 * setLoadingVertexResolverClass} 方法指定自己的实现。
 * </p>
 *
 * <p>
 * 对于同一个点ID，RemoveDuplicatesLoadingResolver 解决该点在图载入阶段的冲突是按照以下顺序进行的：
 * <ol>
 * <li>解决 {@linkplain MutationContext#addVertexRequest(Vertex) addVertexRequest}
 * 引起的冲突： 添加点时，选择第一个添加的点。
 * <li>解决 {@linkplain MutationContext#addEdgeRequest(WritableComparable, Edge)
 * addEdgeRequest} 引起的冲突： 添加边时，首先删除点中已有的重复边（终点相同），然后添加不重复的边。
 * <li>
 * 忽略
 * {@linkplain MutationContext#removeEdgeRequest(WritableComparable, WritableComparable)
 * removeEdgeRequest} 以及 {@linkplain MutationContext#removeVertexRequest(WritableComparable)
 * removeVertexRequest}。
 * </ol>
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
 * @see JobConf#setLoadingVertexResolverClass(Class)
 */
@SuppressWarnings("rawtypes")
public class RemoveDuplicatesLoadingResolver<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
    extends LoadingVertexResolver<I, V, E, M> {

  /**
   * 提供图载入时的一种去重冲突处理方法.
   *
   *
   * <p>
   * 首先处理添加点请求，然后处理添加边的请求，详细处理规则见：{@linkplain RemoveDuplicatesLoadingResolver
   * 本类说明}
   * </p>
   *
   * @param vertexId
   *     冲突点的ID
   * @param vertexChanges
   *     关于该点的添加和删除请求
   */
  @Override
  public Vertex<I, V, E, M> resolve(I vertexId, VertexChanges<I, V, E, M> vertexChanges)
      throws IOException {

    /**
     * 1. If creation of vertex desired, pick first vertex.
     */
    Vertex<I, V, E, M> vertex = addVertexIfDesired(vertexId, vertexChanges);

    if (vertex != null) {
      /** 2. If edge addition, add the unique edges */
      addEdges(vertexId, vertex, vertexChanges);
    } else {
      System.err.println("Ignore all addEdgeRequests for vertex#" + vertexId);
    }

    return vertex;
  }


  /**
   * 图载入阶段，处理添加点的请求.
   *
   * @param vertexId
   *     请求添加的点的ID
   * @param vertexChanges
   *     包含请求添加的点
   * @return 第一个请求添加的点，或者没有请求添加点时，返回null
   */
  protected Vertex<I, V, E, M> addVertexIfDesired(I vertexId,
                                                  VertexChanges<I, V, E, M> vertexChanges) {
    Vertex<I, V, E, M> vertex = null;
    if (hasVertexAdditions(vertexChanges)) {
      vertex = vertexChanges.getAddedVertexList().get(0);
    }

    return vertex;
  }

  /**
   * 图载入阶段，处理添加边的请求.
   *
   * @param vertexId
   *     请求添加的边所在的点的ID
   * @param vertex
   *     请求点的边所在的点
   * @param vertexChanges
   *     包含请求添加的边
   * @throws IOException
   *     去除点本身拥有的重复边，以及请求添加的边中的重复边
   */
  protected void addEdges(I vertexId, Vertex<I, V, E, M> vertex,
                          VertexChanges<I, V, E, M> vertexChanges) throws IOException {
    // I. Remove duplicate edges from vertex's edge list.
    Set<I> destVertexId = new HashSet<I>();
    if (vertex.hasEdges()) {
      List<Edge<I, E>> edgeList = vertex.getEdges();
      for (Iterator<Edge<I, E>> edges = edgeList.iterator(); edges.hasNext(); ) {
        Edge<I, E> edge = edges.next();
        if (destVertexId.contains(edge.getDestVertexId())) {
          edges.remove();
        } else {
          destVertexId.add(edge.getDestVertexId());
        }
      }
    }

    if (hasEdgeAdditions(vertexChanges)) {

      // II. Ignore duplicate edge request 
      for (Edge<I, E> edge : vertexChanges.getAddedEdgeList()) {
        if (destVertexId.contains(edge.getDestVertexId())) {
          continue;
        }
        destVertexId.add(edge.getDestVertexId());
        vertex.addEdge(edge.getDestVertexId(), edge.getValue());
      }
    }
  }

  /**
   * 检查是否存在添加点的请求。
   *
   * @param changes
   *     待检查的点变化的集合
   * @return 集合中包含添加点的请求，返回true，否则返回false
   */
  protected boolean hasVertexAdditions(VertexChanges<I, V, E, M> changes) {
    return changes != null && changes.getAddedVertexList() != null
           && !changes.getAddedVertexList().isEmpty();
  }

  /**
   * 检查是否存在添加边的请求。
   *
   * @param changes
   *     待检查的点变化的集合
   * @return 集合中包含添加边的请求，则返回true，否则返回false
   */
  protected boolean hasEdgeAdditions(VertexChanges<I, V, E, M> changes) {
    return changes != null && changes.getAddedEdgeList() != null
           && !changes.getAddedEdgeList().isEmpty();
  }

}
