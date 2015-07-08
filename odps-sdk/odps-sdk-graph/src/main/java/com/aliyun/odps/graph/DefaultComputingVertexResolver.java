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
 * DefaultComputingVertexResolver 用于处理迭代计算时产生的图拓扑修改及冲突.
 *
 * <p>
 * 在迭代计算阶段，用户可以调用 {@link MutationContext} 的接口向图中添加、删除点或边，由此引入的图拓扑修改及冲突默认由此类解决，用户也可以通过
 * {@link GraphJob} 提供的
 * {@linkplain JobConf#setComputingVertexResolverClass(Class)
 * setComputingVertexResolverClass} 方法指定自己的实现。
 * </p>
 *
 * <p>
 * 对于同一个点ID，DefaultComputingVertexResolver 解决该点在迭代计算阶段产生的冲突是按照以下顺序进行的：
 * <ol>
 * <li>解决 {@linkplain MutationContext#addVertexRequest(Vertex) addVertexRequest}
 * 引起的冲突： 添加点时，如果点已经存在或者待添加的点存在重复边（起始和终点相同的边），则报错。
 * <li>解决 {@linkplain MutationContext#addEdgeRequest(WritableComparable, Edge)
 * addEdgeRequest} 引起的冲突： 添加边时，如果对应起始点不存在或者导致重复边，则报错。
 * <li>
 * 解决
 * {@linkplain MutationContext#removeEdgeRequest(WritableComparable, WritableComparable)
 * removeEdgeRequest} 引起的冲突： 删除边时，如果该边不存在，则报错。
 * <li>解决 {@linkplain MutationContext#removeVertexRequest(WritableComparable)
 * removeVertexRequest} 引起的冲突：删除点时，如果该点不存在，则报错。
 * <li>解决 {@linkplain ComputeContext#sendMessage(WritableComparable, Writable)
 * sendMessage} 引起的冲突：如果发送消息时，目标点不存在，则报错.
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
 * @see JobConf#setComputingVertexResolverClass(Class)
 */
@SuppressWarnings("rawtypes")
public class DefaultComputingVertexResolver<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
    extends ComputingVertexResolver<I, V, E, M> {

  @Override
  public Vertex<I, V, E, M> resolveExistVertexMutations(I vertexId,
                                                        Vertex<I, V, E, M> vertex,
                                                        VertexChanges<I, V, E, M> vertexChanges)
      throws IOException {

    /**
     * 1. If add vertex desired, throw exception.
     */
    if (hasVertexAdditions(vertexChanges)) {
      throw new IOException("ODPS-0730001: And vertex(Id: '"
                            + vertexId + "') that already exists");
    }

    /** 2. If edge addition, add the edges */
    addEdges(vertexId, vertex, vertexChanges);

    /** 3. If the vertex exists, first prune the edges. */
    removeEdges(vertexId, vertex, vertexChanges);

    /** 4. If vertex removal desired, remove the vertex. */
    vertex = removeVertexIfDesired(vertexId, vertex, vertexChanges);

    return vertex;

  }

  @Override
  public Vertex<I, V, E, M> resolveNotExistVertexMutations(I vertexId,
                                                           VertexChanges<I, V, E, M> vertexChanges,
                                                           boolean hasMessages) throws IOException {

    /**
     * 1. If creation of vertex desired, pick first vertex. 
     */
    Vertex<I, V, E, M> vertex = addVertexIfDesired(vertexId, vertexChanges);

    /** 2. If edge addition, add the edges */
    addEdges(vertexId, vertex, vertexChanges);

    /** 3. If the vertex exists, first prune the edges. */
    removeEdges(vertexId, vertex, vertexChanges);

    /** 4. If vertex removal desired, remove the vertex. */
    vertex = removeVertexIfDesired(vertexId, vertex, vertexChanges);

    /** 5. If send messages to not exist vertex, throw exception.*/
    if (vertex == null && hasMessages) {
      throw new IOException("ODPS-0730001: Send messages to vertex(Id: '"
                            + vertexId + "') that is not exist");
    }

    return vertex;
  }

  /**
   * 迭代计算阶段，处理删除边的请求。
   *
   * @param vertexId
   *     请求删除的边所在的点的ID
   * @param vertex
   *     请求删除的边所在的点
   * @param vertexChanges
   *     包含要从该点中删除的边
   * @throws IOException
   *     请求删除边时，点不存在，或者删除不存在的边
   */
  protected void removeEdges(I vertexId, Vertex<I, V, E, M> vertex,
                             VertexChanges<I, V, E, M> vertexChanges) throws IOException {
    if (hasEdgeRemovals(vertexChanges)) {
      if (vertex == null) {
        throw new IOException("ODPS-0730001: Remove edge from vertex(Id: '"
                              + vertexId + "') that does not exist");
      }
      if (!vertex.hasEdges()) {
        throw new IOException("ODPS-0730001: Remove edge from vertex(Id: '"
                              + vertexId + "') that does not has edges");
      }

      for (I removedDestVertex : vertexChanges.getRemovedEdgeList()) {
        boolean found = false;
        List<Edge<I, E>> edgeList = vertex.getEdges();
        for (Iterator<Edge<I, E>> edges = edgeList.iterator(); edges.hasNext(); ) {
          Edge<I, E> edge = edges.next();
          if (edge.getDestVertexId().equals(removedDestVertex)) {
            edges.remove();
            found = true;
          }
        }
        if (!found) {
          throw new IOException("ODPS-0730001:Remove edge(DestVertexId: '"
                                + removedDestVertex + "') that does not exist for " + vertex);
        }
      }
    }
  }

  /**
   * 迭代计算阶段，处理删除点的请求。如果存在删除点的请求，则返回null，否则返回输入 的{@link Vertex}参数.
   *
   * @param vertexId
   *     请求删除的点的ID
   * @param vertex
   *     请求删除的点
   * @param vertexChanges
   *     包含是否要删除点的请求
   * @return 如果请求删除点，则返回null，否则返回vertex本身
   * @throws IOException
   *     请求删除点时，点不存在
   */
  protected Vertex<I, V, E, M> removeVertexIfDesired(I vertexId,
                                                     Vertex<I, V, E, M> vertex,
                                                     VertexChanges<I, V, E, M> vertexChanges)
      throws IOException {
    if (hasVertexRemovals(vertexChanges)) {
      if (vertex == null) {
        throw new IOException("ODPS-0730001: Remove vertex(Id: '" + vertexId
                              + "') that does not exist");
      } else {
        vertex = null;
      }
    }
    return vertex;
  }

  /**
   * 迭代计算阶段，处理添加点的请求.
   *
   * @param vertexId
   *     请求添加的点的ID
   * @param vertexChanges
   *     包含请求添加的点
   * @return 请求添加的点，或者没有请求添加点时，返回null
   * @throws IOException
   *     多次（大于一次）请求添加点
   */
  protected Vertex<I, V, E, M> addVertexIfDesired(I vertexId,
                                                  VertexChanges<I, V, E, M> vertexChanges)
      throws IOException {
    Vertex<I, V, E, M> vertex = null;
    if (hasVertexAdditions(vertexChanges)) {
      if (vertexChanges.getAddedVertexList().size() > 1) {
        throw new IOException("ODPS-0730001: Add duplicate vertices for id: '"
                              + vertexId + "'");
      }
      vertex = vertexChanges.getAddedVertexList().get(0);
    }

    return vertex;
  }

  /**
   * 迭代计算阶段，处理添加边的请求.
   *
   * @param vertexId
   *     请求添加的边所在的点的ID
   * @param vertex
   *     请求点的边所在在的点
   * @param vertexChanges
   *     包含请求添加的边
   * @throws IOException
   *     点本身拥有的边存在重复边，或者请求添加的边中存在重复边
   */
  protected void addEdges(I vertexId, Vertex<I, V, E, M> vertex,
                          VertexChanges<I, V, E, M> vertexChanges) throws IOException {
    Set<I> destVertexId = new HashSet<I>();
    if (vertex != null && vertex.hasEdges()) {
      for (Edge<I, E> edge : vertex.getEdges()) {
        if (destVertexId.contains(edge.getDestVertexId())) {
          throw new IOException(
              "ODPS-0730001: Duplicate edges for vertex, from '"
              + vertex.getId() + "' to '" + edge.getDestVertexId() + "'");
        }
        destVertexId.add(edge.getDestVertexId());
      }
    }

    if (hasEdgeAdditions(vertexChanges)) {
      if (vertex == null) {
        throw new IOException("ODPS-0730001: Add edge to vertex(Id: '" + vertexId
                              + "') that does not exist");
      }

      /** Check whether or not vertex has duplicate edges */
      for (Edge<I, E> edge : vertexChanges.getAddedEdgeList()) {
        if (destVertexId.contains(edge.getDestVertexId())) {
          throw new IOException(
              "ODPS-0730001: Add duplicate edges for vertex, from '"
              + vertex.getId() + "' to '" + edge.getDestVertexId() + "'");
        }
        destVertexId.add(edge.getDestVertexId());
        vertex.addEdge(edge.getDestVertexId(), edge.getValue());
      }
    }
  }


  /**
   * 检查是否存在删除点的请求。
   *
   * @param changes
   *     待检查的点变化的集合
   * @return 集合中包含删除点的请求，返回true，否则返回false
   */
  protected boolean hasVertexRemovals(VertexChanges<I, V, E, M> changes) {
    return changes != null && changes.getRemovedVertexCount() > 0;
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

  /**
   * 检查是否存在删除边的请求。
   *
   * @param changes
   *     待检查的点变化的集合
   * @return 集合中包含删除边的请求，则返回true，否则返回false
   */
  protected boolean hasEdgeRemovals(VertexChanges<I, V, E, M> changes) {
    return changes != null && changes.getRemovedEdgeList() != null
           && !changes.getRemovedEdgeList().isEmpty();
  }


}
