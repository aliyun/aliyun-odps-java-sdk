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

package com.aliyun.odps.graph.local;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.graph.Edge;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.VertexChanges;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;

/**
 * Structure to hold all the possible graph mutations that can occur during a
 * superstep.
 *
 * @param <I>
 *     Vertex index value
 * @param <V>
 *     Vertex value
 * @param <E>
 *     Edge value
 * @param <M>
 *     Message value
 */
@SuppressWarnings("rawtypes")
public class LocalVertexMutations<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
    implements VertexChanges<I, V, E, M> {

  /**
   * List of added vertices during the last superstep
   */
  private List<Vertex<I, V, E, M>> addedVertexList;
  /**
   * Count of remove vertex requests
   */
  private int removedVertexCount = 0;
  /**
   * List of added edges
   */
  private List<Edge<I, E>> addedEdgeList;
  /**
   * List of removed edges
   */
  private List<I> removedEdgeList;

  public LocalVertexMutations() {

  }

  /**
   * Copy the vertex mutations.
   *
   * @return Copied vertex mutations
   */

  @Override
  public List<Vertex<I, V, E, M>> getAddedVertexList() {
    return addedVertexList;
  }

  /**
   * Add a vertex mutation
   *
   * @param vertex
   *     Vertex to be added
   */
  public void addVertex(Vertex<I, V, E, M> vertex) {
    if (addedVertexList == null) {
      addedVertexList = new ArrayList<Vertex<I, V, E, M>>(1);
    }
    addedVertexList.add(vertex);
  }

  @Override
  public int getRemovedVertexCount() {
    return removedVertexCount;
  }

  /**
   * Removed a vertex mutation (increments a count)
   */
  public void removeVertex() {
    ++removedVertexCount;
  }

  @Override
  public List<Edge<I, E>> getAddedEdgeList() {
    return addedEdgeList;
  }

  /**
   * Add an edge to this vertex
   *
   * @param edge
   *     Edge to be added
   */
  public void addEdge(Edge<I, E> edge) {
    if (addedEdgeList == null) {
      addedEdgeList = new ArrayList<Edge<I, E>>(1);
    }
    addedEdgeList.add(edge);
  }

  @Override
  public List<I> getRemovedEdgeList() {
    return removedEdgeList;
  }

  /**
   * Remove an edge on this vertex
   *
   * @param destinationVertexId
   *     Vertex index of the destination of the edge
   */
  public void removeEdge(I destinationVertexId) {
    if (removedEdgeList == null) {
      removedEdgeList = new ArrayList<I>(1);
    }
    removedEdgeList.add(destinationVertexId);
  }

  /**
   * Add one vertex mutations to another
   *
   * @param vertexMutations
   *     Object to be added
   */
  public void addVertexMutations(LocalVertexMutations<I, V, E, M> other) {
    List<Vertex<I, V, E, M>> otherAddedVertexList = other.getAddedVertexList();
    if (otherAddedVertexList != null) {
      if (addedVertexList == null) {
        addedVertexList = new ArrayList<Vertex<I, V, E, M>>(
            otherAddedVertexList);
      } else {
        addedVertexList.addAll(otherAddedVertexList);
      }
    }
    removedVertexCount += other.getRemovedVertexCount();
    List<Edge<I, E>> otherAddedEdgeList = other.getAddedEdgeList();
    if (otherAddedEdgeList != null) {
      if (addedEdgeList == null) {
        addedEdgeList = new ArrayList<Edge<I, E>>(otherAddedEdgeList);
      } else {
        addedEdgeList.addAll(otherAddedEdgeList);
      }
    }
    List<I> otherRemovedEdgeList = other.getRemovedEdgeList();
    if (otherRemovedEdgeList != null) {
      if (removedEdgeList == null) {
        removedEdgeList = new ArrayList<I>(otherRemovedEdgeList);
      } else {
        removedEdgeList.addAll(otherRemovedEdgeList);
      }
    }
  }

}
