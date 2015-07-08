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
import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;

/**
 * Vertex 表示图的一个点，封装了 ODPS Graph 作业的主要计算逻辑，图作业都需要提供 Vertex 实现类.
 *
 * <p>
 * 一个点包括：
 * <ul>
 * <li>标识 id，自定义，继承自 {@link WritableComparable} ，通常要求全局唯一，{@link Partitioner} 根据
 * id 决定由哪个 Worker 负责；
 * <li>值 value，自定义，继承自 {@link Writable}；
 * <li>状态 halt，布尔值，表示该点是否结束；
 * <li>边集 edges，以该点为起始点的所有边列表；
 * </ul>
 * 在迭代执行过程中，ODPS Graph 框架会按一定频率做 checkpoint，上述内容将被持久化并在 failover
 * 时被恢复，除此之外的其他信息，用户需要在 {@linkplain #setup(WorkerContext) setup} 函数中进行初始化。
 * </p>
 *
 * <p>
 * Vertex 是抽象类，通常继承实现下面的方法：
 * <ul>
 * <li>{@link #setup(WorkerContext) setup} 默认实现为空逻辑，在迭代开始时会被调用，通常在此方法里对点进行初始化；
 * <li>{@link #compute(ComputeContext, Iterable) compute}
 * 抽象方法，必须实现，在每轮迭代中，如果点为非结束状态或者收到上一轮迭代发送的消息，则此方法会被调用；
 * <li>{@linkplain #cleanup(WorkerContext) cleanup} 默认实现为空逻辑，在迭代结束后会被调用；
 * </ul>
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
 * @see GraphLoader
 * @see Aggregator
 * @see WorkerContext
 */
@SuppressWarnings("rawtypes")
public abstract class Vertex<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable> {

  private I id;
  private V value;
  private boolean halt;
  private List<Edge<I, E>> edges = null;

  /**
   * 设置 Vertex ID，要求继承自 {@link WritableComparable}.
   * <p>
   * 在
   * {@linkplain GraphLoader#load(com.aliyun.odps.io.LongWritable, com.aliyun.odps.Record, Vertex)}
   * 中可以调用此方法为点设置 ID
   * </p>
   *
   * @param id
   *     Vertex ID
   */
  public final void setId(I id) {
    this.id = id;
  }

  /**
   * 获取 Vertex ID.
   *
   * @return 返回 Vertex ID
   */
  public final I getId() {
    return id;
  }

  /**
   * 获取点的取值，自定义，继承自 {@link Writable};
   *
   * @return 点的取值
   */
  public final V getValue() {
    return value;
  }

  /**
   * 设置点的取值，自定义，继承自 {@link Writable};
   *
   * <p>
   * 在
   * {@linkplain GraphLoader#load(LongWritable recordNum, WritableRecord record,
   * MutationContext<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE, MESSAGE> context)}
   * 中可以调用此方法为点设置值
   * </p>
   *
   * @param value
   *     点的取值
   */
  public final void setValue(V value) {
    this.value = value;
  }

  /**
   * 设置点为结束状态.
   *
   * <p>
   * 本方法通常在 {@link #compute(ComputeContext, Iterable)}
   * 中调用，如果在一轮迭代中调用此方法将点设置为结束状态后：
   * <ul>
   * <li>如果该点收到消息，则下一轮迭代时框架会调用 {@link #wakeUp()} 自动唤醒该点，并调用该点的
   * {@link #compute(ComputeContext, Iterable)} 方法；
   * <li>如果该点未收到消息，则下一轮迭代时略过调用此点的 {@link #compute(ComputeContext, Iterable)}
   * 方法，直到后续被唤醒；
   * <ul>
   * 在一轮迭代后，如果所有点都处于结束状态，并且没有产生任何消息，则迭代终止。
   * </p>
   */
  public void voteToHalt() {
    this.halt = true;
  }

  /**
   * 唤醒此点，通常由框架调用.
   *
   * <p>
   * 如果需要唤醒某点，只需要给点发送消息，在下一轮迭代时，收到消息的点会被自动唤醒。
   * </p>
   */
  public void wakeUp() {
    halt = false;
  }

  /**
   * 查询此点是否处于结束状态.
   *
   * @return 若为结束状态，返回true，否则false
   */
  public boolean isHalted() {
    return halt;
  }

  /**
   * 查询此点是否有出边.
   *
   * @return 若边数大于0, 返回true，否则false
   */
  public final boolean hasEdges() {
    return edges != null && !edges.isEmpty();
  }

  /**
   * 获取以此点为起始的边集，调用该方法前，建议调用{@link #hasEdges()} 确定此点是否存在出边.
   *
   * @return 以此点为起始的边集
   */
  public final List<Edge<I, E>> getEdges() {
    return edges;
  }

  /**
   * 获取以此点为起始的边的个数。
   *
   * @return 以此点为起始的边的个数
   */
  public final int getNumEdges() {
    return hasEdges() ? getEdges().size() : 0;
  }

  /**
   * 设置以此点为起始的边集，默认为null.
   *
   * @param edges
   *     以此点为起始的边集
   */
  public final void setEdges(List<Edge<I, E>> edges) {
    this.edges = edges;
  }

  /**
   * 增加一条边.
   *
   * @param destVertexId
   *     边终点 ID
   * @param edgeValue
   *     边值
   */
  @SuppressWarnings("unchecked")
  public final void addEdge(I destVertexId, E edgeValue) {
    if (edges == null) {
      edges = new ArrayList<Edge<I, E>>(1);
    }
    edges.add(new Edge(destVertexId, edgeValue));
  }

  /**
   * 给定终点，删除对应边，如果有多个相同终点的重复边，也一并会被删除.
   *
   * @param destVertexId
   *     边的终点 ID
   * @return 成功删除的边集合，若没有找到对应的边，返回空集合
   */
  public final List<Edge<I, E>> removeEdges(I destVertexId) {
    List<Edge<I, E>> removeEdges = new ArrayList<Edge<I, E>>();
    if (edges != null) {
      for (Edge<I, E> edge : edges) {
        if (edge.getDestVertexId().equals(destVertexId)) {
          removeEdges.add(edge);
        }
      }
      edges.removeAll(removeEdges);
    }
    return removeEdges;
  }

  /**
   * 初始化函数，默认实现为空逻辑，在所有迭代开始前调用.
   *
   * <p>
   * 通常在此方法里对点进行初始化，如局部变量。
   *
   * @param context
   *     上下文信息
   * @throws IOException
   */
  public void setup(WorkerContext<I, V, E, M> context) throws IOException {
  }

  /**
   * 计算函数，必须实现此函数.
   *
   * <p>
   * 在每轮迭代中，如果点为非结束状态或者收到上一轮迭代发送的消息，则此方法会被调用，通常在此方法中：
   * <ul>
   * <li>处理上一次迭代发给当前点的消息；
   * <li>根据需要对图进行编辑：1）修改点/边的取值；2）发送消息给某些点；3）增加/删除点或边；
   * <li>通过 {@link #aggregate(Object)} 或 {@link #aggregate(int, Object)}
   * 汇总信息到全局信息；
   * <li>设置当前点状态，结束或非结束状态；
   * </ul>
   * <br/>
   * 迭代进行过程中，框架会将消息以异步的方式发送到对应 Worker 并在下一轮迭代时进行处理，用户无需关心。
   * </p>
   *
   * @param context
   *     上下文信息
   * @param messages
   *     上一次迭代发给当前点的消息
   * @throws IOException
   */
  public abstract void compute(ComputeContext<I, V, E, M> context,
                               Iterable<M> messages) throws IOException;

  /**
   * 清除函数，默认实现为空逻辑，在所有迭代结束后调用.
   *
   * <p>
   * 通常在此方法里把点的计算结果写到结果表中。
   *
   * @param context
   *     上下文信息
   * @throws IOException
   */
  public void cleanup(WorkerContext<I, V, E, M> context) throws IOException {
  }

  /**
   * 顶点转换成字符串格式.
   * <p>
   * 字符串格式如下：
   * Vertex（id=<顶点id>，value=<顶点value>，#edgesNum=<顶点出边数量>）
   * 注意：
   * 如果顶点id和顶点value是自定义类型，需要重写其toString()方法
   * </p>
   *
   * @return 顶点转换的字符串格式
   */
  @Override
  public String toString() {
    return "Vertex(id=" + getId() + ",value=" + getValue() + ",#edgesNum="
           + getNumEdges() + ")";
  }

}
