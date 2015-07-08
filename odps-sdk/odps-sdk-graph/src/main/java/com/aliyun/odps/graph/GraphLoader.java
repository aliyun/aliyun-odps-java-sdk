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
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;
import com.aliyun.odps.io.WritableRecord;

/**
 * GraphLoader 用于载入图，将 ODPS 表的记录解析为图的点或边信息.
 *
 * <p>
 * 通过 {@link GraphJob} 的 {@linkplain GraphJob#setGraphLoaderClass(Class)
 * setGraphLoaderClass} 方法提供自定义的 GraphLoader 实现。
 * </p>
 *
 * @param <VERTEX_ID>
 *     Vertex ID 类型
 * @param <VERTEX_VALUE>
 *     Vertex Value 类型
 * @param <EDGE_VALUE>
 *     Edge Value 类型
 * @param <MESSAGE>
 *     Message 类型
 * @see GraphJob#addInput(TableInfo)
 * @see GraphJob#addInput(TableInfo, String[])
 * @see MutationContext
 */
@SuppressWarnings("rawtypes")
public abstract class GraphLoader<VERTEX_ID extends WritableComparable, VERTEX_VALUE extends Writable, EDGE_VALUE extends Writable, MESSAGE extends Writable> {

  /**
   * 此方法会在 GraphLoader 对象被new出来后立即调用，传入运行时的{@link Configuration},
   * {@link TableInfo}等对象
   * 
   * @param conf
   *          运行时的 Configuration 对象
   * @param workerId
   *          所在 Worker 的ID，从0计数
   * @param tableInfo
   *          所在 Worker 的输入表信息
   * @throws IOException
   * 
   *           建议使用
   *           {@link #setup(Configuration, int, TableInfo, MutationContext)} 替代
   */
  @Deprecated
  public void setup(Configuration conf, int workerId, TableInfo tableInfo)
      throws IOException {
  }
  
  /**
   * 此方法会在 GraphLoader 对象被new出来后立即调用，传入运行时的{@link Configuration},
   * {@link TableInfo}, {@link MutationContext}等对象
   * 
   * @param conf
   *          运行时的 Configuration 对象
   * @param workerId
   *          所在 Worker 的ID，从0计数
   * @param tableInfo
   *          所在 Worker 的输入表信息
   * @param context
   *          图拓扑上下文，存解析结果
   * @throws IOException
   */
  public void setup(Configuration conf, int workerId, TableInfo tableInfo,
      MutationContext<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE, MESSAGE> context)
      throws IOException {
    this.setup(conf, workerId, tableInfo);
  }

  /**
   * 本方法用于将 ODPS 的输入记录解析为图的点或边信息.
   *
   * <p>
   * 框架会将 {@link GraphJob#addInput(TableInfo)}。
   * </p>
   *
   * <p>
   * 从输入数据构造图中的点或者边，根据{@link Record}中的数据，使用{@link MutationContext}的接口，
   * 向图中添加/删除、点/边，适用于{@link Record}包含点或者边数据的输入类型。通过将输入表的一条记录
   * 解析为图的一个点（包括以该点为起点的边）或者边，将图载入到计算框架中。
   * </p>
   * <p>
   * 注意：
   * <ul>
   * <li>此处添加/删除的点/边，都会经过载入阶段的{@link VertexResolver}解决冲突，最终参与计算
   * <li>{@link MutationContext}添加/删除的点/边，用户应保证对象不被复用，包括对象的成员变量
   * <li>添加/删除的对象如果有默认值，最好在{@link VertexResolver#resolve}再赋值
   * </ul>
   *
   * <pre>
   * 正确的用法(调用MutationContext接口需保证不复用，recordNum和record可以直接使用，无需拷贝)：
   * public abstract void load(LongWritable recordNum, Record record,
   *    MutationContext<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE, MESSAGE> context) throws IOException {
   *    // 每次重新创建新的Vertex对象
   *    MyVertex vertex = new MyVertex();
   *    LongWritable id = (LongWritable)record.get(0)；
   *    // 框架保证不复用record，可以直接使用record内部的列，无需拷贝
   *    vertex.setId(id);
   *    vertex.setValue((LongWritable)record.get(1));
   *    vertex.addEdge((LongWritable)record.get(2), (LongWritable)record.get(3));
   *    context.addVertexRequest(vertex);
   *
   *    // 应保证Edge也是不被复用的
   *    context.addEdgeRequest(id, new Edge<LongWritable, LongWritable>(
   *      (LongWritable)record.get(4), (LongWritable)record.get(5)));
   * }
   *
   * 错误的用法(Vertex对象被复用，导致添加的所有点的ID相同)：
   * MyVertex vertex = new MyVertex();
   * public abstract void load(LongWritable recordNum, Record record,
   *    MutationContext<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE, MESSAGE> context) throws IOException {
   *    LongWritable id = (LongWritable)record.get(0)；
   *    // 修改了上次添加的Vertex对象的ID
   *    vertex.setId(id);
   *    vertex.setValue((LongWritable)record.get(1));
   *    vertex.addEdge((LongWritable)record.get(2), (LongWritable)record.get(3));
   *    context.addVertexRequest(vertex);
   * }
   * </pre>
   *
   * @param recordNum
   *     待处理记录的序号，从1开始计数
   * @param record
   *     待处理记录
   * @param context
   *     图拓扑上下文，存解析结果
   * @throws IOException
   */
  public abstract void load(LongWritable recordNum, WritableRecord record,
                            MutationContext<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE, MESSAGE> context)
      throws IOException;
}
