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

/**
 * Master 上下文对象接口，封装了框架提供的核心功能.
 * 该接口暂时不对用户开放
 *
 * @param <ComputeValue>
 *     ComputeValue 类型
 */
@Deprecated
public interface MasterContext<ComputeValue extends Writable> {

  /**
   * 获取作业配置
   *
   * @return 作业配置
   */
  public Configuration getConfiguration();

  /**
   * 获取worker数量
   *
   * @return worker数量
   */
  public int getNumWorkers();

  /**
   * 获取当前超步，即第几次迭代，从 0 开始计数.
   *
   * @return 当前超步
   */
  public long getSuperstep();

  /**
   * 获取最大迭代次数
   *
   * @return 最大迭代次数
   */
  public long getMaxIteration();

  /**
   * 获取Master上compute value的对象
   *
   * @return compute value的对象
   */
  public ComputeValue getComputeValue();

  /**
   * 获取每个Aggregator是否停止的迭代器
   *
   * @return 每个Aggregator是否停止的迭代器
   */
  public Iterable<Boolean> getAggregatorTerminated();

  /**
   * 获取作业是否停止
   *
   * @return 作业是否停止
   */
  public boolean isHalted();

  /**
   * 发送命令停止迭代计算
   */
  public void haltComputation();

  /**
   * 获取顶点总数
   *
   * @return 顶点总数
   */
  public long getTotalNumVertices();

  /**
   * 获取边总数
   *
   * @return 边总数
   */
  public long getTotalNumEdges();

  /**
   * 获取当前停止的顶点总数
   *
   * @return 当前停止的顶点总数
   */
  public long getNumHaltedVertices();

  /**
   * 获取当前superstep发送消息总数
   *
   * @return 发送消息总数
   */
  public long getNumMessages();

  /**
   * 写记录到默认输出.
   *
   * @param fieldVals
   *     待写出的记录值
   * @throws IOException
   * @see GraphJob#addOutput(TableInfo)
   */
  public abstract void write(Writable... fieldVals) throws IOException;

  /**
   * 写记录到给定标签输出.
   *
   * @param label
   *     输出标签
   * @param fieldVals
   *     待写出的记录值
   * @throws IOException
   * @see GraphJob#addOutput(TableInfo, String)
   */
  public abstract void write(String label, Writable... fieldVals)
      throws IOException;
}
