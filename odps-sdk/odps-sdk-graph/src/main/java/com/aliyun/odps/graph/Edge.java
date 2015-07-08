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

import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;

/**
 * Edge 表示图的一条边.
 *
 * <p>
 * 在 ODPS Graph 程序中，边总是依附于起点，所以 Edge 只存终点和边值。
 * </p>
 *
 * @param <VERTEX_ID>
 *     点类型
 * @param <EDGE_VALUE>
 *     边值类型
 */
@SuppressWarnings("rawtypes")
public final class Edge<VERTEX_ID extends WritableComparable, EDGE_VALUE extends Writable> {

  private VERTEX_ID destVertexId;
  private EDGE_VALUE value;

  /**
   * 给定终点和边值，构造一条边.
   *
   * @param destVertexId
   *     终点
   * @param value
   *     边值
   */
  public Edge(VERTEX_ID destVertexId, EDGE_VALUE value) {
    super();
    this.destVertexId = destVertexId;
    this.value = value;
  }

  /**
   * 返回边的终点.
   *
   * @return 边的终点
   */
  public VERTEX_ID getDestVertexId() {
    return destVertexId;
  }

  /**
   * 返回边的值
   *
   * @return 边值
   */
  public EDGE_VALUE getValue() {
    return value;
  }

  /**
   * 设置边值
   *
   * @param value
   *     边值
   */
  public void setValue(EDGE_VALUE value) {
    this.value = value;
  }

  /**
   * 边转换成字符串格式.
   * <p>
   * 字符串格式如下：
   * Edge[<目标顶点id>，<边的value>]
   * 注意：
   * 如果顶点id和边value是自定义类型，需要重写其toString()方法
   * </p>
   *
   * @return 边转换的字符串格式
   */
  @Override
  public String toString() {
    return "Edge [" + destVertexId + ", " + value + "]";
  }
}
