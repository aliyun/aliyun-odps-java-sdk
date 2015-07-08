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

package com.aliyun.odps.graph.partitioner;

import com.aliyun.odps.graph.Partitioner;
import com.aliyun.odps.io.WritableComparable;

/**
 * 系统默认的分区类,
 * 通过顶点ID的哈希值对worker数取余的方法，确定顶点分配到哪个worker
 *
 * @param <VERTEX_ID>
 *     Vertex ID 类型
 */
public class HashPartitioner<VERTEX_ID extends WritableComparable<?>> extends
                                                                      Partitioner<VERTEX_ID> {

  @Override
  public int getPartition(VERTEX_ID vertexId, int numPartitions) {
    return Math.abs(vertexId.hashCode() % numPartitions);
  }

}
