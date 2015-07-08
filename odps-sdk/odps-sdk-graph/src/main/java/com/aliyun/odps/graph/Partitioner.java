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
import com.aliyun.odps.io.WritableComparable;

/**
 * 分区类，决定 {@link Vertex} 由哪个 Worker 负责.
 *
 * 可以通过{@link GraphJob#setPartitionerClass(Class)}方法自定义分区类，否则使用默认的分区类
 * HashPartitioner
 *
 * @param <VERTEX_ID>
 *     Vertex ID 类型
 */
@SuppressWarnings({"rawtypes"})
public abstract class Partitioner<VERTEX_ID extends WritableComparable> {

  /**
   * 此方法会在分区类对象被new出来后立即调用，传入运行时的{@link JobConf}对象
   *
   * @param conf
   *     运行时的JobConf对象
   */
  public void configure(Configuration conf) throws IOException {
  }

  /**
   * 给定 Vertex ID 和 Worker 个数，计算该 Vertex 由哪个 Worker 负责.
   *
   * <p>
   * 此方法通常实现为对 Vertex ID 的某个哈希函数，默认的分区类 HashPartitioner
   * </p>
   *
   * @param vertexId
   *     待分区的 Vertex ID
   * @param numWorkers
   *     分区数，既 Worker 个数
   * @return 分区序号，必须是 0 到 numWorkers-1
   */
  public abstract int getPartition(VERTEX_ID vertexId, int numWorkers);

}
