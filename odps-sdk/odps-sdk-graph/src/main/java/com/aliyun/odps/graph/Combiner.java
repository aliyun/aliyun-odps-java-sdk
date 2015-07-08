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
import com.aliyun.odps.io.WritableComparable;

/**
 * Combiner 对发送给同一个点的消息进行合并，用于减少网络开销和内存占用.
 *
 * <p>
 * Combiner 会被用于发送端和接收端，发送端可以减少消息发送量，接收端可以减少内存占用.
 *
 * @param <I>
 *     Vertex ID 类型
 * @param <M>
 *     Message 类型
 */
public abstract class Combiner<I extends WritableComparable, M extends Writable> {

  /**
   * 此方法会在 Combiner 对象被new出来后立即调用，传入运行时的{@link Configuration}对象
   *
   * @param conf
   *     运行时的 {@link Configuration} 对象
   */
  public void configure(Configuration conf) throws IOException {
  }

  /**
   * 将发送给同一个点的多条消息聚集成一条消息.
   *
   * @param vertexId
   *     点的标识
   * @param combinedMessage
   *     合并后的消息
   * @param messageToCombine
   *     将要被合并的消息
   * @throws IOException
   */
  public abstract void combine(I vertexId, M combinedMessage, M messageToCombine)
      throws IOException;

}
