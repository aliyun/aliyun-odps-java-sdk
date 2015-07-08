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

import com.aliyun.odps.io.Writable;

/**
 * MasterComputer接口允许 Master在 作业启动时，每轮迭代开始前，作业结束时执行用户代码逻辑。
 * 该接口暂时不对用户开放
 *
 * @param <ComputeValue>
 *     Master上的compute value数据类型
 */
@Deprecated
public class MasterComputer<ComputeValue extends Writable> {

  /**
   * 该方法会在第一次迭代开始前调用
   *
   * @param context
   *     Master的上下文信息，并且可以通过context执行控制job运行停止状态，输出记录的操作
   * @param values
   *     Master上compute value集合迭代器，其中每个compute value对应一个worker上的compute value
   * @throws IOException
   */
  public void setup(MasterContext<ComputeValue> context,
                    Iterable<ComputeValue> values) throws IOException {
  }

  /**
   * 该方法会在每轮迭代开始前调用.
   *
   * @param context
   *     Master的上下文信息，并且可以通过context执行控制job运行停止状态，输出记录的操作
   * @param values
   *     Master上compute value集合迭代器，其中每个compute value对应一个worker上的compute value
   * @throws IOException
   */
  public void compute(MasterContext<ComputeValue> context,
                      Iterable<ComputeValue> values) throws IOException {
  }

  /**
   * 该方法会在最后一次迭代结束后调用.
   *
   * @param context
   *     Master的上下文信息，并且可以通过context执行控制job运行停止状态，输出记录的操作
   * @param values
   *     Master上compute value集合迭代器，其中每个compute value对应一个worker上的compute value
   * @throws IOException
   */
  public void cleanup(MasterContext<ComputeValue> context,
                      Iterable<ComputeValue> values) throws IOException {
  }
}
