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
 * WorkerComputer 允许在 Worker 开始和退出时执行用户自定义的逻辑.
 *
 * <p>
 * 用户可以通过 {@link GraphJob#setWorkerComputerClass(Class)} 提供 WorkerComputer
 * 的实现，运行时可以通过 {@link WorkerContext#getWorkerValue()()} 方法取得 在
 * {@link #setup(WorkerContext, Writable)} 中初始化的 Worker 级共享对象，并在计算 过程中更新，Worker
 * 结束时在 {@link #cleanup(WorkerContext)} 中获取最后的值，可以写出。
 * </p>
 *
 * <p>
 * WorkerValue 用于存储 Worker 的初始化信息，如读取资源，获取图的全局信息，每个 Worker 内 唯一。将这些信息存储在
 * WorkerValue 中，此后在 Worker 的整个生命周期内，能够读取和更新。 在FailOver时，会保存和载入运行时
 * WorkerValue。资源类信息建议每次判断不存在时再读取，不 建议将资源等只读类数据序列化。如果实现该类子类时，不指定泛型参数，默认为
 * {@link NullWritable}。
 * </p>
 *
 * <p>
 * 一个 Worker 会实例化一个 WorkerComputer 对象，其中：
 * <ul>
 * <li>{@link #setup(WorkerContext, Writable)} 方法在 Worker 加载点后，且调用点的 setup
 * 方法之前，会被执行一次，通常用于初始化一些所有点都需要的信息，如读取资源等，可以存储到 Writable 参数中；
 * <li>{@link #cleanup(WorkerContext)} 方法在 Worker 调用所有点的 cleanup 方法之后，会被执行一次，
 * 通常用于写结果数据等等；
 * </ul>
 * </p>
 */
public class WorkerComputer<WorkerValue extends Writable> {

  /**
   * 此方法会第一次迭代开始前调用，通常用于初始化一些所有点都需要的信息，如读取资源等。可以将这些信息 存储在 WorkerValue
   * 中，并在计算过程中通过 {@link WorkerContext#getWorkerValue()} 实时获 取该值并更新，更新后的值在当前
   * Worker 立即生效。
   *
   * <p>
   * 具体调用的时间点是 Worker 加载点后，且调用点的 setup 方法之前，默认无任何操作。如果没有指定泛型 参数，则传入的 workerValue
   * 参数为NullWritable。
   *
   * @param context
   *     当前 Worker 上下文，可以获取全局信息
   * @param workerValue
   *     用于存储初始化值，Worker 生命周期内一直存在
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public void setup(WorkerContext context, WorkerValue workerValue)
      throws IOException {

  }

  /**
   * 此方法会在最后一次迭代结束后调用，通常用于写结果数据等等.
   *
   * <p>
   * 具体调用的时间点是在 Worker 调用所有点的 cleanup 方法之后，Worker 退出前，默认无任何操作。
   *
   * @param context
   *     当前 Worker 上下文，可以获取全局信息及当前 Worker 历史执行信息
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public void cleanup(WorkerContext context) throws IOException {

  }
}
