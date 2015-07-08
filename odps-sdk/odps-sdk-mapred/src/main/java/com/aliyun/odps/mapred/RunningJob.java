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

package com.aliyun.odps.mapred;

import java.io.IOException;

import com.aliyun.odps.counter.Counters;

/**
 * 作业运行时对象，用于跟踪运行中的 MapReduce作业实例.
 *
 * @see JobClient
 */
public interface RunningJob {

  /**
   * 获取作业运行实例ID，用于查看运行日志和作业管理.
   *
   * @return 作业运行实例ID
   */
  public String getInstanceID();

  /**
   * 查询作业是否结束.
   *
   * <p>
   * 结束状态：
   * <ul>
   * <li>{@link JobStatus#SUCCEEDED}
   * <li>{@link JobStatus#FAILED}
   * <li>{@link JobStatus#KILLED}
   * </ul>
   * 如果 {@link #getJobStatus()} 返回状态是上述三种，则返回true
   * </p>
   *
   * @return 如果作业结束，返回true，否则false
   */
  public boolean isComplete();

  /**
   * 查询作业实例是否运行成功.
   *
   * <p>
   * 如果 {@link #getJobStatus()} 返回状态为{@link JobStatus#SUCCEEDED}
   * ，返回true，否则返回false
   * </p>
   *
   * @return 作业实例成功结束，返回true，否则返回false
   */
  public boolean isSuccessful();

  /**
   * 等待直至作业实例结束
   *
   * @throws IOException
   */
  public void waitForCompletion();

  /**
   * 查询作业实例运行状态.
   *
   * <p>
   * 作业状态定义见：{@link JobStatus}
   * </p>
   *
   * @return 作业状态
   * @see JobStatus
   */
  public JobStatus getJobStatus();

  /**
   * Kill 此作业运行实例
   */
  public void killJob();

  /**
   * 获取作业运行实例的 Counters 信息，MapReduce 框架会汇总所有 Mapper/Reducer 任务设置的 Counters.
   *
   * @return 作业运行实例的 {@link Counters} 信息
   */
  public Counters getCounters();

  /**
   * 获取任务的诊断信息.
   *
   * @return 诊断信息
   */
  public String getDiagnostics();

  /**
   * 获取作业的Map进度，通过计算已完成map数/map总数得到，返回值范围在0.0到1.0之间，当所有Mapper完成后，则返回1.0.
   *
   * @return 作业的map进度
   * @throws IOException
   */
  public float mapProgress() throws IOException;

  /**
   * 获取作业的Reduce进度，通过计算已完成reduce数/reduce总数得到，返回值范围在0.0到1.0之间，当所有Reducer完成后，则返回1.0；
   * 对于map-only作业，作业未完成时返回0.0，作业完成后返回1.0.
   *
   * @return 作业的reduce进度
   * @throws IOException
   */
  public float reduceProgress() throws IOException;
}
