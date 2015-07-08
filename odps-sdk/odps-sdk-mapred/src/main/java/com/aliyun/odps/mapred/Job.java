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

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.pipeline.Pipeline;

/**
 * 作业提交和跟踪，可以通过 Job 定义、提交、控制、查询作业实例.
 *
 * @see JobClient
 */
public class Job extends JobConf {

  private RunningJob info;

  /**
   * 默认构造函数
   */
  public Job() {
    this(new JobConf());
  }

  /**
   * 构造函数，给定一个 {@link Configuration} 对象
   *
   * @param conf
   *     配置管理器
   */
  public Job(Configuration conf) {
    super(conf);
  }

  /**
   * 查询作业是否结束.
   *
   * @return 作业结束返回true，否则返回false
   * @see RunningJob#isComplete()
   */
  public boolean isComplete() {
    if (info == null) {
      return false;
    }
    return info.isComplete();
  }

  /**
   * 查询作业实例是否运行成功.
   *
   * @return 作业成功反馈true，否则返回false
   * @see RunningJob#isSuccessful()
   */
  public boolean isSuccessful() {
    if (info == null) {
      return false;
    }
    return info.isSuccessful();
  }

  /**
   * Kill 此作业运行实例
   *
   * @see RunningJob#killJob()
   */
  public void killJob() {
    if (info == null) {
      return;
    }
    info.killJob();
  }

  /**
   * 获取当前作业实例的 Counters.
   *
   * @return Counters，或null如果任务没有完成。
   */
  public Counters getCounters() {
    if (info == null) {
      return null;
    }
    return new Counters(info.getCounters());
  }

  /**
   * 非阻塞（异步）方式提交作业后立即返回，作业提交失败时抛{@link OdpsException}异常，类似于
   * {@link JobClient#submitJob(JobConf)}.
   *
   * <p>
   * 代码示例如下：
   *
   * <pre>
   * Job job = new Job();
   * ... //config job
   * job.submit();
   * while (!job.isComplete()) {
   *   Thread.sleep(2000);
   * }
   * if (!job.isSuccessful()) {
   *   throw new Exception("Job failed!");
   * }
   * </pre>
   *
   * </p>
   *
   * @throws OdpsException
   */
  public void submit() throws OdpsException {
    info = JobClient.submitJob(this);
  }

  /**
   * 阻塞（同步）方式提交作业并等待作业结束，类似于 {@link JobClient#runJob(JobConf)}.
   *
   * <p>
   * 如果未调用{@link #submit()}，会先调用，然后轮询作业直至作业结束。<br/>
   * 该方法的返回值指示作业是否运行成功，作业主程序（main函数）需要对返回值进行判断决定程序是否返回非0值，从而影响console的返回值。 <br/>
   *
   * @throws OdpsException
   */
  public boolean run() throws OdpsException {
    return waitForCompletion();
  }

  /**
   * 阻塞（同步）方式提交作业并等待作业结束，类似于 {@link JobClient#runJob(JobConf)}.
   *
   * <p>
   * 如果未调用{@link #submit()}，会先调用，然后轮询作业直至作业结束。<br/>
   * 该方法的返回值指示作业是否运行成功，作业主程序（main函数）需要对返回值进行判断决定程序是否返回非0值，从而影响console的返回值。 <br/>
   *
   * 代码示例如下：
   *
   * <pre>
   * Job job = new Job();
   * ... //config job
   * boolean success = job.waitForCompletion(true);
   * if (!success) {
   *   throw new Exception(&quot;Job failed!&quot;);
   * }
   * </pre>
   *
   * </p>
   *
   * @return 如果作业成功，返回true，否则返回false
   * @throws OdpsException
   *     作业提交或者轮询作业状态失败，抛异常
   */
  public boolean waitForCompletion() throws OdpsException {
    if (info == null) {
      submit();
    }
    info.waitForCompletion();
    return isSuccessful();
  }

  /**
   * 获取任务的instance ID
   *
   * @return instance ID，或null如果任务没有被提交
   */
  public String getInstanceID() {
    if (info == null) {
      return null;
    }
    return info.getInstanceID();
  }

  /**
   * 获取任务的状态
   *
   * @return 任务状态
   */
  public JobStatus getJobStatus() {
    if (info == null) {
      return JobStatus.PREP;
    }
    return info.getJobStatus();
  }

  /**
   * 获取任务的诊断信息
   *
   * @return 任务的诊断信息
   */
  public String getDiagnostics() {
    if (info == null) {
      return "";
    }
    return info.getDiagnostics();
  }

  /**
   * 获取任务的map执行进度
   *
   * @return
   * @throws IOException
   */
  public float mapProgress() throws IOException {
    if (info == null) {
      return 0f;
    }
    return info.mapProgress();
  }

  /**
   * 获取任务的reduce执行进度
   *
   * @return
   * @throws IOException
   */
  public float reduceProgress() throws IOException {
    if (info == null) {
      return 0f;
    }
    return info.reduceProgress();
  }

  /**
   * 增加作业输入
   *
   * <p>
   * 作业运行过程中，框架会读取输入表的数据为一条条 {@link Record}，传给 {@link Mapper} 进行处理。
   * </p>
   * <p>
   * </p>
   *
   * <p>
   * <b>示例：</b>
   *
   * <pre>
   * Job job = new Job();
   *
   * job.addInput(TableInfo.builder().tableName(tblName).build());
   * </pre>
   *
   * </p>
   * <p>
   * <b>限制：</b>
   * <ul>
   * <li>输入表或分区要求已经存在，且对指定列具有读权限
   * <li>调用一次 <i>addInput</i> 视为一路输入，ODPS MapReduce
   * 单个作业的输入路数不能超过1024，且表的数量不能超过64，注意这里并非限制最多读 1024 分区
   * <li>不支持通配符表名或分区范围查询的方式指定作业输入
   * </ul>
   * </p>
   *
   * @param tbl
   *     输入表信息
   * @param cols
   *     指定读取的列
   * @see GraphLoader
   */
  public void addInput(TableInfo table) {
    InputUtils.addTable(table, this);
  }

  /**
   * 增加默认作业输出.
   *
   * @param tbl
   *     输出表信息
   * @throws IOException
   */
  public void addOutput(TableInfo table) {
    OutputUtils.addTable(table, this);
  }

  /**
   * 设置以Pipeline模式运行MapReduce。
   * <br/>
   * <p>
   * <b>注意</b>：Pipeline模式下，job.setMapper/ReducerClass设置皆无效
   * </p>
   *
   * @param pipeline
   */
  public void setPipeline(Pipeline pipeline) {
    Pipeline.toJobConf(this, pipeline);
  }

}
