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

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.utils.ReflectionUtils;

/**
 * ODPS MapReduce作业的客户端.
 *
 * 提供两个方法用于提交MapReduce作业：<br/>
 * （1）阻塞（同步）方式：{@link #runJob(JobConf)}，提交作业并等待作业结束。<br/>
 * （2）非阻塞（异步）方式：{@link #submitJob(JobConf)}，提交作业立即返回。<br/>
 *
 * @see JobConf
 */
public class JobClient {

  private static final Log LOG = LogFactory.getLog(JobClient.class);

  /**
   * 阻塞（同步）方式提交MapReduce作业并等待作业结束，作业成功则返回作业运行时的对象 {@link RunningJob}.
   *
   * <p>
   * 以下情况发生时抛{@link OdpsException}：
   * <ul>
   * <li>提交作业时异常
   * <li>轮询作业状态异常
   * <li>作业失败，注意：这与{@link #submitJob(JobConf)}异常行为不同
   * </ul>
   * </p>
   *
   * <p>
   * 作业主程序（main函数）需要谨慎处理该异常，因为会影响到console的返回值： <br/>
   * 如果不catch异常，作业失败时会抛出异常，console返回值为非0；如果catch异常且不再向外抛出，即使作业失败，console返回值也为0。<br/>
   * </p>
   *
   * 示例代码：
   *
   * <pre>
   * Job job = new JobConf();
   * ... //config job
   * JobClient.runJob(job); // will throw exception when job failed
   * </pre>
   *
   * @param job
   *     MapReduce作业配置
   * @return MapReduce作业运行时对象
   * @throws OdpsException
   *     作业提交或运行失败时抛异常
   * @see #submitJob(JobConf)
   */
  public static RunningJob runJob(JobConf job) throws OdpsException {
    Long baseTime = new Date().getTime();
    RunningJob rJob = submitJob(job, baseTime);
    rJob.waitForCompletion();
    if (!rJob.isSuccessful()) {
      throw new OdpsException(rJob.getDiagnostics());
    }
    return rJob;
  }

  /**
   * 阻塞（同步）方式提交Pipeline作业并等待作业结束，作业成功则返回作业运行时的对象 {@link RunningJob}.
   *
   * @param job
   *     作业全局配置
   * @param pipeline
   *     pipeline实例
   * @return 作业运行时对象
   * @throws OdpsException
   *     作业提交或运行失败时抛异常
   */
  public static RunningJob runJob(JobConf job, Pipeline pipeline) throws OdpsException {
    Pipeline.toJobConf(job, pipeline);
    return runJob(job);
  }

  /**
   * 非阻塞（异步）方式提交MapReduce作业后立即返回，作业提交成功即返回作业运行时对象 {@link RunningJob}.
   *
   * <p>
   * 只有当提交作业发生异常抛{@link OdpsException}（注意：这与 {@link #runJob(JobConf)} 异常行为不同，
   * {@link #runJob(JobConf)} 在作业失败时会抛异常），所以如果使用 JobClient.submitJob，需要通过返回的 {@link RunningJob}
   * 对象提供的方法判断作业是否运行成功。
   * </p>
   * <p>
   * 取得{@link RunningJob}对象后，可以轮询作业状态，示例代码：
   *
   * <pre>
   * Job job = new JobConf();
   * ... //config job
   * RunningJob rJob = JobClient.submitJob(job);
   * while (!rJob.isComplete()) {
   *   Thread.sleep(4000); // do your work or sleep
   * }
   * if (rJob.isSuccessful()) {
   *   System.out.println(&quot;Job Success!&quot;);
   * } else {
   *   System.err.println(&quot;Job Failed!&quot;);
   * }
   * </pre>
   *
   * 或者直接使用 {@link RunningJob#waitForCompletion()} 轮询作业直至作业结束，示例代码：
   *
   * <pre>
   * Job job = new JobConf();
   * ... //config job
   * RunningJob rJob = JobClient.submitJob(job);
   * rJob.waitForCompletion();
   * if (rJob.isSuccessful()) {
   *   System.out.println(&quot;Job Success!&quot;);
   * } else {
   *   System.err.println(&quot;Job Failed!&quot;);
   * }
   * </pre>
   *
   * </p>
   *
   * @param job
   *     MapReduce作业配置
   * @return MapReduce作业运行时对象
   * @throws OdpsException
   *     作业提交失败时抛异常
   * @see #runJob(JobConf)
   */
  public static RunningJob submitJob(JobConf job) throws OdpsException {
    return submitJob(job, 0L);
  }

  public static RunningJob submitJob(JobConf job, Pipeline pipeline) throws OdpsException {
    Pipeline.toJobConf(job, pipeline);
    return submitJob(job);
  }

  @SuppressWarnings("unchecked")
  private static RunningJob submitJob(JobConf job, Long baseTime) throws OdpsException {
    SessionState ss = SessionState.get();
    String runner = null;
    if (ss.isLocalRun()) {
      runner = "com.aliyun.odps.mapred.LocalJobRunner";
    } else {
      runner = "com.aliyun.odps.mapred.LotBridgeJobRunner";
    }
    JobRunner jobrunner = null;
    try {
      Class<? extends JobRunner> clz = (Class<? extends JobRunner>) Class.forName(runner);
      jobrunner = ReflectionUtils.newInstance(clz, job);
    } catch (ClassNotFoundException e) {
      LOG.fatal("Internal error: corrupted installation.", e);
      throw new RuntimeException(e);
    }
    RunningJob rj = jobrunner.submit();
    System.out.println("InstanceId: " + rj.getInstanceID());
    return rj;
  }

}
