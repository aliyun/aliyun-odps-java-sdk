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

package com.aliyun.odps.graph.job;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.Status;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.graph.utils.LogUtils;
import com.aliyun.odps.mapred.JobStatus;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.utils.StringUtils;

/**
 * 提交到ODPS服务端的运行作业实例
 */
public class NetworkRunningJob implements RunningJob {

  public static final SimpleDateFormat DATEFORMAT = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss");
  protected Task task;
  protected Instance instance;
  protected JobStatus state = JobStatus.PREP;
  protected Counters counters = new Counters();
  protected boolean stopped = false;

  private long timeMark = 0;
  private boolean hasPrintSummary = false;
  private boolean hasPrintResult = false;
  protected String diagnostics = "";

  public NetworkRunningJob(Task task, Instance instance) {
    this.task = task;
    this.instance = instance;
  }

  @Override
  public String getInstanceID() {
    return instance.getId();
  }

  @Override
  public boolean isComplete() {
    if (isFinished()) {
      return true;
    }
    updateStatus();
    return isFinished();
  }

  @Override
  public boolean isSuccessful() {
    if (isFinished()) {
      return (state == JobStatus.SUCCEEDED);
    }
    updateStatus();
    return (state == JobStatus.SUCCEEDED);
  }

  @Override
  public void waitForCompletion() {
    int sleeptime = 1000;
    while (!isComplete()) {
      try {
        Thread.sleep(sleeptime);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      sleeptime = ((sleeptime + 500) > 4000) ? 4000 : sleeptime + 500;
    }
  }

  @Override
  public JobStatus getJobStatus() {
    if (isFinished()) {
      return state;
    }
    updateStatus();
    return state;
  }

  @Override
  public void killJob() {
    stopped = true;
    try {
      instance.stop();
    } catch (OdpsException ex) {
      throw new RuntimeException("Kill job Failed", ex);
    }
  }

  @Override
  public Counters getCounters() {
    return counters;
  }

  @Override
  public String getDiagnostics() {
    return diagnostics;
  }

  protected boolean isFinished() {
    return (state == JobStatus.FAILED) || (state == JobStatus.SUCCEEDED)
           || (state == JobStatus.KILLED);
  }

  protected void updateStatus() {
    int maxRetryTimes = 3;
    int retry_times = 0;
    while (true) {
      try {
        Status instanceStatus = instance.getStatus();
        if (instanceStatus == Status.RUNNING
            || instanceStatus == Status.SUSPENDED) {
          state = JobStatus.RUNNING;
        } else if (instanceStatus == Status.TERMINATED) {
          TaskStatus taskStatus = instance.getTaskStatus().get(task.getName());
          switch (taskStatus.getStatus()) {
            case WAITING:
            case RUNNING:
            case FAILED:
              state = JobStatus.FAILED;
              break;
            case SUCCESS:
              state = JobStatus.SUCCEEDED;
              break;
            case CANCELLED:
              state = JobStatus.KILLED;
              break;

            default:
              throw new OdpsException("Got Unknown task status: "
                                      + taskStatus.getStatus());
          }
        } else {
          throw new OdpsException("Got unknown instance status '"
                                  + instanceStatus + "'");
        }

        // try to print process information
        if (needPrintProcess() || isFinished()) {
          printProgress(instanceStatus);
        }

        break; // success
      } catch (Exception ex) {
        retry_times++;
        if (retry_times > maxRetryTimes) {
          throw new RuntimeException(ex);
        }
        System.err.println("Update status failed, retry counter: "
                           + retry_times + ", Exception: " + ex.getMessage());
        try {
          Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {

        }
      }
    }

    if (isFinished()) {
      maxRetryTimes = 3;
      retry_times = 0;
      while (true) {
        try {
          printSummaryAndCollectCounters();
          printResult();
          break; // success
        } catch (Exception ex) {
          retry_times++;
          if (retry_times > maxRetryTimes) {
            throw new RuntimeException(ex);
          }
          System.err.println("Get summary or result failed, retry counter: "
                             + retry_times + ", Exception: " + ex.getMessage());
          try {
            Thread.sleep(30 * 1000);
          } catch (InterruptedException e) {
          }
        }
      }
    }

  }

  private boolean needPrintProcess() {
    long now = System.currentTimeMillis();
    long interval = 30 * 1000;
    if (now - timeMark > interval) {
      timeMark = now;
      return true;
    }
    return false;
  }

  private void printProgress(Status instanceStatus) throws OdpsException {
    try {
      System.out.println(DATEFORMAT.format(new Date()) + " " + instanceStatus.name()
                         + "\t" + LogUtils
          .assembleProgress(instance.getTaskSummary(task.getName())));
    } catch (Exception e) {
      throw new OdpsException("printProgress enconter error: "
                              + StringUtils.stringifyException(e));
    }
  }

  private void printSummaryAndCollectCounters() throws IOException {
    if (!hasPrintSummary && isSuccessful()) {
      try {
        TaskSummary taskSummary = instance.getTaskSummary(task.getName());
        LogUtils.fillCountersAndShowSummary(taskSummary, counters);
        hasPrintSummary = true;
      } catch (Exception ex) {
        throw new IOException("Get summary encounter error: " + ex.getMessage());
      }
    }
  }

  private void printResult() throws IOException {
    if (!hasPrintResult) {
      try {
        Map<String, String> m = instance.getTaskResults();
        String result = m.get(task.getName());

        if (state == JobStatus.SUCCEEDED) {
          if (!StringUtils.isNullOrEmpty(result)) {
            System.err.println(result);
          }
          System.err.println("OK");
        } else {
          System.err.println("FAILED: " + result);
        }

        hasPrintResult = true;
      } catch (Exception ex) {
        throw new IOException("Get result encounter error: " + ex.getMessage());
      }
    }
  }

  @Override
  public float mapProgress() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public float reduceProgress() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }
}
