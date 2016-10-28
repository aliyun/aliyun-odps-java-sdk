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

package com.aliyun.odps.mapred.bridge;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSON;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.StageProgress;
import com.aliyun.odps.Instance.Status;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.util.CostResultParser;
import com.aliyun.odps.counter.CounterGroup;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.mapred.EventListener;
import com.aliyun.odps.mapred.JobStatus;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.udf.utils.CounterUtils;

public class BridgeRunningJob implements RunningJob {

  protected Instance instance;
  protected JobStatus state = JobStatus.PREP;
  protected Counters counters = new Counters();
  protected String diagnostics = "";
  protected boolean stopped = false;

  private long timeMark = 0;
  private boolean hasPrintSummary = false;
  private boolean hasPrintResult = false;

  private float mapProgress = 0;
  private float reduceProgress = 0;

  private final String taskName;
  private EventListener event = null;

  private boolean isCountersOk = true;
  private boolean isCostMode = false;


  public BridgeRunningJob(Instance instance, String taskName, EventListener event) {
    this.instance = instance;
    this.taskName = taskName;
    this.event = event;

    // print logview url to stdout
    startUp();
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
    // Clean
    event.onComplete();
  }

  private void startUp() {
    try {
      String log = SessionState.get().getOdps().logview().generateLogView(instance, 7 * 24);
      System.out.println(log);
    } catch (Exception e) {
      // do nothing if not load logview class
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
    // Clean
    event.onComplete();
  }

  @Override
  public Counters getCounters() {
    if (!isCountersOk) {
      throw new RuntimeException("Get Counters Failed!");
    }
    return counters;
  }

  @Override
  public String getDiagnostics() {
    return diagnostics;
  }


  protected boolean isFinished() {
    return (state == JobStatus.FAILED) || (state == JobStatus.SUCCEEDED) || (state
                                                                             == JobStatus.KILLED);
  }

  protected void updateStatus() {
    try {
      Status instanceStatus = instance.getStatus();
      if (instanceStatus == Status.RUNNING || instanceStatus == Status.SUSPENDED) {
        state = JobStatus.RUNNING;
      } else if (instanceStatus == Status.TERMINATED) {
        TaskStatus taskStatus = instance.getTaskStatus().values().iterator().next();
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
            throw new OdpsException("Got Unknown task status: " + taskStatus.getStatus());
        }
      } else {
        throw new OdpsException("Got unknown instance status '" + instanceStatus + "'");
      }

      // try to print process information
      if (needPrintProcess() || isFinished()) {
        printProgress();
      }
    } catch (OdpsException ex) {
      throw new RuntimeException(ex);
    }

    if (isFinished()) {
      try {
        printSummaryAndCollectCounters();
        printResult();
      } catch (Exception ex) {
        isCountersOk = false;
        System.out.println("Get summary failed.");
      }
    }

  }

  private boolean needPrintProcess() {
    long now = System.currentTimeMillis();
    long interval = 5 * 1000;
    if (now - timeMark > interval) {
      timeMark = now;
      return true;
    }
    return false;
  }

  /**
   * Print job progress.
   *
   * @throws IOException
   * @throws OdpsException
   *     if get task progress failed
   */
  private void printProgress() throws OdpsException {
    PrintStream out = System.out;

    List<StageProgress> stages = null;
    stages = instance.getTaskProgress(taskName);

    if (stages != null && stages.size() != 0) {

      out.print(Instance.getStageProgressFormattedString(stages));

      int mappers = 0;
      int reducers = 0;
      mapProgress = 0;
      reduceProgress = 0;
      for (StageProgress stage : stages) {
        int totalWorkers = stage.getTotalWorkers();
        if (stage.getName().startsWith("M")) {
          mappers += totalWorkers;
          mapProgress += stage.getFinishedPercentage()/100.0 * totalWorkers;
        } else {
          reducers += totalWorkers;
          reduceProgress += stage.getFinishedPercentage()/100.0 * totalWorkers;
        }
      }
      mapProgress = mappers == 0 ? 0 : mapProgress/mappers;
      reduceProgress = reducers == 0 ? 0 : reduceProgress/reducers;
    } else {
      out.print("...");
    }
    out.print('\r');
    out.flush();
  }

  @SuppressWarnings({"rawtypes"})
  private void printSummaryAndCollectCounters() throws IOException {
    if (hasPrintSummary || !isSuccessful()) {
      return;
    }

    TaskSummary taskSummary = null;
    try {
      taskSummary = instance.getTaskSummary(taskName);
    } catch (OdpsException ex) {
      throw new IOException("Get summary encounter error: ", ex);
    }

    if (taskSummary == null) {
      if (System.getProperty("omit.taskstatus.failure", "false").equalsIgnoreCase("true")) {
        System.out.println("No summary in place.");
        return;
      } else {
        throw new IOException("No summary in place.");
      }
    }

    Map tasks = (Map) taskSummary.get("Stages");
    Counters tmpCounters;
    for (Object e : tasks.values()) {
      Map task = (Map) e;
      Map countersValue = (Map) task.get("UserCounters");
      if (countersValue != null) {
        tmpCounters = CounterUtils.createFromJsonString(JSON.toJSONString(countersValue));
        Iterator<CounterGroup> iter = tmpCounters.iterator();
        while (iter.hasNext()) {
          if (iter.next().getName().equals("ODPS_SDK_FRAMEWORK_COUNTER_GROUP")) {
            // ignore framework counter group
            iter.remove();
          }
        }
        counters.incrAllCounters(tmpCounters);
      }
    }
    System.out.println(taskSummary.getSummaryText());
    System.out.println(counters);
    hasPrintSummary = true;
  }

  private void printResult() throws IOException {
    if (!hasPrintResult) {
      try {
        Map<String, String> m = instance.getTaskResults();
        diagnostics = m.values().iterator().next();

        if (state == JobStatus.SUCCEEDED) {
          if (!StringUtils.isEmpty(diagnostics)) {
            if (isCostMode) {
              diagnostics = CostResultParser.parse(diagnostics, "MapReduce");
            }
            System.err.println(diagnostics);
          }
          System.err.println("OK");
        } else {
          if (!StringUtils.isEmpty(diagnostics)) {
            System.err.println("FAILED: " + diagnostics);
          } else {
            System.err.println("ERROR: " + state.toString());
          }
        }

        hasPrintResult = true;
      } catch (Exception ex) {
        throw new IOException("Get result encounter error: " + ex.getMessage());
      }
    }
  }

  @Override
  public float mapProgress() throws IOException {
    if (isSuccessful()) {
      return 1f;
    }
    return this.mapProgress;
  }

  @Override
  public float reduceProgress() throws IOException {
    if (isSuccessful()) {
      return 1f;
    }
    return this.reduceProgress;
  }

  public boolean isCostMode() {
    return isCostMode;
  }

  public void setIsCostMode(boolean isCostMode) {
    this.isCostMode = isCostMode;
  }

}
