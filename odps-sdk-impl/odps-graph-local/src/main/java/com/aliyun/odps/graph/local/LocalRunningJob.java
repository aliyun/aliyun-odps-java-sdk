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

package com.aliyun.odps.graph.local;

import java.io.IOException;

import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.mapred.JobStatus;
import com.aliyun.odps.mapred.RunningJob;


public class LocalRunningJob implements RunningJob {

  protected String jobId;
  protected JobStatus state = JobStatus.PREP;
  protected Counters counters = null;
  protected boolean stopped = false;

  public LocalRunningJob(String jobId, JobStatus state, Counters counters) {
    this.jobId = jobId;
    this.state = state;
    this.counters = counters;
  }

  @Override
  public String getInstanceID() {
    return jobId;
  }


  @Override
  public boolean isComplete() {
    return isFinished();
  }

  @Override
  public boolean isSuccessful() {
    return (state == JobStatus.SUCCEEDED);
  }

  @Override
  public void waitForCompletion() {
  }

  @Override
  public JobStatus getJobStatus() {
    return state;
  }

  @Override
  public void killJob() {
    stopped = true;
  }

  @Override
  public Counters getCounters() {
    return counters;
  }

  protected boolean isFinished() {
    return (state == JobStatus.FAILED) || (state == JobStatus.SUCCEEDED)
           || (state == JobStatus.KILLED);
  }

  @Override
  public String getDiagnostics() {
    return null;
  }

  @Override
  public float mapProgress() throws IOException {
    return 0;
  }

  @Override
  public float reduceProgress() throws IOException {
    return 0;
  }
}
