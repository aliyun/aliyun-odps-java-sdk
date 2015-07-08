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

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.graph.JobConf;
import com.aliyun.odps.local.common.JobDirecotry;

public class RuntimeContext {

  private static final String LOCAL_RUN_DIR = "graph_local_jobs";

  private static RuntimeContext INSTANCE = null;

  private String jobId;
  private File jobDir;
  private File inputDir;
  private File outputDir;
  private File resourceDir;
  private File counterDir;
  private File superStepDir;
  private Counters counters = new Counters();

  private RuntimeContext(String jobId, JobConf conf) throws IOException {
    this.jobId = jobId;

    jobDir = new JobDirecotry().getJobDir();
    inputDir = new File(jobDir, "input");
    outputDir = new File(jobDir, "output");
    resourceDir = new File(jobDir, "resource");
    superStepDir = new File(jobDir, "superSteps");
    counterDir = new File(jobDir, "counters");

    inputDir.mkdirs();
    outputDir.mkdirs();
    resourceDir.mkdirs();
    superStepDir.mkdirs();
    counterDir.mkdirs();
  }

  public synchronized static RuntimeContext create(String jobId, JobConf conf)
      throws IOException {
    INSTANCE = new RuntimeContext(jobId, conf);
    return INSTANCE;
  }

  public synchronized static RuntimeContext getInstance() {
    return INSTANCE;
  }

  public Counters getCounters() {
    return counters;
  }

  public String getJobId() {
    return jobId;
  }

  public File getJobDir() {
    return jobDir;
  }

  public File getJobFile() {
    return new File(jobDir, "job.xml");
  }

  public File getInputDir(String tbl, String partSpec) {
    String folder = (StringUtils.isEmpty(partSpec)) ? tbl : tbl + "/"
                                                            + partSpec;
    return new File(inputDir, folder);
  }

  public File getOutputDir(String label) {
    if (label.isEmpty()) {
      label = "__default__";
    }
    return new File(outputDir, label);
  }

  public File getResourceDir() {
    return resourceDir;
  }

  public File getSuperStepDir() {
    return superStepDir;
  }

  public File getCounterDir() {
    return counterDir;
  }

}
