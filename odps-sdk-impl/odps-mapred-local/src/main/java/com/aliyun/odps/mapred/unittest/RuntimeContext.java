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

package com.aliyun.odps.mapred.unittest;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.mapred.conf.JobConf;

public class RuntimeContext {

  private static final String LOCAL_RUN_DIR = "mr_ut_local_jobs";

  private static WareHouse wareHouse = WareHouse.getInstance();

  private static RuntimeContext INSTANCE = null;

  private String jobId;

  private RuntimeContext(String jobId, JobConf jobconf) throws IOException {
    this.jobId = jobId;
    wareHouse.setConfiguration(jobconf);
    wareHouse.getConfiguration().set(Constants.LOCAL_TEMP_DIR, LOCAL_RUN_DIR);
    wareHouse.setJobName(jobId);
  }

  public synchronized static RuntimeContext create(String jobId, JobConf jobconf)
      throws IOException {
    INSTANCE = new RuntimeContext(jobId, jobconf);
    return INSTANCE;
  }

  public static WareHouse getWareHouse() {
    return wareHouse;
  }

  public boolean containsKey(String key) {
    return wareHouse.caintainsKey(key);
  }

  public String getJobId() {
    return jobId;
  }

  public File getJobDir() {
    return wareHouse.getJobDir().getJobDir();
  }

  public File getJobFile() {
    return wareHouse.getJobDir().getJobFile();
  }

  public File getInputDir(String tbl, String partSpec) {
    String folder = (StringUtils.isEmpty(partSpec)) ? tbl : tbl + "/"
        + partSpec;
    return wareHouse.getJobDir().getInputDir(folder);
  }

  public File getOutputDir(String label) {
    return wareHouse.getJobDir().getOutputDir(label);
  }

  public File getResourceDir() {
    return wareHouse.getJobDir().getResourceDir();
  }

}
