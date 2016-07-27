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
import java.nio.charset.Charset;

import com.aliyun.odps.utils.StringUtils;

import com.aliyun.odps.utils.CommonUtils;


public class LocalRunContext {

  private static LocalRunContext instance = null;

  private int recordLimit = 100;
  private int maxMapTasks = 100;
  private int maxReduceTasks = 1;
  private Charset encoding = null;
  private File wareHouse = new File("./warehouse");
  private boolean cleanJobDir = false;
  private boolean cleanUtDir = true;

  private LocalRunContext() {
    String s = System.getProperty("odps.mapred.local.record.limit");
    if (!StringUtils.isEmpty(s)) {
      recordLimit = Integer.parseInt(s);
      CommonUtils.checkArgument("'input record limit'", recordLimit, 0, 10000);
    }
    s = System.getProperty("odps.mapred.local.map.max.tasks");
    if (!StringUtils.isEmpty(s)) {
      maxMapTasks = Integer.parseInt(s);
      CommonUtils.checkArgument("'max map tasks'", maxMapTasks, 0, 100);
    }
    s = System.getProperty("odps.mapred.local.reduce.max.tasks");
    if (!StringUtils.isEmpty(s)) {
      maxReduceTasks = Integer.parseInt(s);
      CommonUtils.checkArgument("'max reduce tasks'", maxReduceTasks, 0, 100);
    }
    s = System.getProperty("odps.mapred.local.job.clean");
    if (!StringUtils.isEmpty(s)) {
      cleanJobDir = Boolean.parseBoolean(s);
    }
    s = System.getProperty("odps.mapred.ut.clean");
    if (!StringUtils.isEmpty(s)) {
      cleanUtDir = Boolean.parseBoolean(s);
    }
    s = System.getProperty("odps.mapred.local.file.encoding");
    if (!StringUtils.isEmpty(s)) {
      encoding = Charset.forName(s);
    } else {
      encoding = Charset.forName("UTF-8");
    }
    s = System.getProperty("odps.mapred.local.warehouse");
    if (!StringUtils.isEmpty(s)) {
      wareHouse = new File(s);
    }

    wareHouse.mkdirs();
  }

  public synchronized static LocalRunContext getInstance() {
    if (instance == null) {
      instance = new LocalRunContext();
    }
    return instance;
  }

  public int getRecordLimit() {
    return recordLimit;
  }

  public int getMaxMapTasks() {
    return maxMapTasks;
  }

  public int getMaxReduceTasks() {
    return maxReduceTasks;
  }

  public boolean isCleanJobDir() {
    return cleanJobDir;
  }

  public boolean isCleanUtDir() {
    return cleanUtDir;
  }

  public Charset getEncoding() {
    return encoding;
  }

  public File getWareHouse() {
    return wareHouse;
  }
}
