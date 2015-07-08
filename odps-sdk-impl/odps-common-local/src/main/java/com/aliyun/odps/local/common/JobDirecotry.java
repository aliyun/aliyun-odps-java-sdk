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

package com.aliyun.odps.local.common;

import java.io.File;

import com.aliyun.odps.utils.StringUtils;

public class JobDirecotry {

  public File getJobDir() {
    File f = new File(WareHouse.getInstance().getJobDirStr(), WareHouse.getInstance().getJobName());
    if (!f.exists()) {
      f.mkdirs();
    }
    return f;
  }

  public File getJobFile() {
    return new File(getJobDir(), "job.xml");
  }

  public File getInputBaseDir() {
    File f = new File(getJobDir(), "input");
    return f;
  }

  public File getOutputBaseDir() {
    File f = new File(getJobDir(), "output");
    return f;
  }

  public File getInputDir(String relativePath) {
    File f = new File(getJobDir(), "input" + File.separator + relativePath);
    if (!f.exists()) {
      f.mkdirs();
    }
    return f;
  }

  /**
   * 将job临时目录中的输入表数据文件映射为Warehouse中的输入表数据文件
   *
   * 如将~/temp/mr_20150212083126_505_21462/input/project_name/wc_in2/p1=1/p2=2/
   * data 映射为~/warehouse/project_name/wc_in2/p1=1/p2=2/data
   *
   * @param jobInputFile
   * @return
   */
  public File convertInputFile(File jobInputFile) {
    String jobInputFileStr = jobInputFile.getAbsolutePath().replaceAll("\\\\", "/");
    File jobInputBaseDir = new File(getJobDir(), "input");
    String jobInputBaseDirStr = jobInputBaseDir.getAbsolutePath().replaceAll("\\\\", "/");
    if (!jobInputFileStr.startsWith(jobInputBaseDirStr)) {
      return null;
    }
    if (jobInputBaseDirStr.length() == jobInputFileStr.length()) {
      return new File(getJobDir(), "input");
    }
    String relativePath = jobInputFileStr.substring(jobInputBaseDirStr.length());
    if (relativePath.equals("/")) {
      return new File(getJobDir(), "input");
    }
    if (relativePath.startsWith("/")) {
      relativePath = relativePath.substring(1);
    }
    int index = relativePath.indexOf("/");
    // project directory
    if (index == -1) {
      return new File(WareHouse.getInstance().getWarehouseDir(), relativePath);
    }
    String projectName = relativePath.substring(0, index);
    relativePath = relativePath.substring(index);
    return new File(WareHouse.getInstance().getWarehouseDir(), projectName + File.separator
                                                               + Constants.TABLES_DIR
                                                               + File.separator + relativePath);
  }

  public File getOutputDir(String label) {
    StringBuilder sb = new StringBuilder("output" + File.separator);
    if (StringUtils.isNullOrEmpty(label)) {
      sb.append("__default__");
    } else {
      sb.append(label);
    }
    File f = new File(getJobDir(), sb.toString());
    if (!f.exists()) {
      f.mkdirs();
    }
    return f;
  }

  public File getResourceDir() {
    File f = new File(getJobDir(), "resource");
    if (!f.exists()) {
      f.mkdirs();
    }
    return f;
  }

  public boolean hasResource(String resourceName) {
    File resourceRootDir = new File(getJobDir(), "resource");
    if (!resourceRootDir.exists()) {
      resourceRootDir.mkdirs();
    }
    File resourceDir = new File(resourceRootDir, resourceName);
    return resourceDir.exists();
  }
}
