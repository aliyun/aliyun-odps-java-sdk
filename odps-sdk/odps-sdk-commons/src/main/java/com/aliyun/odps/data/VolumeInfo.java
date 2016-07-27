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

package com.aliyun.odps.data;


/**
 * Volume信息，用于定义 ODPS M/R 作业要读取或者生成的Volume.
 * author junzheng.zjz
 */

public class VolumeInfo {

  public static final String DEFAULT_LABEL = "__default__";

  private String projectName;

  private String volumeName;

  private String partSpec;

  private String label;
  
  public VolumeInfo() {
      
  }

  public VolumeInfo(String volumeName, String partSpec) {
    this(null, volumeName, partSpec, DEFAULT_LABEL);
  }

  public VolumeInfo(String volumeName, String partSpec, String label) {
    this(null, volumeName, partSpec, label);
  }

  public VolumeInfo(String projectName, String volumeName, String partSpec, String label) {
    this.projectName = projectName;
    this.volumeName = volumeName;
    this.partSpec = partSpec;
    this.label = label;
  }

  public VolumeInfo(VolumeInfo volume) {
    this(volume.projectName, volume.volumeName, volume.partSpec, volume.label);
  }

  public String getProjectName() {
    return this.projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  public String getVolumeName() {
    return this.volumeName;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public String getPartSpec() {
    return this.partSpec;
  }

  public void setPartSpec(String partSpec) {
    this.partSpec = partSpec;
  }

  public String getLabel() {
    return this.label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that instanceof VolumeInfo) {
      VolumeInfo t = (VolumeInfo) that;
      return this.projectName.equals(t.projectName)
             && this.volumeName.equals(t.volumeName)
             && this.partSpec.equals(t.partSpec);
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (projectName != null && !projectName.trim().isEmpty()) {
      sb.append(projectName).append('.');
    }
    sb.append(volumeName).append('.').append(partSpec);
    return sb.toString();
  }
}
