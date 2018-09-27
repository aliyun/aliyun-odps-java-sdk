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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Properties;

import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.volume.FileSystem;

public class MockExecutionContext extends ExecutionContext {

  public MockExecutionContext() {
    workerID = 0;
    stageID = "bar_foo";
  }

  public void setTableInfo(String tableInfo) {
    this.tableInfo = tableInfo;
  }

  @Override
  public void claimAlive() {

  }

  @Override
  public BufferedInputStream readResourceFileAsStream(String resourceName)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] readResourceFile(String resourceName) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterable<Object[]> readResourceTable(String resourceName)
      throws IOException {
    return null;
  }

  @Override
  public VolumeInfo getInputVolumeInfo() throws IOException {
    return null;
  }

  @Override
  public VolumeInfo getInputVolumeInfo(String label) throws IOException {
    return null;
  }

  @Override
  public VolumeInfo getOutputVolumeInfo() throws IOException {
    return null;
  }

  @Override
  public VolumeInfo getOutputVolumeInfo(String label) throws IOException {
    return null;
  }

  @Override
  public FileSystem getInputVolumeFileSystem() throws IOException {
    return null;
  }

  @Override
  public FileSystem getInputVolumeFileSystem(String label) throws IOException {
    return null;
  }

  @Override
  public FileSystem getOutputVolumeFileSystem() throws IOException {
    return null;
  }

  @Override
  public FileSystem getOutputVolumeFileSystem(String label) throws IOException {
    return null;
  }

  private Properties configurations = new Properties();
  @Override
  public Properties getConfigurations() {
    return configurations;
  }

  @Override
  public Iterable<BufferedInputStream> readCacheArchiveAsStream(String resourceName)
      throws IOException {
    return null;
  }

  @Override
  public Iterable<BufferedInputStream> readCacheArchiveAsStream(String resourceName,
      String relativePath) throws IOException {
    return null;
  }

  @Override
  public FileSystem getTempFileSystem() throws IOException {
    return null;
  }
}
