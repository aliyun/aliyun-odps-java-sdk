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

package com.aliyun.odps.udf.local;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.volume.FileSystem;

public class LocalExecutionContext extends ExecutionContext {

  private WareHouse wareHouse;

  public LocalExecutionContext() {
    wareHouse = WareHouse.getInstance();
  }

  @Override
  public void claimAlive() {
  }

  @Override
  public BufferedInputStream readResourceFileAsStream(String resourceName) throws IOException {
    try {
      return wareHouse.readResourceFileAsStream(wareHouse.getOdps().getDefaultProject(),
                                                resourceName, ',');
    } catch (OdpsException e) {
      throw new IOException(e.getMessage());
    }

  }

  @Override
  public byte[] readResourceFile(String resourceName) throws IOException {
    try {
      return wareHouse.readResourceFile(wareHouse.getOdps().getDefaultProject(), resourceName, ',');
    } catch (OdpsException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public Iterable<Object[]> readResourceTable(final String resourceName) throws IOException {
    return new Iterable<Object[]>() {

      @Override
      public Iterator<Object[]> iterator() {
        try {
          return wareHouse
              .readResourceTable(wareHouse.getOdps().getDefaultProject(), resourceName, ',');
        } catch (IOException e) {
          throw new LocalRunError(e.getMessage());
        } catch (OdpsException e) {
          throw new LocalRunError(e.getMessage());
        }
      }
    };

  }

  //TODO
  @Override
  public VolumeInfo getInputVolumeInfo() throws IOException {
    return null;
  }

  //TODO
  @Override
  public VolumeInfo getInputVolumeInfo(String label) throws IOException {
    return null;
  }

  //TODO
  @Override
  public VolumeInfo getOutputVolumeInfo() throws IOException {
    return null;
  }

  //TODO
  @Override
  public VolumeInfo getOutputVolumeInfo(String label) throws IOException {
    return null;
  }

  //TODO
  @Override
  public FileSystem getInputVolumeFileSystem() throws IOException {
    return null;
  }

  //TODO
  @Override
  public FileSystem getInputVolumeFileSystem(String label) throws IOException {
    return null;
  }

  //TODO
  @Override
  public FileSystem getOutputVolumeFileSystem() throws IOException {
    return null;
  }

  //TODO
  @Override
  public FileSystem getOutputVolumeFileSystem(String label) throws IOException {
    return null;
  }

  //TODO
  @Override
  public Iterable<BufferedInputStream> readCacheArchiveAsStream(String resourceName)
      throws IOException {
    return null;
  }

  //TODO
  @Override
  public Iterable<BufferedInputStream> readCacheArchiveAsStream(String resourceName,
      String relativePath) throws IOException {
    return null;
  }

}
