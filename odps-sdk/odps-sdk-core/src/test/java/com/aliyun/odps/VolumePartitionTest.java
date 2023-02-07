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

package com.aliyun.odps;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.tunnel.VolumeTunnel;

//TODO no create volume partition api, should pre create partition before run test
public class VolumePartitionTest extends TestBase {

  private String volumeName = "volume_name_GetFile_test";
  private String partitionName = "volume_partition_GetFile_test";
  private String pathName = "a";
  private String[] fileNames = {"a/a", "a/c"};

  @Before
  public void setUp() throws Exception {
    odps = OdpsTestUtils.newDefaultOdps();
    if (odps.volumes().exists(volumeName)) {
      odps.volumes().delete(volumeName);
    }
    odps.volumes().create(odps.getDefaultProject(), volumeName, "test volumes api");
    VolumeTunnel tunnel = new VolumeTunnel(odps);
    VolumeTunnel.UploadSession session = tunnel.createUploadSession(
        odps.getDefaultProject(),
        volumeName, partitionName);
    for (int i = 0; i < fileNames.length; i++) {
      OutputStream os = session.openOutputStream(fileNames[i]);
      os.write('\n');
      os.close();
    }
    session.commit(fileNames);
  }

  @After
  public void tearDown() throws Exception {
    odps.volumes().delete(volumeName);
  }

  @Test
  public void testIteratorFile() {
    Volume volume = odps.volumes().get(volumeName);
    ArrayList<String> arrayExpected = new ArrayList<String>();
    ArrayList<String> arrayActual = new ArrayList<String>();
    arrayExpected.add("a/");

    VolumePartition partition = volume.getVolumePartition(partitionName);
    Iterator<VolumeFile> iterator = partition.getFileIterator();
    while (iterator.hasNext()) {
      VolumeFile file = iterator.next();
      arrayActual.add(file.getName());
    }
    Assert.assertEquals(arrayExpected, arrayActual);
  }

  @Test
  public void testGetFileIteratorPath() {
    Volume volume = odps.volumes().get(volumeName);
    ArrayList<String> arrayExpected = new ArrayList<String>();
    ArrayList<String> arrayActual = new ArrayList<String>();
    arrayExpected.add("a");
    arrayExpected.add("c");

    VolumePartition partition = volume.getVolumePartition(partitionName);
    Iterator<VolumeFile> iterator1 = partition.getFileIterator(pathName);
    while (iterator1.hasNext()) {
      VolumeFile file = iterator1.next();
      arrayActual.add(file.getName());
    }
    Assert.assertEquals(arrayExpected, arrayActual);
  }
}
