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

import java.util.Iterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;

public class VolumeTest extends TestBase {


  private String volumeName = "volume_name_for_test";
  private String volumeComment = "test volume api";
  private String volumeName1 = "volume_name_with_lifecycle";
  private Long lifecycle = 10L;

  @Before
  public void setUp() throws Exception {
    odps = OdpsTestUtils.newDefaultOdps();
    odps.volumes().create(odps.getDefaultProject(), volumeName, volumeComment);
    odps.volumes().create(odps.getDefaultProject(), volumeName1, volumeComment, Volume.Type.OLD, lifecycle);
  }


  @After
  public void tearDown() throws Exception {
    odps.volumes().delete(volumeName);
    odps.volumes().delete(volumeName1);
  }

  @Test
  public void testGetName() {
    Volume volume = odps.volumes().get(volumeName);
    Assert.assertEquals(volume.getName(), volumeName);
  }

  @Test
  public void testGetComment() {
    Volume volume = odps.volumes().get(volumeName);
    Assert.assertEquals(volume.getComment(), volumeComment);
  }

  @Test
  public void testGetLength() {
    Volume volume = odps.volumes().get(volumeName);
    Assert.assertEquals(volume.getLength(), 0L);
  }

  @Test
  public void testGetFileCount() {
    Volume volume = odps.volumes().get(volumeName);
    Assert.assertEquals(volume.getFileCount(), 0);
  }

  @Test
  public void testGetOwner() {
    Volume volume = odps.volumes().get(volumeName);
    Assert.assertNotNull(volume.getOwner());
  }

  @Test
  public void testGetCreatedTime() {
    Volume volume = odps.volumes().get(volumeName);
    Assert.assertNotNull(volume.getCreatedTime());
  }

  @Test
  public void testGetLastModifiedTime() {
    Volume volume = odps.volumes().get(volumeName);
    Assert.assertNotNull(volume.getLastModifiedTime());
  }

  @Test
  public void testIteratorPartition() {
    Volume volume = odps.volumes().get(volumeName);
    Iterator<VolumePartition> iterator = volume.getPartitionIterator();
    while (iterator.hasNext()) {
      VolumePartition partition = iterator.next();
      Assert.assertNotNull(partition.getName());
    }
  }

  @Test
  public void testGetLifecycle() {
    Volume volume = odps.volumes().get(volumeName);
    Assert.assertNull(volume.getLifecycle());

    Volume volume1 = odps.volumes().get(volumeName1);
    Assert.assertEquals(lifecycle, volume1.getLifecycle());
  }

  @Test
  public void testUpdateLifecycle() throws OdpsException {
    Volume volume1 = odps.volumes().get(volumeName1);

    Long l = 20L;
    volume1.setLifecycle(l);
    odps.volumes().update(volume1);


    Assert.assertEquals(l, odps.volumes().get(volumeName1).getLifecycle());

    //disable
    volume1.setLifecycle(0L);
    odps.volumes().update(volume1);
    Assert.assertNull(odps.volumes().get(volumeName1).getLifecycle());
  }

  @Test
  public void testIteratorPartitionFilter() {
    VolumeFilter volumeFilter = new VolumeFilter();
    volumeFilter.setName("test");
    Volume volume = odps.volumes().get(volumeName);
    Iterator<VolumePartition> iterator = volume.getPartitionIterator(volumeFilter);
    while (iterator.hasNext()) {
      VolumePartition partition = iterator.next();
      Assert.assertNotNull(partition.getName());
    }
  }

  @Test
  public void testDeletePartition() throws Exception {
    //TODO should create partition first, only mr and tunnel can create partition, but mr and tunnel not support volume now.
    //Volume volume = odps.volumes().get("test_volume");
    //volume.deleteVolumePartition("test_partition");
  }
}
