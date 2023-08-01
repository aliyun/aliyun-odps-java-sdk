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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;

public class VolumesTest extends TestBase {

  private static String volumeName = "volumes_name_for_test";
  private static String extVolumeName = "test_ext_volume_sdk";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    odps = OdpsTestUtils.newDefaultOdps();
    if (!odps.volumes().exists(volumeName)) {
      odps.volumes().create(odps.getDefaultProject(), volumeName, "test volumes api");
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    odps.volumes().delete(volumeName);
    try {
      odps.volumes().delete(extVolumeName);
    } catch (Exception e) {
      e.printStackTrace();
      //ignore
    }
  }

  @Test
  public void testCreateVolume() throws OdpsException {
    //Already Test in method setUp() ,please don't create again
  }

  @Test
  public void testGetVolume() {
    Volume volume = odps.volumes().get(volumeName);
    Assert.assertNotNull(volume);
  }

  @Test
  public void testExistsVolume() throws OdpsException {
    Assert.assertTrue(odps.volumes().exists(volumeName));
    Assert.assertFalse(odps.volumes().exists("not_exists_volume"));
  }

  @Test
  public void testIteratorVolume() {
    Iterator<Volume> iterator = odps.volumes().iterator();
    while (iterator.hasNext()) {
      Volume volume = iterator.next();
      Assert.assertNotNull(volume.getName());
    }
  }

  @Test
  public void testIteratorVolumeFilter() {
    VolumeFilter volumeFilter = new VolumeFilter();
    volumeFilter.setName("volume_name");
    Iterator<Volume> iterator = odps.volumes().iterator(volumeFilter);
    while (iterator.hasNext()) {
      Volume volume = iterator.next();
      Assert.assertNotNull(volume.getName());
    }
  }

  @Test
  public void testDeleteVolume() throws OdpsException {
    //Already Test in method tearDown() ,please don't delete again
  }

  @Test(expected = OdpsException.class)
  public void testExternalVolume() throws OdpsException {
    try {
      odps.volumes().delete(extVolumeName);
    } catch (Exception e) {
      //ignore
    }

    Volumes.VolumeBuilder builder = new Volumes.VolumeBuilder();

    String location = "oss://id:key@oss-cn-hangzhou-zmf.aliyuncs.com/12345/test_dir";
    builder.volumeName(extVolumeName).type(Volume.Type.EXTERNAL).extLocation(location);

    try {
      odps.volumes().create(builder);
    } catch (OdpsException e) {
      // id key not specified
      Assert.assertTrue(e.getMessage().contains("Status: 403"));
      throw e;
    }
  }
}
