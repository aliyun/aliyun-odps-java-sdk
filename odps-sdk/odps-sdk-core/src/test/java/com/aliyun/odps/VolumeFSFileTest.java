/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aliyun.odps;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.VolumeFSErrorCode;
import com.aliyun.odps.tunnel.VolumeFSTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.VolumeOutputStream;
import com.aliyun.odps.tunnel.util.Utils;

/**
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
@FixMethodOrder
public class VolumeFSFileTest extends TestBase {

  private static final String TEST_VOLUME = "volumefstest";
  private static final String TEST_DIR = "/" + TEST_VOLUME + "/dir";
  private static final String TEST_FILE = "/" + TEST_VOLUME + "/file";
  private static final String TEST_DIR_LIST_PARENT = "/" + TEST_VOLUME + "/parent";
  private static final String TEST_DIR_LIST_SON_DIR1 = "/" + TEST_VOLUME + "/parent/sondir1";
  private static final String TEST_DIR_LIST_SON_DIR2 = "/" + TEST_VOLUME + "/parent/sondir2";
  private static final String TEST_DIR_LIST_SON_FILE1 = "/" + TEST_VOLUME + "/parent/sonfile1";
  private static final String TEST_DIR_LIST_SON_FILE2 = "/" + TEST_VOLUME + "/parent/sonfile2";
  private static final String TEST_CASE_DIR = "/" + TEST_VOLUME + "/AbCdir";


  private VolumeFSFile volumeFSDir;
  private VolumeFSFile volumeFSFile;

  @Before
  public void setUp() throws Exception {
    long start = System.currentTimeMillis();
    odps = OdpsTestUtils.newDefaultOdps();
    try {
      if (odps.volumes().exists(TEST_VOLUME)) {
        odps.volumes().delete(odps.getDefaultProject(), TEST_VOLUME);
      }
    } catch (OdpsException e) {
    }
    try {
      odps.volumes().create(odps.getDefaultProject(), TEST_VOLUME, "unit test", Volume.Type.NEW);
    } catch (OdpsException e) {
      assertTrue(e.getMessage(), false);
    }
    try {
      volumeFSDir =
          VolumeFSFile.create(odps.getDefaultProject(), TEST_DIR, true, odps.getRestClient());
    } catch (VolumeException e) {
      if (!VolumeFSErrorCode.PathAlreadyExists.equalsIgnoreCase(e.getErrCode())) {
        assertTrue(e.getMessage(), false);
      }
    }
    createFile(TEST_FILE);
    volumeFSFile = odps.volumes().get(TEST_VOLUME).getVolumeFSFile(TEST_FILE);
    System.out.println("setup:" + (System.currentTimeMillis() - start));
  }



  @After
  public void tearDown() {
    long start = System.currentTimeMillis();
    try {
      volumeFSDir.delete(true);
      volumeFSFile.delete(false);
    } catch (VolumeException e) {
      if (!VolumeFSErrorCode.NoSuchPath.equalsIgnoreCase(e.getErrCode())) {
        assertTrue(e.getMessage(), false);
      }
    }
    try {
      odps.volumes().delete(odps.getDefaultProject(), TEST_VOLUME);
    } catch (OdpsException e) {
    }
    System.out.println("tearDown:" + (System.currentTimeMillis() - start));
  }

  @Test
  public void testVolumeMissing() {
    try {
      odps.volumes().get(TEST_VOLUME).getVolumeFSFile("/");
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), VolumeFSErrorCode.VolumeMissing.equalsIgnoreCase(e.getErrCode()));
    }
  }

  @Test
  public void testNoSuchVolume() {
    try {
      VolumeFSFile.create(odps.getDefaultProject(), "/chuxiang_no_such_volume/", true,
          odps.getRestClient());
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), VolumeFSErrorCode.NoSuchVolume.equalsIgnoreCase(e.getErrCode()));
    }
  }


  @Test
  public void testCreateDirExists() {
    try {
      VolumeFSFile.create(odps.getDefaultProject(), TEST_FILE, true, odps.getRestClient());
    } catch (VolumeException e) {
      assertTrue(e.getMessage(),
          VolumeFSErrorCode.PathAlreadyExists.equalsIgnoreCase(e.getErrCode()));
    }
  }

  @Test
  public void testCreateFile() {
    try {
      VolumeFSFile.create(odps.getDefaultProject(), TEST_FILE + "1", false, odps.getRestClient());
    } catch (VolumeException e) {
      assertTrue(e.getMessage(),
          VolumeFSErrorCode.NotAcceptableOperation.equalsIgnoreCase(e.getErrCode()));
    }
  }

  @Test
  public void testCreateDir() {
    try {
      VolumeFSFile.create(odps.getDefaultProject(), TEST_DIR, true, odps.getRestClient());
    } catch (VolumeException e) {
    }
    try {
      VolumeFSFile.create(odps.getDefaultProject(), TEST_DIR, true, odps.getRestClient());
      assertTrue(false);
    } catch (VolumeException e) {
      assertTrue(e.getMessage(),
          VolumeFSErrorCode.PathAlreadyExists.equalsIgnoreCase(e.getErrCode()));
    }
    try {
      VolumeFSFile file = odps.volumes().get(TEST_VOLUME).getVolumeFSFile(TEST_DIR);
      file.reload();
      assertTrue(Boolean.TRUE == file.getIsdir());
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), true);
    } catch (OdpsException e) {
      assertTrue(e.getMessage(), true);
    }

  }

  @Test
  public void testDeleteDir() {
    try {
      volumeFSDir.delete(false);
      try {
        volumeFSDir.reload();
      } catch (OdpsException e) {
        assertTrue(e.getMessage(), VolumeFSErrorCode.NoSuchPath.equalsIgnoreCase(e.getErrorCode()));
      }
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    }
  }

  @Test
  public void testDeleteFile() {
    try {
      volumeFSFile.delete(false);
      try {
        volumeFSFile.reload();
      } catch (OdpsException e) {
        assertTrue(e.getMessage(), VolumeFSErrorCode.NoSuchPath.equalsIgnoreCase(e.getErrorCode()));
      }
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    }
  }

  @Test
  public void testGetDir() {
    try {
      assertTrue(volumeFSDir.getAccessTime() != null);
      assertTrue(volumeFSDir.getBlockReplications() != null);
      assertTrue(volumeFSDir.getBlockSize() != null);
      assertTrue(volumeFSDir.getCreationTime() != null);
      assertTrue(volumeFSDir.getGroup() != null);
      assertTrue(volumeFSDir.getIsdir());
      assertTrue(volumeFSDir.getLength() != null);
      assertTrue(volumeFSDir.getModificationTime() != null);
      assertTrue(volumeFSDir.getOwner() != null);
      assertTrue(TEST_DIR.equalsIgnoreCase(volumeFSDir.getPath()));
      assertTrue(volumeFSDir.getPermission() != null);
      assertTrue(odps.getDefaultProject().equalsIgnoreCase(volumeFSDir.getProject()));
      assertTrue(volumeFSDir.getQuota() != null);
      assertTrue(volumeFSDir.getSymlink() != null);
      assertTrue(TEST_VOLUME.equalsIgnoreCase(volumeFSDir.getVolume()));
    } catch (Exception e) {
      assertTrue(e.getMessage(), false);
    }
  }

  @Test
  public void testGetFile() {
    try {
      assertTrue(volumeFSFile.getAccessTime() != null);
      assertTrue(volumeFSFile.getBlockReplications() != null);
      assertTrue(volumeFSFile.getBlockSize() != null);
      assertTrue(volumeFSFile.getCreationTime() != null);
      assertTrue(volumeFSFile.getGroup() != null);
      assertTrue(!volumeFSFile.getIsdir());
      assertTrue(volumeFSFile.getLength() != null);
      assertTrue(volumeFSFile.getModificationTime() != null);
      assertTrue(volumeFSFile.getOwner() != null);
      assertTrue(TEST_FILE.equalsIgnoreCase(volumeFSFile.getPath()));
      assertTrue(volumeFSFile.getPermission() != null);
      assertTrue(odps.getDefaultProject().equalsIgnoreCase(volumeFSFile.getProject()));
      assertTrue(volumeFSFile.getQuota() != null);
      assertTrue(volumeFSFile.getSymlink() != null);
      assertTrue(TEST_VOLUME.equalsIgnoreCase(volumeFSFile.getVolume()));
    } catch (Exception e) {
      assertTrue(e.getMessage(), false);
    }
  }

  @Test
  public void testRenameDir() {
    Map<String, String> param = new HashMap<String, String>();
    param.put("path", TEST_DIR + "1");
    try {
      volumeFSDir.update(param);
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    }
    try {
      VolumeFSFile dir = odps.volumes().get(TEST_VOLUME).getVolumeFSFile(TEST_DIR);
      assertTrue(dir.getIsdir());
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    } catch (ReloadException e) {
      assertTrue(e.getMessage(), true);
    }
    try {
      VolumeFSFile dir = odps.volumes().get(TEST_VOLUME).getVolumeFSFile(TEST_DIR + "1");
      assertTrue(dir.getIsdir());
      param = new HashMap<String, String>();
      param.put("path", TEST_DIR);
      try {
        dir.update(param);
        dir.getIsdir();
      } catch (VolumeException e) {
        assertTrue(e.getMessage(), false);
      }
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    }

  }

  @Test
  public void testRenameFile() {
    Map<String, String> param = new HashMap<String, String>();
    param.put("path", TEST_FILE + "1");
    try {
      volumeFSFile.update(param);
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    }
    try {
      VolumeFSFile dir = odps.volumes().get(TEST_VOLUME).getVolumeFSFile(TEST_FILE);
      assertTrue(!dir.getIsdir());
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    } catch (ReloadException e) {
      assertTrue(e.getMessage(), true);
    }
    try {
      VolumeFSFile file = odps.volumes().get(TEST_VOLUME).getVolumeFSFile(TEST_FILE + "1");
      file.getIsdir();
      param = new HashMap<String, String>();
      param.put("path", TEST_FILE);
      try {
        file.update(param);
        assertTrue(!file.getIsdir());
      } catch (VolumeException e) {
        assertTrue(e.getMessage(), false);
      }
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    }

  }

  @Test
  public void testSetReplicationForDir() {
    Map<String, String> param = new HashMap<String, String>();
    param.put("replication", "3");
    try {
      volumeFSDir.update(param);
    } catch (VolumeException e) {
      assertTrue(e.getMessage(),
          VolumeFSErrorCode.NotAcceptableOperation.equalsIgnoreCase(e.getErrCode()));
    }
  }

  @Test
  public void testSetReplicationForFile() {
    Map<String, String> param = new HashMap<String, String>();
    param.put("replication", "5");
    try {
      volumeFSFile.update(param);
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    }
    try {
      VolumeFSFile file = odps.volumes().get(TEST_VOLUME).getVolumeFSFile(volumeFSFile.getPath());
      assertTrue(5 == file.getBlockReplications());
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    }
    param.put("replication", "3");
    try {
      volumeFSFile.update(param);
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    }
    try {
      VolumeFSFile file = odps.volumes().get(TEST_VOLUME).getVolumeFSFile(volumeFSFile.getPath());
      assertTrue(3 == file.getBlockReplications());
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    }
  }

  @Test
  public void testListDir() {
    try {
      volumeFSDir =
          VolumeFSFile.create(odps.getDefaultProject(), TEST_DIR_LIST_PARENT, true,
              odps.getRestClient());
      VolumeFSFile.create(odps.getDefaultProject(), TEST_DIR_LIST_SON_DIR1, true,
          odps.getRestClient());
      VolumeFSFile.create(odps.getDefaultProject(), TEST_DIR_LIST_SON_DIR2, true,
          odps.getRestClient());
      createFile(TEST_DIR_LIST_SON_FILE1);
      createFile(TEST_DIR_LIST_SON_FILE2);
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), false);
    }
    Iterator<VolumeFSFile> it = volumeFSDir.iterator();
    int count = 0;
    while (it.hasNext()) {
      count++;
      volumeFSDir = it.next();
      assertTrue(volumeFSDir.getAccessTime() != null);
      assertTrue(volumeFSDir.getBlockReplications() != null);
      assertTrue(volumeFSDir.getBlockSize() != null);
      assertTrue(volumeFSDir.getCreationTime() != null);
      assertTrue(volumeFSDir.getGroup() != null);
      assertTrue(volumeFSDir.getIsdir() != null);
      assertTrue(volumeFSDir.getLength() != null);
      assertTrue(volumeFSDir.getModificationTime() != null);
      assertTrue(volumeFSDir.getOwner() != null);
      assertTrue(volumeFSDir.getPath() != null);
      assertTrue(volumeFSDir.getPermission() != null);
      assertTrue(odps.getDefaultProject().equalsIgnoreCase(volumeFSDir.getProject()));
      assertTrue(volumeFSDir.getQuota() != null);
      assertTrue(volumeFSDir.getSymlink() != null);
      assertTrue(TEST_VOLUME.equalsIgnoreCase(volumeFSDir.getVolume()));
    }
    assertTrue(count == 4);
    try {
      volumeFSDir.delete(false);
    } catch (VolumeException e) {
      assertTrue(e.getMessage(), VolumeFSErrorCode.DeleteConflict.equalsIgnoreCase(e.getErrCode()));
      try {
        volumeFSDir.delete(true);
      } catch (VolumeException e1) {
        assertTrue(e1.getMessage(), false);
      }
      try {
        volumeFSDir.reload();
      } catch (OdpsException e2) {
        assertTrue(e2.getMessage(),
            VolumeFSErrorCode.NoSuchPath.equalsIgnoreCase(e2.getErrorCode()));
      }
    }
  }

  @Test
  public void testListFile() {
    Iterator<VolumeFSFile> it = volumeFSFile.iterator();
    int count = 0;
    while (it.hasNext()) {
      count++;
      volumeFSFile = it.next();
      assertTrue(volumeFSFile.getAccessTime() != null);
      assertTrue(volumeFSFile.getBlockReplications() != null);
      assertTrue(volumeFSFile.getBlockSize() != null);
      assertTrue(volumeFSFile.getCreationTime() != null);
      assertTrue(volumeFSFile.getGroup() != null);
      assertTrue(!volumeFSFile.getIsdir());
      assertTrue(volumeFSFile.getLength() != null);
      assertTrue(volumeFSFile.getModificationTime() != null);
      assertTrue(volumeFSFile.getOwner() != null);
      assertTrue(TEST_FILE.equalsIgnoreCase(volumeFSFile.getPath()));
      assertTrue(volumeFSFile.getPermission() != null);
      assertTrue(odps.getDefaultProject().equalsIgnoreCase(volumeFSFile.getProject()));
      assertTrue(volumeFSFile.getQuota() != null);
      assertTrue(volumeFSFile.getSymlink() != null);
      assertTrue(TEST_VOLUME.equalsIgnoreCase(volumeFSFile.getVolume()));
    }
    assertTrue(count == 1);
  }

  @Test
  public void testCaseSensitive() {
    VolumeFSFile lowerCaseFile = null;
    try {
      lowerCaseFile =
          VolumeFSFile.create(odps.getDefaultProject(), TEST_CASE_DIR.toLowerCase(), true,
              odps.getRestClient());
    } catch (VolumeException e) {
      if (!VolumeFSErrorCode.PathAlreadyExists.equalsIgnoreCase(e.getErrCode())) {
        assertTrue(e.getMessage(), false);
      }
    }
    VolumeFSFile upperCaseFile = null;
    try {
      upperCaseFile =
          VolumeFSFile.create(odps.getDefaultProject(), TEST_CASE_DIR.toUpperCase(), true,
              odps.getRestClient());
      upperCaseFile.delete(false);
    } catch (VolumeException e) {
      if (!VolumeFSErrorCode.PathAlreadyExists.equalsIgnoreCase(e.getErrCode())) {
        assertTrue(e.getMessage(), false);
      }
    }
    try {
      lowerCaseFile.reload();
    } catch (OdpsException e) {
      assertTrue(e.getMessage(), false);
    } finally {
      try {
        lowerCaseFile.delete(false);
      } catch (VolumeException e) {
        assertTrue(e.getMessage(), false);
      }
    }

  }

  private void createFile(String path) {
    VolumeFSTunnel volumeFSTunnel = Utils.getVolumeFSTunnel();
    OutputStream outputStream = null;
    try {
      outputStream =
          volumeFSTunnel.openOutputStream(odps.getDefaultProject(), path, 3, new CompressOption());
    } catch (TunnelException e) {
      assertTrue(e.getMessage(), false);
    }
    try {
      outputStream.close();
    } catch (IOException e) {
      assertTrue(e.getMessage(), false);
    }
    String sessionId = null;
    try {
      sessionId = VolumeFSTunnel.getUploadSessionId((VolumeOutputStream) outputStream);
    } catch (TunnelException e) {
      assertTrue(e.getMessage(), false);
    }
    try {
      volumeFSTunnel.commit(odps.getDefaultProject(), path, sessionId);
    } catch (TunnelException e) {
      assertTrue(e.getMessage(), false);
    }
  }
}
