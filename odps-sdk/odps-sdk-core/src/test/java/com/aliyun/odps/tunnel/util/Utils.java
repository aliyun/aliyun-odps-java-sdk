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

package com.aliyun.odps.tunnel.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.odps.VolumeFile;
import com.aliyun.odps.VolumePartition;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.security.SecurityConfiguration;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.tunnel.VolumeTunnel;

public class Utils {

  private static AtomicLong counter = new AtomicLong();
  private static Project project = null;
  private static Odps odps = OdpsTestUtils.newDefaultOdps();

  public static Project getProject() throws OdpsException {
    if (project == null) {
      Odps odps = OdpsTestUtils.newDefaultOdps();
      project = odps.projects().get();
    }
    return project;
  }

  // support multi-sqls: sql1;sql2
  public static String exeSql(String sql) {
    String[] sqlArray = sql.split(";");
    String ret = null;
    for (int i = 0; i < sqlArray.length; ++i) {
      // System.out.println("sqlArray[" + i + "]:" + sqlArray[i]);
      if (null == sqlArray[i] || "" == sqlArray[i]) {
        return null;
      }
      ret = SqlExecutor.exec(getProjectName(), sqlArray[i] + ";");
    }
    return ret;
  }

  public static String getVersion() {
    return "tunnel_aliyunsdk_";
  }

  public static void setDefaultSecurity(String projectName) throws OdpsException {
    Project project = odps.projects().get(projectName);
    SecurityManager sm = project.getSecurityManager();
    SecurityConfiguration sc = sm.getSecurityConfiguration();
    sc.disableProjectProtection();
    sc.enableCheckPermissionUsingAcl();
    sc.enableCheckPermissionUsingPolicy();
    //sc.enableLabelSecurity();
    sc.enableObjectCreatorHasAccessPermission();
    sc.enableObjectCreatorHasGrantPermission();
    sm.setSecurityConfiguration(sc);
    sc.reload();
  }

  public static String exeSql(String sql, String project) {
    String[] sqlArray = sql.split(";");
    String ret = null;
    for (int i = 0; i < sqlArray.length; ++i) {
      // System.out.println("sqlArray[" + i + "]:" + sqlArray[i]);
      if (null == sqlArray[i] || "" == sqlArray[i]) {
        return null;
      }
      ret = SqlExecutor.exec(project, sqlArray[i] + ";");
    }
    return ret;

  }

  public static VolumeTunnel getTunnelInstance() {
    VolumeTunnel tunnel = new VolumeTunnel(odps);
    return tunnel;
  }

  public static String getRandomProjectName() {
    return "otopen_prj_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
  }

  public static String getRandomVolumeName() {
    return "volume_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
  }

  public static String getRandomPartitionName() {
    return "pt_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
  }

  public static String getRandomFileName() {
    return "file_" + System.currentTimeMillis() + "_" + counter.addAndGet(1);
  }

  public static String getProjectName() {
    return OdpsTestUtils.newDefaultOdps().getDefaultProject();
  }

  public static void createVolume(String volumeName, String projectName) throws Exception {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    odps.setDefaultProject(projectName);
    odps.volumes().create(projectName, volumeName, "test volumes sdk");
  }

  public static void dropVolume(String volumeName, String projectName) throws Exception {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    odps.setDefaultProject(projectName);
    odps.volumes().delete(projectName, volumeName);
  }

  public static List<String> listVolumePartitions(String volumeName, String partitionName,
                                                  String projectName)
      throws Exception {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    odps.setDefaultProject(projectName);
    List<String> result = new LinkedList<String>();
    VolumePartition partiton = odps.volumes().get(volumeName).getVolumePartition(partitionName);
    Iterator<VolumeFile> iter = partiton.getFileIterator();
    while (iter.hasNext()) {
      result.add(iter.next().getName());
    }
    return result;
  }
}
