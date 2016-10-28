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

import java.io.InputStream;
import java.util.List;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Resource.Type;
import com.aliyun.odps.Table;
import com.aliyun.odps.Task;

public interface MetaExplorer {

  /**
   * Upload a resource file and add it to resource list. Returns the resource
   * name, which is expected as <code>filebase+padding+extension</code>.
   *
   * @param filePath
   * @param type
   * @param padding
   * @param isTempResource
   * @return resource name
   * @throws OdpsException
   */
  public abstract String addFileResourceWithRetry(String filePath, Resource.Type type,
                                                  String padding,
                                                  boolean isTempResource) throws OdpsException;

  /**
   * Get default Project
   *
   * @return default project
   */
  public abstract String getDefaultProject();

  public abstract void deleteResource(String name) throws OdpsException;

  public abstract boolean existsTable(String proj, String name) throws OdpsException;

  public abstract boolean existsResource(String proj, String name) throws OdpsException;

  public abstract Table getTable(String proj, String tblName);

  public abstract Instance createInstance(Task task, int priority) throws OdpsException;

  @Deprecated
  public abstract void deleteFunction(String project, String name) throws OdpsException;

  @Deprecated
  public abstract void createFunction(List<String> resources, String project, String name,
                                      String className) throws OdpsException;

  String addTempResourceWithRetry(InputStream in, String name, Type type)
      throws OdpsException;

  boolean existsVolume(String projectName, String volumeName)
      throws OdpsException;
}