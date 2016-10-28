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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Function;
import com.aliyun.odps.Instance;
import com.aliyun.odps.JarResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.Task;

/**
 * Meta explorer
 *
 * Collect meta related operation so that we can improve the code testability.
 */
public class MetaExplorerImpl implements MetaExplorer {

  private static final Log LOG = LogFactory.getLog(MetaExplorer.class);
  private Odps odps;

  public MetaExplorerImpl(Odps odps) {
    this.odps = odps;
  }

  /**
   * Upload a resource file and add it to resource list. The resource name is
   * <code>fileprefix+padding+extension</code>
   *
   * @param filePath
   * @param type
   * @param padding
   * @param isTempResource
   * @return genuine resource name
   * @throws OdpsException
   */
  @Override
  public String addFileResourceWithRetry(String filePath, Resource.Type type, String padding,
                                         boolean isTempResource) throws OdpsException {

    String baseName = FilenameUtils.getBaseName(filePath);
    String extension = FilenameUtils.getExtension(filePath);
    String resourceName = baseName + padding;

    File file = new File(filePath);
    FileResource res = null;
    switch (type) {
      case FILE:
        res = new FileResource();
        break;
      case JAR:
        res = new JarResource();
        break;
      default:
        throw new OdpsException("Unsupported resource type:" + type);
    }

    FileInputStream in = null;
    int trial = 0;
    res.setIsTempResource(isTempResource);
    String rname = null;
    while (trial <= 3) {
      try {
        rname = resourceName + "_" + trial;
        if (extension != null && !extension.isEmpty()) {
          rname += "." + extension;
        }
        res.setName(rname);
        in = new FileInputStream(file);
        odps.resources().create(res, in);
        return rname;
      } catch (OdpsException e) {
        LOG.error(
            "Upload resource " + rname + " failed:" + e.getMessage() + ", retry count=" + trial);
        try {
          Thread.sleep(60 * 1000);
        } catch (InterruptedException e1) {
          // Silently swallow it.
        }
      } catch (FileNotFoundException e) {
        throw new OdpsException("Resource file: " + filePath + " not found.", e);
      } finally {
        if (in != null) {
          try {
            in.close();
          } catch (IOException e) {
            throw new OdpsException(e);
          }
        }
      }
      trial++;
    }
    throw new OdpsException("Upload resource failed.");

  }

  @Override
  public String addTempResourceWithRetry(InputStream in, String prefix, Resource.Type type)
      throws OdpsException {
    FileResource res = null;
    String extension = "";
    switch (type) {
      case FILE:
        res = new FileResource();
        break;
      case JAR:
        res = new JarResource();
        extension = "jar";
        break;
      default:
        throw new OdpsException("Unsupported resource type:" + type);
    }

    int trial = 0;
    String resourceName = null;
    res.setIsTempResource(true);
    while (trial <= 3) {
      try {
        resourceName = prefix + "_" + trial;
        if (extension != null && !extension.isEmpty()) {
          resourceName += "." + extension;
        }
        res.setName(resourceName);
        odps.resources().create(res, in);
        return resourceName;
      } catch (OdpsException e) {
        LOG.error("Upload resource " + resourceName + " failed:" + e.getMessage() + ", retry count=" + trial);
        try {
          Thread.sleep(60 * 1000);
        } catch (InterruptedException e1) {
          // Silently swallow it.
        }
        try {
          in.reset();
        } catch (IOException e1) {
          // If input stream is not reset-able, drop coming retry.
          break;
        }
      }
      trial++;
    }
    throw new OdpsException("Upload resource failed.");
  }

  @Override
  public boolean existsVolume(String projectName, String volumeName)
      throws OdpsException {
    return odps.volumes().exists(projectName, volumeName);
  }

  @Override
  public void deleteResource(String name) throws OdpsException {
    odps.resources().delete(odps.getDefaultProject(), name);
  }

  @Override
  public boolean existsTable(String proj, String name) throws OdpsException {
    if (proj == null || proj.isEmpty()) {
      proj = odps.getDefaultProject();
    }
    return odps.tables().exists(proj, name);
  }

  @Override
  public boolean existsResource(String proj, String name) throws OdpsException {
    if (proj == null || proj.isEmpty()) {
      proj = odps.getDefaultProject();
    }
    return odps.resources().exists(proj, name);
  }

  @Override
  public void createFunction(List<String> resources, String project, String name, String className)
      throws OdpsException {
    Function func = new Function();
    func.setName(name);
    func.setClassType(className);
    func.setResources(resources);
    odps.functions().create(func);
  }

  @Override
  public void deleteFunction(String project, String name) throws OdpsException {
    odps.functions().delete(project, name);
  }

  @Override
  public Table getTable(String projectName, String tblName) {
    if (projectName == null) {
      projectName = odps.getDefaultProject();
    }
    return odps.tables().get(projectName, tblName);
  }

  @Override
  public String getDefaultProject() {
    return odps.getDefaultProject();
  }

  @Override
  public Instance createInstance(Task task, int priority) throws OdpsException {
    return odps.instances().create(task, priority);
  }
}
