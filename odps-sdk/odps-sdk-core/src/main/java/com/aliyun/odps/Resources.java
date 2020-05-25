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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.Resource.ResourceModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * Resources 表示ODPS内所有{@link Resource}的集合，可以通过此对象可以创建、删除和浏览
 * 各种Resource类型。
 * <br />
 * 比如：
 * <pre>
 * Odps odps = new Odps(account);
 * odps.setDefaultProject("my_project");
 *
 * Resources rs = odps.resources();
 * for (Resource r : rs) {
 *   ...
 * }
 *
 * FileInputStream in = new FileInputStream(new File("file_path"));
 * FileResource fr = new FileResource();
 * fr.setName("resource_name");
 * rs.create(fr, in);
 *
 * Resource r = rs.get("resource_name");
 * </pre>
 *
 * @author zhemin.nizm@alibaba-inc.com
 */
public class Resources implements Iterable<Resource> {

  /* for resource listing */
  @XmlRootElement(name = "Resources")
  private static class ListResourcesResponse {

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
    private Integer maxItems;

    @XmlElement(name = "Resource")
    private final List<ResourceModel> resources = new ArrayList<ResourceModel>();

  }

  private final RestClient client;
  private Odps odps;

  Resources(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  /**
   * 创建文件资源
   *
   * @param r
   *     {@link FileResource}类型对象
   * @param in
   *     上传资源的输入流
   * @throws OdpsException
   */
  public void create(FileResource r, InputStream in) throws OdpsException {
    create(getDefaultProjectName(), r, in);
  }

  /**
   * 创建文件资源
   *
   * @param projectName
   *     资源所在{@link Project}名称
   * @param r
   *     {@link Resource}类型对象
   * @param in
   *     上传资源的输入流
   * @throws OdpsException
   */
  public void create(String projectName, FileResource r, InputStream in)
      throws OdpsException {
    try {
      createFile(projectName, r, in, false);
    } catch (IOException e) {
      new OdpsException(e.getMessage(), e);
    }
  }

  /**
   * 创建表资源
   *
   * @param r
   *     {@link TableResource}类型对象
   * @throws OdpsException
   */
  public void create(TableResource r) throws OdpsException {
    create(getDefaultProjectName(), r);
  }

  /**
   * 创建表资源
   *
   * @param projectName
   *     资源所在{@link Project}名称
   * @param r
   *     {@link TableResource}类型对象
   * @throws OdpsException
   */
  public void create(String projectName, TableResource r) throws OdpsException {
    createTable(projectName, r, false);
  }

  /**
   * 创建 Volume 资源
   *
   * @param r
   *     {@link VolumeResource}类型对象
   * @throws OdpsException
   */
  public void create(VolumeResource r) throws OdpsException {
    create(getDefaultProjectName(), r);
  }

  /**
   * 创建 Volume 资源
   *
   * @param projectName
   * @param r
   *     {@link VolumeResource}类型对象
   * @throws OdpsException
   */
  public void create(String projectName, VolumeResource r) throws OdpsException {
    addVolumeResource(projectName, r, false);
  }

  /**
   * 更新 Volume 资源
   *
   * @param r
   *     {@link VolumeResource}类型对象
   * @throws OdpsException
   */
  public void update(VolumeResource r) throws OdpsException {
    update(getDefaultProjectName(), r);
  }

  /**
   * 更新 Volume 资源
   *
   * @param projectName
   * @param r
   *     {@link VolumeResource}类型对象
   * @throws OdpsException
   */
  public void update(String projectName, VolumeResource r) throws OdpsException {
    addVolumeResource(projectName, r, true);
  }


  private void addVolumeResource(String projectName, VolumeResource r, boolean isUpdate) throws OdpsException {
    RestClient client = odps.getRestClient();
    String method;
    String resource;
    if (isUpdate) {
      method = "PUT";
      resource = ResourceBuilder.buildResourceResource(projectName, r.getName());
    } else {
      method = "POST";
      resource = ResourceBuilder.buildResourcesResource(projectName);
    }

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.ODPS_RESOURCE_TYPE, r.model.type.toLowerCase());
    headers.put(Headers.ODPS_RESOURCE_NAME, r.getName());
    headers.put(Headers.ODPS_COPY_FILE_SOURCE, r.getVolumePath());

    if (r.getComment() != null) {
      headers.put(Headers.ODPS_COMMENT, r.getComment());
    }

    client.request(resource, method, null, headers, null);

  }

  /**
   * 更新表资源
   *
   * @param r
   *     {@link TableResource}类型对象
   * @throws OdpsException
   */
  public void update(TableResource r) throws OdpsException {
    update(getDefaultProjectName(), r);
  }

  /**
   * 更新表资源
   *
   * @param projectName
   *     资源所在{@link Project}
   * @param r
   *     {@link TableResource}类型对象
   * @throws OdpsException
   */
  public void update(String projectName, TableResource r) throws OdpsException {
    createTable(projectName, r, true);
  }

  /**
   * 更新文件资源
   *
   * @param r
   *     {@link FileResource}类型对象
   * @param in
   *     文件输入流
   * @throws OdpsException
   */
  public void update(FileResource r, InputStream in) throws OdpsException {
    update(getDefaultProjectName(), r, in);
  }

  /**
   * 更新文件资源
   *
   * @param projectName
   *     资源所在{@link Project}名称
   * @param r
   *     {@link FileResource}类型对象
   * @param in
   *     文件输入流
   * @throws OdpsException
   */
  public void update(String projectName, FileResource r, InputStream in)
      throws OdpsException {
    try {
      createFile(projectName, r, in, true);
    } catch (IOException e) {
      new OdpsException(e.getMessage(), e);
    }
  }

  /**
   * 获得资源文件的字节流
   *
   * @param projectName
   *     资源所在{@link Project}名称
   * @param name
   *     资源名称
   * @throws OdpsException
   */
  public InputStream getResourceAsStream(String projectName, String name) throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(projectName).append("/resources")
        .append("/" + name);

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/octet-stream");
    Response response = client.request(resource.toString(), "GET", null, headers, null);
    return new ByteArrayInputStream(response.getBody());
  }

  /**
   * 获得资源文件的字节流
   *
   * @param name
   *     资源名称
   * @throws OdpsException
   */
  public InputStream getResourceAsStream(String name) throws OdpsException {
    return getResourceAsStream(getDefaultProjectName(), name);
  }

  private void createTable(String project, TableResource r, Boolean overwrite)
      throws OdpsException {
    if (r.getName() == null || r.getName().length() == 0) {
      throw new OdpsException("Table Resource Name should not empty.");
    }

    String resource = null;
    String method;
    if (overwrite) {
      method = "PUT";
      resource = ResourceBuilder.buildResourceResource(project, r.getName());
    } else {
      method = "POST";
      resource = ResourceBuilder.buildResourcesResource(project);
    }

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "text/plain");
    headers.put(Headers.ODPS_RESOURCE_TYPE, r.getType().toString()
        .toLowerCase());
    headers.put(Headers.ODPS_RESOURCE_NAME, r.getName());
    headers.put(Headers.ODPS_COPY_TABLE_SOURCE,
                r.getSourceTableName());
    if (r.getComment() != null) {
      headers.put(Headers.ODPS_COMMENT, r.getComment());
    }

    client.request(resource, method, null, headers, null);
  }

  private void createFile(String project, Resource res, InputStream in,
                          Boolean overwrite) throws OdpsException, IOException {

    FileResource r = (FileResource) res;

    if (r.getName() == null || r.getName().length() == 0) {
      throw new OdpsException("File Resource Name should not empty.");
    }

    String resource = null;
    String method;
    if (overwrite) {
      method = "PUT";
      resource = ResourceBuilder.buildResourceResource(project, r.getName());
    } else {
      method = "POST";
      resource = ResourceBuilder.buildResourcesResource(project);
    }

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/octet-stream");
    headers.put(Headers.CONTENT_DISPOSITION,
                "attachment;filename=" + r.getName());
    headers.put(Headers.ODPS_RESOURCE_TYPE, r.getType().toString()
        .toLowerCase());
    headers.put(Headers.ODPS_RESOURCE_NAME, r.getName());
    if (r.getComment() != null) {
      headers.put(Headers.ODPS_COMMENT, r.getComment());
    }

    if (r.getIsTempResource()) {
      headers.put(Headers.ODPS_RESOURCE_IS_TEMP, String.valueOf(r.getIsTempResource()));
    }

    client.request(resource.toString(), method, null, headers,
                   in, IOUtils.getInputStreamLength(in));

  }

  /**
   * 获取资源信息
   *
   * @param name
   *     资源名称
   * @return {@link Resource}对象
   * @throws OdpsException
   */
  public Resource get(String name) {
    String[] tmpPair = name.split("/resources/");
    if (tmpPair.length > 1) {
      String project = tmpPair[0];
      String resourceName = tmpPair[1];
      return get(project, resourceName);
    } else {
      return get(getDefaultProjectName(), name);
    }
  }

  /**
   * 获取资源信息
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param name
   *     资源名称
   * @return {@link Resource}对象
   * @throws OdpsException
   */
  public Resource get(String projectName, String name) {
    ResourceModel rm = new ResourceModel();
    rm.name = name;
    return Resource.getResource(rm, projectName, odps);
  }

  /**
   * 判断指定资源是否存在
   *
   * @param resourceName
   *     资源名称
   * @return 如果指定资源存在，则返回true，否则返回false
   * @throws OdpsException
   */
  public boolean exists(String resourceName) throws OdpsException {
    return exists(getDefaultProjectName(), resourceName);
  }

  /**
   * 判断指定资源是否存在
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param resourceName
   *     资源名称
   * @return 如果指定资源存在，则返回true，否则返回false
   * @throws OdpsException
   */
  public boolean exists(String projectName, String resourceName)
      throws OdpsException {
    try {
      Resource t = get(projectName, resourceName);
      t.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    } catch (ReloadException e) {
      if (e.getCause() instanceof NoSuchObjectException) {
        return false;
      }
      throw e;
    }
  }

  /**
   * 删除资源
   *
   * @param name
   *     资源名称
   */
  public void delete(String name) throws OdpsException {
    delete(getDefaultProjectName(), name);
  }

  /**
   * 删除资源
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param name
   *     资源名称
   */
  public void delete(String projectName, String name) throws OdpsException {
    String resource = ResourceBuilder.buildResourceResource(projectName, name);
    client.request(resource, "DELETE", null, null, null);
  }

  /**
   * 上传临时文件资源
   *
   * @param fileName
   *     本地文件名
   * @return {@link Resource}对象
   * @throws OdpsException
   */
  public FileResource createTempResource(String fileName) throws OdpsException {
    return createTempResource(getDefaultProjectName(), fileName);
  }

  /**
   * 上传临时文件资源
   *
   * @param fileName
   *     本地文件名
   * @param projectName
   *     所在{@link Project}名称
   * @return {@link Resource}对象
   * @throws OdpsException
   */
  public FileResource createTempResource(String projectName, String fileName) throws OdpsException {
    return createTempResource(projectName, fileName, Resource.Type.FILE);
  }

  /**
   * @param projectName
   *     本地文件名
   * @param fileName
   *     所在{@link Project}名称
   * @param type
   *     资源类型
   * @return {@link Resource}对象
   * @throws OdpsException
   */
  public FileResource createTempResource(String projectName, String fileName, Resource.Type type)
      throws OdpsException {
    File tempFile = null;
    tempFile = new File(fileName);

    if (!tempFile.exists()) {
      throw new OdpsException("File or Directory '" + fileName + "' does not exist.");
    }
    if (tempFile.isDirectory()) {
      throw new OdpsException("Temp resource should be file, not directory:" + fileName);
    }
    Resource resourceTmp = Resource.createResource(type);
    if (!(resourceTmp instanceof FileResource)) {
      throw new OdpsException("Invalid temp resource type :" + String.valueOf(type) + ".");
    }
    FileResource resource = (FileResource) resourceTmp;
    resource.setIsTempResource(true);
    String resourceName = UUID.randomUUID().toString() + "_" + tempFile.getName();
    resource.setName(resourceName);
    FileInputStream input;
    try {
      input = new FileInputStream(tempFile);
    } catch (FileNotFoundException e) {
      throw new OdpsException("File or Directory '" + fileName + "' does not exist.");
    }

    create(projectName, resource, input);
    return (FileResource) get(projectName, resourceName);
  }

  /**
   * 获得资源迭代器
   *
   * @param projectName
   *     所在{@link Project}名称
   * @return {@link Resource}迭代器
   */
  public Iterator<Resource> iterator(final String projectName) {
    return new ResourceListIterator(projectName, null);
  }

  /**
   * 获得资源迭代器
   *
   * @return {@link Resource}迭代器
   */
  @Override
  public Iterator<Resource> iterator() {
    return iterator(getDefaultProjectName());
  }


  /**
   * 获得资源 iterable 迭代器
   *
   * @param projectName
   *     所在{@link Project}名称
   * @return {@link Resource} iterable 迭代器
   */
  public Iterable<Resource> iterable(final String projectName) {
    return new Iterable<Resource>() {
      @Override
      public Iterator<Resource> iterator() {
        return new ResourceListIterator(projectName, null);
      }
    };
  }

  /**
   * 获得资源 iterable 迭代器
   *
   * @return {@link Resource} iterable 迭代器
   */
  public Iterable<Resource> iterable() {
    return iterable(getDefaultProjectName());
  }

  private class ResourceListIterator extends ListIterator<Resource> {

    Map<String, String> params = new HashMap<String, String>();
    String name;
    String project;

    ResourceListIterator(String projectName, String resourceName) {
      this.project = projectName;
      this.name = resourceName;
    }

    @Override
    protected List<Resource> list() {
      ArrayList<Resource> resources = new ArrayList<Resource>();

      params.put("expectmarker", "true"); // since sprint-11

      String lastMarker = params.get("marker");
      if (params.containsKey("marker") && lastMarker.length() == 0) {
        return null;
      }

      if (name != null) {
        params.put("name", name);
      }

      String resource = ResourceBuilder.buildResourcesResource(project);
      try {
        ListResourcesResponse resp = client.request(
            ListResourcesResponse.class, resource, "GET", params);

        for (ResourceModel model : resp.resources) {
          Resource t = Resource.getResource(model, project, odps);
          resources.add(t);
        }

        params.put("marker", resp.marker);
      } catch (OdpsException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

      return resources;
    }
  }

  private String getDefaultProjectName() {
    String project = client.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }

}
