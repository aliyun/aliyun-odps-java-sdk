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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;

import com.aliyun.odps.Resource.ResourceModel;
import com.aliyun.odps.Resource.Type;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Params;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.utils.NameSpaceSchemaUtils;
import com.aliyun.odps.utils.StringUtils;

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
  @Root(name = "Resources", strict = false)
  private static class ListResourcesResponse {

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;

    @ElementList(entry = "Resource", inline = true, required = false)
    private List<ResourceModel> resources = new ArrayList<ResourceModel>();

  }

  private final RestClient client;
  private Odps odps;
  private int chunkSize;

  Resources(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
    this.chunkSize = 64 << 20;
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
    create(projectName, odps.getCurrentSchema(), r, in);
  }

  /**
   * Create a file resource.
   *
   * @param project Project name.
   * @param schema Schema name. Null or empty string means using the default schema.
   * @param resource File resource.
   * @param in Input stream of the content.
   */
  public void create(
      String project,
      String schema,
      FileResource resource,
      InputStream in) throws OdpsException {
    try {
      createFileResource(project, schema, resource, in, false);
    } catch (IOException e) {
      throw new OdpsException(e);
    }
  }

  /**
   * 创建表资源
   *
   * @param resource
   *     {@link TableResource}类型对象
   * @throws OdpsException
   */
  public void create(TableResource resource) throws OdpsException {
    create(getDefaultProjectName(), resource);
  }

  /**
   * 创建表资源
   *
   * @param projectName
   *     资源所在{@link Project}名称
   * @param resource
   *     {@link TableResource}类型对象
   * @throws OdpsException
   */
  public void create(String projectName, TableResource resource) throws OdpsException {
    create(projectName, odps.getCurrentSchema(), resource);
  }

  /**
   * Create a table resource.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param resource Table resource.
   * @throws OdpsException
   */
  public void create(
      String projectName,
      String schemaName,
      TableResource resource) throws OdpsException {
    createTableResource(projectName, schemaName, resource, false);
  }

  /**
   * 创建 Volume 资源
   *
   * @param resource
   *     {@link VolumeResource}类型对象
   * @throws OdpsException
   */
  public void create(VolumeResource resource) throws OdpsException {
    create(getDefaultProjectName(), resource);
  }

  /**
   * 创建 Volume 资源
   *
   * @param projectName
   * @param resource
   *     {@link VolumeResource}类型对象
   * @throws OdpsException
   */
  public void create(String projectName, VolumeResource resource) throws OdpsException {
    addVolumeResource(projectName, resource, false);
  }

  /**
   * 更新 Volume 资源
   *
   * @param resource
   *     {@link VolumeResource}类型对象
   * @throws OdpsException
   */
  public void update(VolumeResource resource) throws OdpsException {
    update(getDefaultProjectName(), resource);
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


  private void addVolumeResource(
      String projectName,
      VolumeResource r,
      boolean isUpdate) throws OdpsException {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }
    if (r == null) {
      throw new IllegalArgumentException("Argument 'resource' cannot be null or empty");
    }

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

    HashMap<String, String> headers = new HashMap<>();
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
    update(projectName, odps.getCurrentSchema(), r);
  }

  /**
   * Update designated table resource.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param resource Table resource.
   */
  public void update(
      String projectName,
      String schemaName,
      TableResource resource) throws OdpsException {
    createTableResource(projectName, schemaName, resource, true);
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
    update(projectName, odps.getCurrentSchema(), r, in);
  }

  /**
   * Update designated file resource.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param resource File resource.
   * @param in Input stream of the content.
   * @throws OdpsException
   */
  public void update(
      String projectName,
      String schemaName,
      FileResource resource,
      InputStream in) throws OdpsException {
    try {
      createFileResource(projectName, schemaName, resource, in, true);
    } catch (IOException e) {
      throw new OdpsException(e);
    }
  }

  /**
   * 获得资源文件的字节流
   *
   * @param resourceName
   *     资源名称
   * @throws OdpsException
   */
  public InputStream getResourceAsStream(String resourceName) throws OdpsException {
    return getResourceAsStream(getDefaultProjectName(), resourceName);
  }

  /**
   * 获得资源文件的字节流
   *
   * @param projectName
   *     资源所在{@link Project}名称
   * @param resourceName
   *     资源名称
   * @throws OdpsException
   */
  public InputStream getResourceAsStream(
      String projectName,
      String resourceName) throws OdpsException {
    return getResourceAsStream(projectName, odps.getCurrentSchema(), resourceName);
  }

  public InputStream getResourceAsStream(
      String projectName,
      String schemaName,
      String resourceName) throws OdpsException {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }
    if (StringUtils.isNullOrEmpty(resourceName)) {
      throw new IllegalArgumentException("Argument 'resourceName' cannot be null or empty");
    }

    return new ResourceInputStream(client, projectName, schemaName, resourceName);
  }

  private void createTableResource(
      String project,
      String schema,
      TableResource r,
      boolean overwrite) throws OdpsException {

    if (StringUtils.isNullOrEmpty(project)) {
      throw new IllegalArgumentException("Argument 'project' cannot be null or empty");
    }
    if (r == null) {
      throw new IllegalArgumentException("Argument 'resource' cannot be null");
    }
    if (r.getName() == null || r.getName().length() == 0) {
      throw new OdpsException("Table Resource Name should not empty.");
    }

    String resource;
    String method;
    if (overwrite) {
      method = "PUT";
      resource = ResourceBuilder.buildResourceResource(project, r.getName());
    } else {
      method = "POST";
      resource = ResourceBuilder.buildResourcesResource(project);
    }

    Map<String, String> params = NameSpaceSchemaUtils.initParamsWithSchema(schema);

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

    client.request(resource, method, params, headers, null);
  }

  private void createFileResource(
      String project,
      String schema,
      FileResource r,
      InputStream in,
      boolean overwrite) throws OdpsException, IOException {

    if (StringUtils.isNullOrEmpty(project)) {
      throw new IllegalArgumentException("Argument 'project' cannot be null or empty");
    }
    if (r == null) {
      throw new IllegalArgumentException("Argument 'resource' cannot be null");
    }
    if (StringUtils.isNullOrEmpty(r.getName())) {
      throw new OdpsException("Resource Name should not empty.");
    }
    if (in == null) {
      throw new OdpsException("Argument 'in' cannot be null");
    }

    MessageDigest digest = DigestUtils.getMd5Digest();
    byte[] tmpContent = new byte[chunkSize];
    long totalBytes = 0L;
    int readSize;
    int cnt = 0;
    List<String> tmpFiles = new ArrayList<>();
    while ((readSize = in.read(tmpContent)) != -1) {
      digest.update(tmpContent, 0, readSize);
      InputStream input = new ByteArrayInputStream(tmpContent, 0, readSize);
      FileResource tmp = new FileResource();
      String tmpName;

      // Using a deterministic temp resource name instead of a random UUID has 2 benefits:
      // 1. Human readable, much easier to debug
      // 2. Temp resources that has been uploaded could be reused if the MD5 matches.
      if (NameSpaceSchemaUtils.isSchemaEnabled(schema)) {
        tmpName = String.format("%s.%s.part.tmp.%06d", schema, r.getName(), cnt);
      } else {
        tmpName = String.format("%s.part.tmp.%06d", r.getName(), cnt);
      }

      tmp.setIsTempResource(true);
      tmp.setName(tmpName);
      tmpFiles.add(tmpName);
      createTempPartFile(project, schema, tmp, input);

      cnt++;
      totalBytes += readSize;
      input.close();
    }

    byte[] md5Bytes = digest.digest();
    String commitContent = toHexString(md5Bytes) + "|" + String.join(",", tmpFiles);
    InputStream is = new ByteArrayInputStream(commitContent.getBytes(StandardCharsets.UTF_8));
    FileResource f = new FileResource();
    f.setName(r.getName());
    f.setComment(r.getComment());
    f.setIsTempResource(r.getIsTempResource());
    f.model.type = r.getType().toString();
    mergeTempPartFiles(project, schema, f, is, overwrite, totalBytes);
    is.close();
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
   * @param resourceName
   *     资源名称
   * @return {@link Resource}对象
   * @throws OdpsException
   */
  public Resource get(String projectName, String resourceName) {
    return get(projectName, odps.getCurrentSchema(), resourceName);
  }

  /**
   * Get designated file resource.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param resourceName Resource name.
   * @return {@link Resource}
   */
  public Resource get(
      String projectName,
      String schemaName,
      String resourceName) {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }
    if (StringUtils.isNullOrEmpty(resourceName)) {
      throw new IllegalArgumentException("Argument 'resourceName' cannot be null");
    }

    ResourceModel rm = new ResourceModel();
    rm.name = resourceName;
    rm.schemaName = schemaName;
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
    return exists(projectName, odps.getCurrentSchema(), resourceName);
  }

  /**
   * Check if designated resource exists.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param resourceName Resource name.
   * @return True if the resource exists, else false.
   * @throws OdpsException
   */
  public boolean exists(
      String projectName,
      String schemaName,
      String resourceName) throws OdpsException {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }
    if (StringUtils.isNullOrEmpty(resourceName)) {
      throw new IllegalArgumentException("Argument 'resourceName' cannot be null or empty");
    }

    try {
      Resource t = get(projectName, schemaName, resourceName);
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
   * @param resourceName
   *     资源名称
   */
  public void delete(String resourceName) throws OdpsException {
    delete(getDefaultProjectName(), resourceName);
  }

  /**
   * 删除资源
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param resourceName
   *     资源名称
   */
  public void delete(String projectName, String resourceName) throws OdpsException {
    delete(projectName, odps.getCurrentSchema(), resourceName);
  }

  /**
   * Delete designated resource.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param resourceName Resource name.
   * @throws OdpsException
   */
  public void delete(
      String projectName,
      String schemaName,
      String resourceName) throws OdpsException {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }
    if (StringUtils.isNullOrEmpty(resourceName)) {
      throw new IllegalArgumentException("Argument 'resourceName' cannot be null or empty");
    }

    String resource = ResourceBuilder.buildResourceResource(projectName, resourceName);
    Map<String, String> params = NameSpaceSchemaUtils.initParamsWithSchema(schemaName);
    client.request(resource, "DELETE", params, null, null);
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
   * Create a temporary file resource.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param filePath Local file path.
   * @param type Resource type. Could be {@link Resource.Type#FILE}, {@link Resource.Type#ARCHIVE},
   *             {@link Resource.Type#PY} and {@link Resource.Type#JAR}.
   * @return {@link FileResource}
   */
  public FileResource createTempFileResource(
      String projectName,
      String schemaName,
      String filePath,
      Type type) throws OdpsException {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }
    if (StringUtils.isNullOrEmpty(filePath)) {
      throw new IllegalArgumentException("Argument 'filePath' cannot be null or empty");
    }
    if (type == null) {
      throw new IllegalArgumentException("Argument 'type' cannot be null");
    }

    File tempFile = new File(filePath);
    if (!tempFile.exists()) {
      throw new IllegalArgumentException("File or Directory '" + filePath + "' does not exist.");
    }
    if (tempFile.isDirectory()) {
      throw new IllegalArgumentException("Temp resource should be file, not directory:" + filePath);
    }

    Resource resource = Resource.createResource(type);
    if (!(resource instanceof FileResource)) {
      throw new IllegalArgumentException("Unsupported resource type: " + type.toString());
    }

    ((FileResource) resource).setIsTempResource(true);
    String resourceName = UUID.randomUUID().toString() + "_" + tempFile.getName();
    resource.setName(resourceName);
    FileInputStream input;
    try {
      input = new FileInputStream(tempFile);
    } catch (FileNotFoundException e) {
      throw new OdpsException("File or Directory '" + filePath + "' does not exist.");
    }

    create(projectName, schemaName, (FileResource) resource, input);
    IOUtils.closeSilently(input);
    return (FileResource) get(projectName, resourceName);
  }

  /**
   * @param projectName
   *     本地文件名
   * @param filePath
   *     所在{@link Project}名称
   * @param type
   *     资源类型
   * @return {@link Resource}对象
   * @throws OdpsException
   */
  public FileResource createTempResource(String projectName, String filePath, Type type)
      throws OdpsException {
    return createTempFileResource(projectName, odps.getCurrentSchema(), filePath, type);
  }

  private String toHexString(byte[] bytes) {
    String md5 = new java.math.BigInteger(1, bytes).toString(16);
    return new String(new char[32 - md5.length()]).replace("\0", "0") + md5;
  }

  private void createTempPartFile(
      String project,
      String schema,
      Resource res,
      InputStream in) throws OdpsException, IOException {

    FileResource r = (FileResource) res;
    if (r.getName() == null || r.getName().length() == 0) {
      throw new OdpsException("Temp Part Resource Name should not empty.");
    }

    if (!r.getIsTempResource()) {
      throw new OdpsException("Part Resource must be Temp Resource.");
    }

    if (r.getType().equals(Resource.Type.VOLUMEFILE) ||
        r.getType().equals(Resource.Type.VOLUMEARCHIVE) ||
        r.getType().equals(Resource.Type.TABLE) ||
        r.getType().equals(Resource.Type.UNKOWN)) {
      throw new OdpsException("Temp Part Resource's type is invalid!");
    }

    String resource = ResourceBuilder.buildResourcesResource(project);
    String method = "POST";

    HashMap<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/octet-stream");
    headers.put(Headers.CONTENT_DISPOSITION,
                "attachment;filename=" + r.getName());
    headers.put(Headers.ODPS_RESOURCE_TYPE, r.getType().toString()
        .toLowerCase());
    headers.put(Headers.ODPS_RESOURCE_NAME, r.getName());
    if (r.getComment() != null) {
      headers.put(Headers.ODPS_COMMENT, r.getComment());
    }
    headers.put(Headers.ODPS_RESOURCE_IS_TEMP, String.valueOf(r.getIsTempResource()));

    HashMap<String, String> params = NameSpaceSchemaUtils.initParamsWithSchema(schema);
    params.put(Params.ODPS_RESOURCE_IS_PART, "true");

    client.request(resource, method, params, headers, in, IOUtils.getInputStreamLength(in));
  }

  private void mergeTempPartFiles(
      String project,
      String schema,
      Resource res,
      InputStream in,
      boolean overwrite,
      long totalBytes) throws OdpsException, IOException {

    InputStream inputStream = Objects.requireNonNull(in);
    FileResource r = (FileResource) res;

    if (r.getName() == null || r.getName().length() == 0) {
      throw new OdpsException("File Resource Name should not empty.");
    }

    String resource;
    String method;
    if (overwrite) {
      method = "PUT";
      resource = ResourceBuilder.buildResourceResource(project, r.getName());
    } else {
      method = "POST";
      resource = ResourceBuilder.buildResourcesResource(project);
    }

    HashMap<String, String> headers = new HashMap<>();
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
    headers.put(Headers.ODPS_RESOURCE_MERGE_TOTAL_BYTES, String.valueOf(totalBytes));

    HashMap<String, String> params = NameSpaceSchemaUtils.initParamsWithSchema(schema);
    params.put(Params.ODPS_RESOURCE_OP_MERGE, "true");

    odps.getRestClient().request(resource, method, params, headers,
                                 inputStream, IOUtils.getInputStreamLength(inputStream));
  }

  /**
   * 获得资源迭代器
   *
   * @param projectName
   *     所在{@link Project}名称
   * @return {@link Resource}迭代器
   */
  public Iterator<Resource> iterator(final String projectName) {
    return new ResourceListIterator(projectName, odps.getCurrentSchema(), null);
  }

  /**
   * Get a resource iterator of the given schema in the given project.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @return A resource iterator.
   */
  public Iterator<Resource> iterator(final String projectName, String schemaName) {
    return iterator(projectName, schemaName, null);
  }

  /**
   * Get a resource iterator of the given schema in the given project.
   *
   * @param projectName  Project name.
   * @param schemaName   Schema name. Null or empty string means using the default schema.
   * @param resourceName the resource name to filter.
   *                     The resource name of the result will contain this string, not start with
   * @return A resource iterator.
   */
  public Iterator<Resource> iterator(final String projectName, String schemaName,
                                     String resourceName) {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }

    return new ResourceListIterator(projectName, schemaName, resourceName);
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
    return () -> new ResourceListIterator(projectName, odps.getCurrentSchema(), null);
  }

  /**
   * 获得资源 iterable 迭代器
   *
   * @return {@link Resource} iterable 迭代器
   */
  public Iterable<Resource> iterable() {
    return iterable(getDefaultProjectName());
  }

  /**
   * Get a resource iterable of the given schema in the given project.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @return A resource iterable.
   */
  public Iterable<Resource> iterable(final String projectName, String schemaName) {
   return iterable(projectName, schemaName, null);
  }


  /**
   * Get a resource iterable of the given schema in the given project.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param resourceName the resource name to filter
   * @return A resource iterable.
   */
  public Iterable<Resource> iterable(final String projectName, String schemaName, String resourceName) {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }

    return () -> new ResourceListIterator(projectName, schemaName, resourceName);
  }

  private class ResourceListIterator extends ListIterator<Resource> {

    Map<String, String> params = new HashMap<>();
    String name;
    String project;
    String schemaName;

    ResourceListIterator(String projectName, String schemaName, String resourceName) {
      this.project = projectName;
      this.schemaName = schemaName;
      this.name = resourceName;
      params = NameSpaceSchemaUtils.initParamsWithSchema(schemaName);
    }

    @Override
    public List<Resource> list(String marker, long maxItems) {
      if (marker != null) {
        params.put("marker", marker);
      }
      if (maxItems >= 0) {
        params.put("maxitems", String.valueOf(maxItems));
      }
      return list();
    }

    @Override
    public String getMarker() {
      return params.get("marker");
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
          model.schemaName = schemaName;
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

  protected void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }
}
