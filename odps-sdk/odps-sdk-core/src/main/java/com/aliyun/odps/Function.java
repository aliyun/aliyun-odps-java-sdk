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

import com.aliyun.odps.commons.transport.Params;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.utils.TagUtils;
import com.aliyun.odps.utils.TagUtils.OBJECT_TYPE;
import com.aliyun.odps.utils.TagUtils.OPERATION_TYPE;
import com.aliyun.odps.utils.TagUtils.ObjectRef;
import com.aliyun.odps.utils.TagUtils.ObjectTagInfo;
import com.aliyun.odps.utils.TagUtils.SetObjectTagInput;
import com.aliyun.odps.utils.TagUtils.SimpleTag;
import com.aliyun.odps.utils.TagUtils.TagRef;
import com.aliyun.odps.utils.StringUtils;

/**
 * Function表示ODPS中的函数
 *
 * @author zhemin.nizm@alibaba-inc.com
 */
public class Function extends LazyLoad {

  /**
   * Function model
   */
  @Root(name = "Function", strict = false)
  static class FunctionModel {

    @Element(name = "SchemaName", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String schemaName;

    @Element(name = "Alias", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createdTime;

    @Element(name = "ClassType", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String classType;

    @ElementList(name = "Resources", entry = "ResourceName", required = false)
    ArrayList<String> resources;

    @Element(name = "IsSqlFunction", required = false)
    Boolean isSqlFunction;

    @Element(name = "SqlDefinitionText", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String sqlDefinitionText;
  }

  FunctionModel model;
  private ObjectTagInfo functionTagInfo;
  String project;
  RestClient client;
  Odps odps;

  public Function() {
    // TODO: replace with a static builder
    this.model = new FunctionModel();
  }

  /* package */
  Function(FunctionModel functionModel, String project, Odps odps) {
    this.model = functionModel;
    this.project = project;
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  /**
   * 获得函数名
   *
   * @return 函数名
   */
  public String getName() {
    if (model.name == null && client != null) {
      lazyLoad();
    }
    return model.name;
  }

  /**
   * 设置函数名
   *
   * @param name
   *     函数名
   */
  public void setName(String name) {
    // TODO: replace with a static builder
    model.name = name;
  }

  /**
   * 获得函数所属用户
   *
   * @return 用户名
   */
  public String getOwner() {
    if (model.owner == null && client != null) {
      lazyLoad();
    }
    return model.owner;
  }

  /**
   * 获取函数创建时间
   *
   * @return 函数创建时间
   */
  public Date getCreatedTime() {
    if (model.createdTime == null) {
      lazyLoad();
    }
    return model.createdTime;
  }


  /**
   * 使用getClassPath替代
   */
  @Deprecated
  public String getClassType() {
    if (model.classType == null && client != null) {
      lazyLoad();
    }
    return model.classType;
  }

  /**
   * 获得函数使用的类名
   *
   * @return 函数使用的类名
   */
  public String getClassPath() {
    if (model.classType == null && client != null) {
      lazyLoad();
    }
    return model.classType;
  }

  /**
   * 建议使用setClassPath替代
   */
  @Deprecated
  public void setClassType(String classType) {
    // TODO: replace with a static builder
    model.classType = classType;
  }

  /**
   * 设置函数使用的类名
   *
   * @param classPath
   *     函数使用的类名
   */
  public void setClassPath(String classPath) {
    // TODO: replace with a static builder
    model.classType = classPath;
  }

  /**
   * 获得函数相关的资源列表。UDF所用到的资源列表，这个里面必须包括UDF代码所在的资源。如果用户UDF中需要读取其他资源文件，这个列表中还得包括UDF所读取的资源文件列表。
   *
   * @return 资源列表
   */
  public List<Resource> getResources() {
    if (model.resources == null && client != null) {
      lazyLoad();
    }

    /* copy */
    ArrayList<Resource> resources = new ArrayList<>();
    if (model.resources != null) {
      for (String resourceFullName : model.resources) {
        resources.add(Resource.from(project, resourceFullName, odps));
      }
    }

    return resources;
  }

  /**
   * 解析UDF所用到的资源的名字列表, 资源名字的格式为 <project_name>/resources/<resource_name>
   *
   * @param resources
   *     function 对应资源的名字列表
   * @return 资源名称与其所属 project 对: Map<resource_name, project_name>
   */
  @Deprecated
  private Map<String, String> parseResourcesName(List<String> resources) {
    Map<String, String> resourceMap = new HashMap<>();

    for (String r : resources) {
      String[] splits = r.split("/resources/");
      String resourceProject = null;
      String resourceName = null;

      if (splits.length > 1) {
        resourceProject = splits[0];
        resourceName = splits[1];
      } else {
        resourceProject = this.project;
        resourceName = r;
      }

      resourceMap.put(resourceName, resourceProject);
    }
    return resourceMap;
  }

  /**
   * This method is deprecated. See {@link Function#getResourceFullNames()}
   */
  @Deprecated
  public List<String> getResourceNames() {
    if (model.resources == null && client != null) {
      lazyLoad();
    }
    List<String> resourceNames = new ArrayList<>();

    if (model.resources != null) {
      Map<String, String> resources = parseResourcesName(model.resources);

      for (Map.Entry<String, String> entry : resources.entrySet()) {
        if (entry.getValue().equals(this.project)) {
          resourceNames.add(entry.getKey());
        } else {
          resourceNames.add(entry.getValue() + "/" + entry.getKey());
        }
      }
    }
    return resourceNames;
  }

  /**
   * Get the full names of resources that this function depends on. This method lazy loads the
   * function information, and a {@link ReloadException} will be thrown if lazy loading failed.
   *
   * @return Full names. Possible patters:
   * <project_name>/resources/<resource_name>
   * <project_name>/schemas/<schema_name>/resources/<resource_name>
   */
  public List<String> getResourceFullNames() {
    lazyLoad();
    return model.resources;
  }

  /**
   * 设置函数依赖的相关资源
   *
   * @param resources
   *     资源列表
   */
  public void setResources(List<String> resources) {
    // TODO: replace with a static builder
    model.resources = new ArrayList<>();
    model.resources.addAll(resources);
  }

  /**
   * 获取函数所在{@link Project}名称
   *
   * @return Project名称
   */
  public String getProject() {
    return project;
  }

  public String getSchemaName() {
    lazyLoad();
    return model.schemaName;
  }

  /**
   * Check if this function is a SQL function.
   *
   * @return if this is a SQL function, return true, else false.
   */
  public boolean isSqlFunction() {
    lazyLoad();

    // Could be null after reload since the xml of non-SQL functions doesn't has tag 'IsSqlFunction'
    if (model.isSqlFunction == null) {
      model.isSqlFunction = false;
    }
    return model.isSqlFunction;
  }

  /**
   * Get the SQL definition text of this function.
   *
   * @return if this is a SQL function, return the SQL definition text, else null.
   */
  public String getSqlDefinitionText() {
    lazyLoad();

    if (isSqlFunction()) {
      return model.sqlDefinitionText;
    } else {
      return null;
    }
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildFunctionResource(project, model.name);
    Map<String, String> params = new HashMap<>();
    if (!StringUtils.isNullOrEmpty(model.schemaName)) {
      params.put(Params.ODPS_SCHEMA_NAME, model.schemaName);
    }
    model = client.request(FunctionModel.class, resource, "GET", params);
    setLoaded(true);
  }

  private void reloadTagInfo() {
    String resource = ResourceBuilder.buildFunctionResource(project, model.name);
    // Convert the OdpsException to a ReloadException the keep the consistency of the getter's
    // method signature.
    try {
      functionTagInfo = TagUtils.getObjectTagInfo(resource, null, client);
    } catch (OdpsException e) {
      throw new ReloadException(e);
    }
  }

  public void updateOwner(String newOwner) throws OdpsException {
    String method = "PUT";
    String resource = ResourceBuilder.buildFunctionResource(project, model.name);
    HashMap<String, String> params = new HashMap<>();
    params.put("updateowner", null);
    HashMap<String, String> headers = new HashMap<>();
    headers.put(Headers.ODPS_OWNER, newOwner);
    client.request(resource, method, params, headers, null);
    this.model.owner = newOwner;
  }

  /**
   * Get {@link Tag}(s) attached to this function.
   * @return list of {@link Tag}
   */
  public List<Tag> getTags() {
    reloadTagInfo();
    return TagUtils.getTags(functionTagInfo, odps);
  }

  /**
   * Get simple tags attached to this function.
   * @return a map from category to key value pairs
   */
  public Map<String, Map<String, String>> getSimpleTags() {
    reloadTagInfo();
    return TagUtils.getSimpleTags(functionTagInfo);
  }

  /**
   * Attach a {@link Tag} to this function. The function and tag should be in a same project.
   *
   * @param tag tag to attach
   */
  public void addTag(Tag tag) throws OdpsException {
    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.FUNCTION,
        project,
        model.name,
        null);
    TagRef tagRef = new TagRef(tag.getClassification(), tag.getName());
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.SET, objectRef, tagRef, null);

    TagUtils.updateTagInternal(setObjectTagInput, null, client);
  }

  /**
   * Attach a simple tag to this function. A simple tag is a triad consisted of category, tag key,
   * and tag value.
   *
   * @param category simple tag category, could be nul.
   * @param key simple tag key, cannot be null.
   * @param value simple tag value, cannot be null.
   */
  public void addSimpleTag(
      String category,
      String key,
      String value) throws OdpsException {
    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.FUNCTION,
        project,
        model.name,
        null);
    SimpleTag simpleTag = new SimpleTag(category, Collections.singletonMap(key, value));
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.SET, objectRef, null, simpleTag);

    TagUtils.updateTagInternal(setObjectTagInput, null, client);
  }

  /**
   * Remove a {@link Tag}.
   *
   * @param tag tag to remove.
   */
  public void removeTag(Tag tag) throws OdpsException {

    Objects.requireNonNull(tag);

    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.FUNCTION,
        project,
        model.name,
        null);
    TagRef tagRef = new TagRef(tag.getClassification(), tag.getName());
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.UNSET, objectRef, tagRef, null);

    TagUtils.updateTagInternal(setObjectTagInput, null, client);
  }

  /**
   * Remove a simple tag. A simple tag is a triad consisted of category, tag key, and
   * tag value.
   *
   * @param category category.
   * @param key key.
   * @param value value.
   * @throws OdpsException
   */
  public void removeSimpleTag(
      String category,
      String key,
      String value) throws OdpsException {

    Objects.requireNonNull(category);
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);

    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.FUNCTION,
        project,
        model.name,
        null);
    SimpleTag simpleTag = new SimpleTag(category, Collections.singletonMap(key, value));
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.UNSET, objectRef, null, simpleTag);
    TagUtils.updateTagInternal(setObjectTagInput, null, client);
  }
}
