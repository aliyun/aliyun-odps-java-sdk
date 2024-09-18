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

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Attribute;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.aliyun.odps.Instance.InstanceResultModel;
import com.aliyun.odps.Instance.InstanceResultModel.TaskResult;
import com.aliyun.odps.Instance.TaskStatusModel;
import com.aliyun.odps.XFlow.XFlowModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;

public class XFlows implements Iterable<XFlow> {

  @Root(name = "xflows", strict = false)
  private static class ListXFlowsResponse {

    @ElementList(entry = "xflow", inline = true, required = false)
    private List<XFlowModel> xFlows = new ArrayList<XFlowModel>();

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;
  }

  private Odps odps;

  XFlows(Odps odps) {
    this.odps = odps;
  }

  /**
   * 获取 XFlow 对象
   *
   * @param name XFlow
   * @return
   */
  public XFlow get(String name) {
    return get(name, getDefaultProjectName());
  }

  /**
   * 获取 XFlow 对象
   *
   * @param name Xflow
   * @param projectName
   * @return
   */
  public XFlow get(String name, String projectName) {
    XFlowModel model = new XFlowModel();
    model.setName(name);
    return new XFlow(model, odps.getDefaultProject(), odps);
  }

  /**
   * 判断 XFlow 对象是否存在
   *
   * @param name XFlow
   * @return
   * @throws OdpsException
   */
  public boolean exists(String name) throws OdpsException {
    return exists(name, getDefaultProjectName());
  }

  /**
   * 判断 XFlow 对象是否存在
   *
   * @param name XFlow
   * @param projectName
   * @return
   * @throws OdpsException
   */
  public boolean exists(String name, String projectName) throws OdpsException {
    try {
      XFlow xFlow = get(name);
      xFlow.reload();
    } catch (NoSuchObjectException e) {
      return false;
    }
    return true;
  }

  /**
   * 获得 XFlow 迭代器
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param owner
   *     XFlow owner
   * @return {@link XFlow}迭代器
   */
  public ListIterator<XFlow> iterator(final String projectName, final String owner) {
    return new ListIterator<XFlow>() {
      Map<String, String> params = new HashMap<String, String>();

      @Override
      protected List<XFlow> list() {
        ArrayList<XFlow> xFlows = new ArrayList<XFlow>();

        String lastMarker = params.get("marker");
        if (params.containsKey("marker") && lastMarker.length() == 0) {
          return null;
        }

        if (owner != null) {
          params.put("owner", owner);
        }

        String resource = ResourceBuilder.buildXFlowsResource(projectName);
        try {

          ListXFlowsResponse resp = odps.getRestClient().request(ListXFlowsResponse.class,
                                                                 resource, "GET", params);

          for (XFlowModel model : resp.xFlows) {
            XFlow t = new XFlow(model, projectName, odps);
            xFlows.add(t);
          }

          params.put("marker", resp.marker);
        } catch (OdpsException e) {
          throw new RuntimeException(e.getMessage(), e);
        }

        return xFlows;
      }
    };
  }

  /**
   * 创建 XFlow 对象
   *
   * @param model
   *     xflow 描述 model
   * @throws OdpsException
   */
  public void create(XFlowModel model) throws OdpsException {
    create(model, getDefaultProjectName());
  }

  /**
   * 创建 XFlow 对象
   *
   * @param model
   *     xflow 描述 model
   * @param projectName
   *     所在{@link Project}名称
   * @throws OdpsException
   */
  public void create(XFlowModel model, String projectName) throws OdpsException {
    String resoure = ResourceBuilder.buildXFlowsResource(projectName);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");
    odps.getRestClient().stringRequest(resoure, "POST", null, headers, model.getXmlSource());
  }

  /**
   * 删除 XFlow 对象
   *
   * @param name
   *     XFlow name
   * @throws OdpsException
   */
  public void delete(String name) throws OdpsException {
    delete(name, getDefaultProjectName());
  }

  /**
   * 删除 XFlow 对象
   *
   * @param name
   *     XFlow name
   * @param projectName
   *     所在{@link Project}名称
   * @throws OdpsException
   */
  public void delete(String name, String projectName) throws OdpsException {
    String resoure = ResourceBuilder.buildXFlowResource(projectName, name);
    odps.getRestClient().request(resoure, "DELETE", null, null, null);
  }

  /**
   * 更新 XFlow 对象
   *
   * @param model
   *     xflow 描述 model
   * @throws OdpsException
   */
  public void update(XFlowModel model) throws OdpsException {
    update(model, getDefaultProjectName());
  }

  /**
   * 更新 XFlow 对象
   *
   * @param model
   *     描述 model
   * @param projectName
   *     所在{@link Project}名称
   * @throws OdpsException
   */
  public void update(XFlowModel model, String projectName) throws OdpsException {
    String resoure = ResourceBuilder.buildXFlowResource(projectName, model.name);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");
    odps.getRestClient().stringRequest(resoure, "PUT", null, headers, model.getXmlSource());
  }

  private String getDefaultProjectName() {
    String project = odps.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }

  /**
   * 获得 XFlow 迭代器
   *
   * @return {@link XFlow}迭代器
   */
  @Override
  public Iterator<XFlow> iterator() {
    return iterator(getDefaultProjectName());
  }

  /**
   * 获得 XFlow 迭代器
   *
   * @param projectName
   *     所在{@link Project}名称
   * @return {@link XFlow}迭代器
   */
  public Iterator<XFlow> iterator(String projectName) {
    return iterator(projectName, null);
  }


  @Root(name = "Instance", strict = false)
  static class AnnoymousXFlowInstance {

    @Element(name = "XflowInstance", required = false)
    XFlowInstance xFlowInstance;
  }

  @Root(name = "XflowInstance", strict = false)
  public static class XFlowInstance {
    /**
     * Property
     */
    @Root(strict = false)
    public static class Property {

      @Element(name = "Name")
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      private String name;

      @Element(name = "Value")
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      private String value;

      public String getName() {
        return name;
      }

      public void setName(String name) {
        this.name = name;
      }

      public String getValue() {
        return value;
      }

      public void setValue(String value) {
        this.value = value;
      }
    }

    /**
     * Config
     */
    @Root(name = "Config", strict = false)
    static class Config {
      Set<Property> config = new LinkedHashSet<Property>();

      @ElementList(entry = "Property", inline = true, required = false)
      private List<Property> getConfigList() {
        return new ArrayList<Property>(config);
      }

      @ElementList(entry = "Property", inline = true, required = false)
      private void setConfigList(List<Property> configList) {
        config = new LinkedHashSet<Property>(configList);
      }
    }

    /**
     * Parameter
     */
    @Root(strict = false)
    public static class Parameter {

      @Element(name = "Key")
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      private String key;

      @Element(name = "Value")
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      private String value;

      public String getKey() {
        return key;
      }

      public void setKey(String key) {
        this.key = key;
      }

      public String getValue() {
        return value;
      }

      public void setValue(String value) {
        this.value = value;
      }
    }

    /**
     * Parameters
     */
    @Root(name = "Parameters", strict = false)
    static class Parameters {

      Set<Parameter> parameters = new LinkedHashSet<Parameter>();

      @ElementList(entry = "Parameter", inline = true, required = false)
      private List<Parameter> getParamList() {
        return new ArrayList<Parameter>(parameters);
      }

      @ElementList(entry = "Parameter", inline = true, required = false)
      private void setParamList(List<Parameter> paramList) {
        parameters = new LinkedHashSet<Parameter>(paramList);
      }
    }

    @Element(name = "RunningMode", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String RunningMode;

    @Element(name = "Project", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String project;

    @Element(name = "Xflow", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String XflowName;

    @Element(name = "Parameters", required = false)
    private Parameters parameters = new Parameters();

    @Element(name = "Guid", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String guid;

    // Priority range [0,9], 0 is the highest priority in odps
    @Element(name = "Priority", required = false)
    private int priority = 1;

    @Element(name = "Config", required = false)
    private Config config = new Config();

    public String getXflowName() {
      return XflowName;
    }

    public void setXflowName(String xflowName) {
      XflowName = xflowName;
    }

    public String getRunningMode() {
      return RunningMode;
    }

    public void setRunningMode(String runningMode) {
      RunningMode = runningMode;
    }

    public String getProject() {
      return project;
    }

    public void setProject(String project) {
      this.project = project;
    }

    public Parameters getParameters() {
      return parameters;
    }

    public final void setParameters(Map<String, String> parameters) {
      this.parameters = new Parameters();
      for (Entry<String, String> p : parameters.entrySet()) {
        setParameter(p.getKey(), p.getValue());
      }
    }

    public void setParameter(String key, String value) {
      Parameter p = new Parameter();
      p.key = key;
      p.value = value;
      parameters.parameters.add(p);
    }

    public String getGuid() { return guid; }

    public void setGuid(String guid) { this.guid = guid; }

    public int getPriority() { return priority; }

    /**
     * 设置作业优先级。优先级的取值去见为[0, 9]的整型值，数字越大，优先级越低。
     *
     * @param priority
     *     优先级 (注：公共云环境此参数无效)
     */
    public void setPriority(int priority) { this.priority = priority; }

    public Config getConfig() {
      return config;
    }

    public final void setConfig(Map<String, String> config) {
      this.config = new Config();
      for (Entry<String, String> p : config.entrySet()) {
        setProperty(p.getKey(), p.getValue());
      }
    }

    public void setProperty(String name, String value) {
      Property p = new Property();
      p.setName(name);
      p.setValue(value);
      config.config.add(p);
    }
  }

  public Instance execute(XFlowInstance xFlowInstance) throws OdpsException {
    return execute(xFlowInstance, getDefaultProjectName());
  }

  public Instance execute(XFlowInstance xFlowInstance, String project) throws OdpsException {
    String xml = null;
    if (project == null) {
      throw new IllegalArgumentException("project required.");
    }

    if (xFlowInstance == null) {
      throw new IllegalArgumentException("Job required.");
    }

    AnnoymousXFlowInstance i = new AnnoymousXFlowInstance();
    i.xFlowInstance = xFlowInstance;
    try {
      xml = SimpleXmlUtils.marshal(i);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }
    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");

    String resource = ResourceBuilder.buildInstancesResource(project);
    Response resp = odps.getRestClient().stringRequest(resource, "POST", null, headers, xml);

    String location = resp.getHeaders().get(Headers.LOCATION);
    if (location == null || location.trim().length() == 0) {
      throw new OdpsException("Invalid response, Location header required.");
    }

    String instanceId = location.substring(location.lastIndexOf("/") + 1);

    Map<String, TaskResult> results = new HashMap<>();

    if (resp.getStatus() == 200 && resp.getBody() != null && resp.getBody().length > 0) {
      try {
        InstanceResultModel result = SimpleXmlUtils.unmarshal(resp, InstanceResultModel.class);
        for (TaskResult r : result.taskResults) {
          results.put(r.name, r);
        }
      } catch (Exception e) {
        throw new OdpsException("Invalid create instance response.", e);
      }
    }
    TaskStatusModel model = new TaskStatusModel();
    model.name = instanceId;

    Instance instance = new Instance(getDefaultProjectName(), model, results, odps);
    return instance;
  }

  private static final String CHARSET = "utf-8";

  @Root(strict = false)
  public static class XResult {

    public String getInstanceId() {
      return instanceId;
    }

    public String getName() {
      return name;
    }

    public String getResult() {
      return result.getString();
    }

    public String getNodeType() {
      return nodeType;
    }

    @Attribute(name = "NodeType", required = false)
    String nodeType;

    @Element(name = "InstanceId", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String instanceId;

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Result", required = false)
    Instance.Result result;
  }

  @Root(name = "Xinstance", strict = false)
  private static class XFlowResult {

    private Set<XResult> xresults = new LinkedHashSet<XResult>();

    @ElementList(name = "Actions", entry = "Action", required = false)
    private List<XResult> getXResultList() {
      return new ArrayList<XResult>(xresults);
    }

    @ElementList(name = "Actions", entry = "Action", required = false)
    private void setXResultList(List<XResult> xResultList) {
      xresults = new LinkedHashSet<XResult>(xResultList);
    }

    public Set<XResult> getXResults() {
      return xresults;
    }
  }

  public Map<String, XResult> getXResults(Instance xInstance) throws OdpsException {
    String uri = ResourceBuilder.buildInstanceResource(xInstance.getProject(), xInstance.getId());
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("xresult", null);
    XFlowResult
        xFlowResult =
        odps.getRestClient().request(XFlowResult.class, uri, "GET", params, null, null);
    HashMap<String, XResult> result = new HashMap<String, XResult>();

    for (XResult i : xFlowResult.getXResults()) {
      result.put(i.getName(), i);
    }

    return result;
  }

  public String getXSource(Instance xInstance) throws OdpsException {
    String
        resource =
        ResourceBuilder.buildInstanceResource(xInstance.getProject(), xInstance.getId());
    Map<String, String> params = new HashMap<String, String>();
    params.put("xsource", null);
    Response response = odps.getRestClient().request(resource, "GET", params, null, null);
    try {
      return new String(response.getBody(), CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }

  public XFlowInstance getXFlowInstance(Instance instance) throws OdpsException {
    try {
      AnnoymousXFlowInstance annoymousXFlowInstance =
          SimpleXmlUtils.unmarshal(getXSource(instance).getBytes(), AnnoymousXFlowInstance.class);

      return annoymousXFlowInstance.xFlowInstance;
    } catch (Exception e) {
      throw new OdpsException("Invalid create XFlow instance response.", e);
    }
  }

  public boolean isXFlowInstance(Instance instance) {
    try {
      getXFlowInstance(instance);

      return true;
    } catch (OdpsException e) {
      return false;
    }
  }
}
