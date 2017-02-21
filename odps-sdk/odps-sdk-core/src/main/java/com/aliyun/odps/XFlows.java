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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import com.aliyun.odps.Instance.InstanceResultModel;
import com.aliyun.odps.Instance.InstanceResultModel.TaskResult;
import com.aliyun.odps.Instance.TaskStatusModel;
import com.aliyun.odps.XFlow.XFlowModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;

public class XFlows implements Iterable<XFlow> {

  @XmlRootElement(name = "xflows")
  private static class ListXFlowsResponse {

    @XmlElement(name = "xflow")
    private List<XFlowModel> xFlows = new ArrayList<XFlowModel>();

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
    private Integer maxItems;
  }

  private Odps odps;

  XFlows(Odps odps) {
    this.odps = odps;
  }

  /**
   * 获取 XFlow 对象
   *
   * @param XFlow
   *     name
   * @return
   */
  public XFlow get(String name) {
    return get(name, getDefaultProjectName());
  }

  /**
   * 获取 XFlow 对象
   *
   * @param XFlow
   *     name
   * @param project
   *     Name
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
   * @param XFlow
   *     name
   * @return
   * @throws OdpsException
   */
  public boolean exists(String name) throws OdpsException {
    return exists(name, getDefaultProjectName());
  }

  /**
   * 判断 XFlow 对象是否存在
   *
   * @param XFlow
   *     name
   * @param project
   *     Name
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
   * @param 创建
   *     xflow 描述 model
   * @throws OdpsException
   */
  public void create(XFlowModel model) throws OdpsException {
    create(model, getDefaultProjectName());
  }

  /**
   * 创建 XFlow 对象
   *
   * @param 创建
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
   * @param 更新
   *     xflow 描述 model
   * @throws OdpsException
   */
  public void update(XFlowModel model) throws OdpsException {
    update(model, getDefaultProjectName());
  }

  /**
   * 更新 XFlow 对象
   *
   * @param XFlow
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


  @XmlRootElement(name = "Instance")
  static class AnnoymousXFlowInstance {

    @XmlElement(name = "XflowInstance")
    XFlowInstance xFlowInstance;
  }

  @XmlRootElement(name = "XflowInstance")
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"RunningMode", "project", "XflowName", "parameters"})
  public static class XFlowInstance {

    /**
     * Parameter
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {"key", "value"})
    public static class Parameter {

      @XmlElement(name = "Key", required = true)
      private String key;

      @XmlElement(name = "Value", required = true)
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
    @XmlRootElement(name = "Parameters")
    @XmlAccessorType(XmlAccessType.FIELD)
    static class Parameters {

      @XmlElement(name = "Parameter")
      Set<Parameter> parameters = new LinkedHashSet<Parameter>();
    }

    @XmlElement(name = "Xflow")
    private String XflowName;

    @XmlElement(name = "RunningMode")
    private String RunningMode;

    @XmlElement(name = "Project")
    private String project;

    @XmlElement(name = "Parameters")
    private Parameters parameters = new Parameters();

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

    @XmlTransient
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
      xml = JAXBUtils.marshal(i, AnnoymousXFlowInstance.class);
    } catch (JAXBException e) {
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

    Map<String, Instance.Result> results = new HashMap<String, Instance.Result>();

    if (resp.getStatus() == 200 && resp.getBody() != null && resp.getBody().length > 0) {
      try {
        InstanceResultModel result = JAXBUtils.unmarshal(resp, InstanceResultModel.class);
        for (TaskResult r : result.taskResults) {
          results.put(r.name, r.result);
        }
      } catch (JAXBException e) {
        throw new OdpsException("Invalid create instance response.", e);
      }
    }
    TaskStatusModel model = new TaskStatusModel();
    model.name = instanceId;

    Instance instance = new Instance(getDefaultProjectName(), model, results, odps);
    return instance;
  }

  private static final String CHARSET = "utf-8";

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

    @XmlAttribute(name = "NodeType")
    String nodeType;

    @XmlElement(name = "InstanceId")
    String instanceId;

    @XmlElement(name = "Name")
    String name;

    @XmlElement(name = "Result")
    Instance.Result result;
  }

  @XmlRootElement(name = "Xinstance")
  private static class XFlowResult {

    @XmlElementWrapper(name = "Actions")
    @XmlElement(name = "Action")
    private Set<XResult> xresults = new LinkedHashSet<XResult>();

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
          JAXBUtils.unmarshal(getXSource(instance).getBytes(), AnnoymousXFlowInstance.class);

      return annoymousXFlowInstance.xFlowInstance;
    } catch (JAXBException e) {
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
