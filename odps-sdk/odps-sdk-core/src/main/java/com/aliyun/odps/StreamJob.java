package com.aliyun.odps;

import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * StreamJobs表示ODPS中所有StreamJob的集合
 *
 * @author zhiyong.dai@alibaba-inc.com
 */
public class StreamJob extends LazyLoad {

  private String project;
  private StreamJobModel model;
  private RestClient client;

  public StreamJob(StreamJobModel model, String projectName, RestClient client) {
    this.project = projectName;
    this.model = model;
    this.client = client;
  }

  /**
   * StreamJob model
   */
  @XmlRootElement(name = "StreamJob")
  static class StreamJobModel {

    @XmlElement(name = "Name")
    String name;

    @XmlElement(name = "Status")
    String status;

    @XmlElement(name = "Owner")
    String owner;

    @XmlElement(name = "CPUBind")
    String cpuBind;

    @XmlElement(name = "WorkerNum")
    String workerNum;

    @XmlElement(name = "CreateTime")
    String createTime;

    @XmlElement(name = "SQL")
    String sql;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildStreamJobResource(project, model.name);
    Response resp = client.request(resource, "GET", null, null, null);
    try {
      model = JAXBUtils.unmarshal(resp, StreamJobModel.class);
    } catch (Exception e) {
      throw new OdpsException("Can't bind xml to " + StreamJobModel.class, e);
    }
    setLoaded(true);
  }

  /**
   * 获取StreamJob名
   *
   * @return StreamJob名称
   */
  public String getName() {
    return model.name;
  }

  public String getStatus() {
    return model.status;
  }

  public int getCPUBind() {
    return Integer.parseInt(model.cpuBind);
  }

  /**
   * 获取StreamJob所属用户
   *
   * @return 所属用户
   */
  public String getOwner() {
    return model.owner;
  }

  public int getWorkerNum() {
    return Integer.parseInt(model.workerNum);
  }

  public Date getCreateTime() {
    Date date = new Date(Long.parseLong(model.createTime) * 1000);
    return date;
  }

}
