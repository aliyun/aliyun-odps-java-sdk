package com.aliyun.odps;

import java.util.Date;

import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

/**
 * Stream 对象是ODPS中用于管理增量数据的对象，用户可以通过Stream对象对增量数据进行管理，
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Stream extends LazyLoad {

  private StreamModel model;
  private final RestClient client;

  /**
   * Stream对象对应的xml对象, 通过get stream获得
   */
  @Root(name = "StreamObject", strict = false)
  static class StreamModel {

    @Element(name = "Project")
    String projectName;

    @Element(name = "Name")
    String name;

    @Element(name = "RefTableProject")
    String refTableProject;

    @Element(name = "RefTableName")
    String refTableName;

    @Element(name = "RefTableId", required = false)
    String refTableId;

    @Element(name = "RefTableVersion", required = false)
    String refTableVersion;

    @Element(name = "CreateTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createdTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModifiedTime;

    @Element(name = "version", required = false)
    String version;
  }

  Stream(StreamModel model, String projectName, Odps odps) {
    this.model = model;
    this.model.projectName = projectName;
    this.client = odps.getRestClient();
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildStreamObjectResource(model.projectName, model.name);
    reload(client.request(StreamModel.class, resource, "GET"));
  }

  private void reload(StreamModel model) {
    this.model = model;
    setLoaded(true);
  }


  public String getName() {
    return model.name;
  }

  public String getProjectName() {
    return model.projectName;
  }

  public String getRefTableProject() {
    if (model.refTableProject == null) {
      lazyLoad();
    }
    return model.refTableProject;
  }

  public String getRefTableName() {
    if (model.refTableName == null) {
      lazyLoad();
    }
    return model.refTableName;
  }

  public String getRefTableId() {
    if (model.refTableId == null) {
      lazyLoad();
    }
    return model.refTableId;
  }

  public String getRefTableVersion() {
    if (model.refTableVersion == null) {
      lazyLoad();
    }
    return model.refTableVersion;
  }

  public Date getCreateTime() {
    if (model.createdTime == null) {
      lazyLoad();
    }
    return model.createdTime;
  }

  public Date getLastModifiedTime() {
    if (model.lastModifiedTime == null) {
      lazyLoad();
    }
    return model.lastModifiedTime;
  }

  public String version() {
    if (model.version == null) {
      lazyLoad();
    }
    return model.version;
  }
}
