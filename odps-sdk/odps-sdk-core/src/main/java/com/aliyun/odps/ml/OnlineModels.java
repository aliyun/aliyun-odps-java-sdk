package com.aliyun.odps.ml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.ListIterator;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.ml.OnlineModel.OnlineModelDesc;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.RestClient;

/**
 * OnlineModels表示ODPS中所有在线模型的集合
 *
 * @author chao.liu@alibaba-inc.com
 */
public class OnlineModels implements Iterable<OnlineModel> {

  @XmlRootElement(name = "Onlinemodels")
  private static class ListOnlineModelsResponse {

    @XmlElement(name = "Onlinemodel")
    private List<OnlineModelDesc> onlineModelDescs = new ArrayList<OnlineModelDesc>();

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
    private Integer maxItems;
  }

  private RestClient client;

  public OnlineModels(RestClient client) {
    this.client = client;
  }

  /**
   * 获得指定的在线模型信息
   *
   * @param modelName
   *     在线模型名
   * @return
   */
  public OnlineModel get(String modelName) {
    return get(getDefaultProjectName(), modelName);
  }

  /**
   * 获得指定模型信息
   *
   * @param projectName
   *     所在Project名称
   * @param modelName
   *     在线模型名
   * @return
   */
  public OnlineModel get(String projectName, String modelName) {
    OnlineModelDesc desc = new OnlineModelDesc();
    desc.project = projectName;
    desc.modelName = modelName;
    return new OnlineModel(desc, client);
  }

  /**
   * 判断指定在线模型是否存在
   *
   * @param modelName
   *     在线模型名
   * @return 存在返回true, 否则返回false
   * @throws OdpsException
   */
  public boolean exists(String modelName) throws OdpsException {
    return exists(getDefaultProjectName(), modelName);
  }

  /**
   * 判断指定在线模型是否存在
   *
   * @param projectName
   *     所在Project名称
   * @param modelName
   *     在线模型名
   * @return 存在返回true, 否则返回flase
   * @throws OdpsException
   */
  public boolean exists(String projectName, String modelName)
      throws OdpsException {
    try {
      OnlineModel m = get(projectName, modelName);
      m.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  /**
   * 获取默认Project的所有模型信息迭代器
   *
   * @return 模型迭代器
   */
  @Override
  public Iterator<OnlineModel> iterator() {
    return iterator(getDefaultProjectName(), null);
  }

  /**
   * 获取在线模型信息迭代器
   *
   * @param projectName
   *     指定Project名称
   * @return 模型迭代器
   */
  public Iterator<OnlineModel> iterator(final String projectName) {
    return iterator(projectName, null);
  }

  /**
   * 获取默认Project的在线模型信息迭代器
   *
   * @param filter
   *     过滤条件
   * @return 在线模型迭代器
   */
  protected Iterator<OnlineModel> iterator(final OnlineModelFilter filter) {
    return iterator(getDefaultProjectName(), filter);
  }

  /**
   * 获得在线模型信息迭代器
   *
   * @param projectName
   *     所在Project名称
   * @param filter
   *     过滤条件
   * @return 在线模型迭代器
   */
  protected Iterator<OnlineModel> iterator(final String projectName,
                                           final OnlineModelFilter filter) {
    return new ListIterator<OnlineModel>() {

      Map<String, String> params = new HashMap<String, String>();

      @Override
      protected List<OnlineModel> list() {
        ArrayList<OnlineModel> models = new ArrayList<OnlineModel>();
        params.put("expectmarker", "true"); // since sprint-11

        String lastMarker = params.get("marker");
        if (params.containsKey("marker")
            && (lastMarker == null || lastMarker.length() == 0)) {
          return null;
        }

        if (filter != null) {
          if (filter.getName() != null) {
            params.put("name", filter.getName());
          }
        }

        String resource = ModelResourceBuilder
            .buildOnlineModelResource(projectName);
        try {
          ListOnlineModelsResponse resp = client.request(
              ListOnlineModelsResponse.class, resource, "GET",
              params);

          for (OnlineModelDesc desc : resp.onlineModelDescs) {
            OnlineModel m = new OnlineModel(desc, client);
            models.add(m);
          }
          params.put("marker", resp.marker);
        } catch (OdpsException e) {
          throw new RuntimeException(e.getMessage(), e);
        }

        return models;
      }
    };
  }

  /**
   * 将训练完成的离线模型发布为在线模型
   *
   * @param modelName
   *     在线模型名
   * @param offlinemodelProject
   *     离线模型项目
   * @param offlinemodelName
   *     离线模型名字
   * @param qos
   *     QOS
   * @return {@link OnlineModel}对象
   * @throws OdpsException
   */
  public OnlineModel create(String modelName, String offlinemodelProject,
                            String offlinemodelName, short qos) throws OdpsException {
    return createInternally(getDefaultProjectName(), modelName, offlinemodelProject,
                  offlinemodelName, qos, "");
  }

	public OnlineModel create(String modelName, String offlinemodelProject, String offlinemodelName, short qos,
			String serviceTag) throws OdpsException {
		return createInternally(getDefaultProjectName(), modelName, offlinemodelProject, offlinemodelName, qos,
				serviceTag);
	}

  /**
   * 将训练完成的离线模型发布为在线模型
   *
   * @param projectName
   *     在线模型所在Project名称
   * @param modelName
   *     在线模型名
   * @param offlinemodelProject
   *     离线模型项目
   * @param offlinemodelName
   *     离线模型名字
   * @param qos
   *     QOS
   * @return {@link OnlineModel}对象
   * @throws OdpsException
   */
  public OnlineModel create(String projectName, String modelName,
                            String offlinemodelProject, String offlinemodelName, short qos)
      throws OdpsException {
    return createInternally(projectName, modelName, offlinemodelProject, offlinemodelName, qos, "");
  }

	public OnlineModel create(String projectName, String modelName,
			String offlinemodelProject, String offlinemodelName,
			short qos, String serviceTag) throws OdpsException {
		return createInternally(projectName, modelName, offlinemodelProject, offlinemodelName, qos, serviceTag);
	}

	public OnlineModel create(OnlineModelInfo modelInfo) throws OdpsException{
		return createInternally(modelInfo);
	}

	public OnlineModel create(OnlineModelInfoNew modelInfo) throws OdpsException{
		return createInternally(modelInfo);
	}

  /**
   * 删除在线模型
   *
   * @param modelName
   *     在线模型名
   * @throws OdpsException
   */
  public void delete(String modelName) throws OdpsException {
    delete(client.getDefaultProject(), modelName);
  }

  /**
   * 删除在线模型
   *
   * @param projectName
   *     在线模型所在Project
   * @param modelName
   *     在线模型名
   * @throws OdpsException
   */
  public void delete(String projectName, String modelName)
      throws OdpsException {
    String resource = ModelResourceBuilder.buildOnlineModelResource(
        projectName, modelName);
    client.request(resource, "DELETE", null, null, null);
  }

  /* private */
  private String getDefaultProjectName() {
    String project = client.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }

	private OnlineModel createInternally(String projectName, String modelName, String offlinemodelProject,
			String offlinemodelName, short qos, String serviceTag) throws OdpsException {
		if (projectName == null || projectName.equals("")) {
			throw new IllegalArgumentException("Project required.");
		}

		if (modelName == null || modelName.equals("")) {
			throw new IllegalArgumentException("ModelName required.");
		}

		if (offlinemodelProject == null || offlinemodelProject.equals("")) {
			throw new IllegalArgumentException("OfflinemodelPrject required.");
		}

		if (offlinemodelName == null || offlinemodelName.equals("")) {
			throw new IllegalArgumentException("OfflinemodelName required.");
		}

		OnlineModelInfo modelInfo = new OnlineModelInfo();
		modelInfo.project = projectName;
		modelInfo.modelName = modelName;
		modelInfo.offlineProject = offlinemodelProject;
		modelInfo.offlineModelName = offlinemodelName;
		modelInfo.QOS = qos;
		modelInfo.serviceTag = serviceTag;

		return createInternally(modelInfo);
	}

	private OnlineModel createInternally(OnlineModelInfo modelInfo) throws OdpsException{
		String xml = null;
		try {
			xml = JAXBUtils.marshal(modelInfo, OnlineModelInfo.class);
		} catch (JAXBException e) {
			throw new OdpsException(e.getMessage(), e);
		}

		HashMap<String, String> headers = new HashMap<String, String>();
		headers.put(Headers.CONTENT_TYPE, "application/xml");

		String resource = ModelResourceBuilder.buildOnlineModelResource(modelInfo.project);
		Response resp = client.stringRequest(resource, "POST", null, headers, xml);

		String location = resp.getHeaders().get(Headers.LOCATION);
		if (location == null || location.trim().length() == 0) {
			throw new OdpsException("Invalid response, Location header required.");
		}

		OnlineModelDesc desc = new OnlineModelDesc();
		desc.project = modelInfo.project;
		desc.modelName = modelInfo.modelName;
		desc.offlinemodelProject = modelInfo.offlineProject;
		desc.offlinemodelName = modelInfo.offlineModelName;
		desc.applyQos = modelInfo.QOS;
		desc.instanceNum = modelInfo.instanceNum;

		return new OnlineModel(desc, client);
	}

	private OnlineModel createInternally(OnlineModelInfoNew modelInfo) throws OdpsException{
		String xml = null;
		try {
			xml = JAXBUtils.marshal(modelInfo, OnlineModelInfoNew.class);
		} catch (JAXBException e) {
			throw new OdpsException(e.getMessage(), e);
		}

		HashMap<String, String> headers = new HashMap<String, String>();
		headers.put(Headers.CONTENT_TYPE, "application/xml");

		String resource = ModelResourceBuilder.buildOnlineModelResource(modelInfo.project);
		Response resp = client.stringRequest(resource, "POST", null, headers, xml);

		String location = resp.getHeaders().get(Headers.LOCATION);
		if (location == null || location.trim().length() == 0) {
			throw new OdpsException("Invalid response, Location header required.");
		}

		OnlineModelDesc desc = new OnlineModelDesc();
		desc.project = modelInfo.project;
		desc.modelName = modelInfo.modelName;
		desc.applyQos = modelInfo.QOS;
		desc.instanceNum = modelInfo.instanceNum;

		return new OnlineModel(desc, client);
	}
}
