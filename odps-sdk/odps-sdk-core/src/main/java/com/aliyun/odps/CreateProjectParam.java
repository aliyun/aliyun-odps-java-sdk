package com.aliyun.odps;


import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.aliyun.odps.utils.StringUtils;

/**
 * 创建项目使用的参数构造类
 *
 */
public class CreateProjectParam {
  private Project.ProjectModel projectModel;

  public CreateProjectParam() {
    this.projectModel = new Project.ProjectModel();
  }

  /**
   * 设置项目名称
   *
   * @param name 项目名称
   * @return 创建项目的参数对象
   */
  public CreateProjectParam name(String name) {
    if (StringUtils.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Project name is required.");
    }

    this.projectModel.name = name;
    return this;
  }

  /**
   * 设置项目 owner
   *
   * @param owner 所有者
   * @return 创建项目的参数对象
   */
  public CreateProjectParam owner(String owner) {
    if (StringUtils.isNullOrEmpty(owner)) {
      throw new IllegalArgumentException("Project owner is required.");
    }

    this.projectModel.owner = owner;
    return this;
  }


  /**
   * 设置项目的 comment
   *
   * @param comment 项目备注
   * @return 创建项目的参数对象
   */
  public CreateProjectParam comment(String comment) {
    this.projectModel.comment = comment;
    return this;
  }

  /**
   * 设置项目的 super admin
   *
   * @param superAdministrator 项目超级管理员
   * @return 创建项目的参数对象
   */
  public CreateProjectParam superAdmin(String superAdministrator) {
    this.projectModel.superAdministrator = superAdministrator;
    return this;
  }

  /**
   * 设置项目的属性集合，覆盖设置
   *
   * @param properties 项目属性集合
   * @return 创建项目的参数对象
   */
  public CreateProjectParam properties(Map<String, String> properties) {
    if (Objects.isNull(this.projectModel.properties)) {
      this.projectModel.properties = new LinkedHashMap<>();
    }

    this.projectModel.properties.putAll(properties);
    return this;
  }

  /**
   * 增加项目属性配置
   *
   * @param key 属性 key
   * @param value 属性 value
   * @return 创建项目的参数对象
   */
  public CreateProjectParam appendProperty(String key, String value) {
    if (Objects.isNull(this.projectModel.properties)) {
      this.projectModel.properties = new LinkedHashMap<>();
    }

    this.projectModel.properties.put(key, value);
    return this;
  }

  /**
   * 设置默认集群
   *
   * @param clusterName  集群名称
   * @return 创建项目的参数对象
   */
  public CreateProjectParam defaultCluster(String clusterName) {
    if (StringUtils.isNullOrEmpty(clusterName)) {
      throw new IllegalArgumentException("Project default cluster is required.");
    }

    this.projectModel.defaultCluster = clusterName;
    return this;
  }

  /**
   * 设置默认计算 quota id 可以为 null, 表示不绑定计算 quota
   *
   * @param quotaId
   * @return 创建项目的参数对象
   */
  public CreateProjectParam defaultQuotaId(String quotaId) {
    this.projectModel.defaultQuotaID = quotaId;
    return this;
  }


  /**
   * 设置 group, 可选
   *
   * @param groupName
   * @return 创建项目的参数对象
   */
  public CreateProjectParam groupName(String groupName) {
    this.projectModel.projectGroupName = groupName;
    return this;
  }

  Project.ProjectModel getProjectModel() {
    return this.projectModel;
  }

}
