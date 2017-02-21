package com.aliyun.odps.ml;

/**
 * OnlineModelFilter用于查询所有模型时根据条件过滤
 */
public class OnlineModelFilter {

  private String name;

  /**
   * 设置模型名前缀
   *
   * @param name
   *     模型名前缀
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * 获得模型名前缀
   *
   * @return 模型名前缀
   */
  public String getName() {
    return this.name;
  }
}
