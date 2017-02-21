package com.aliyun.odps.ml;

/**
 * OnlineStatus表示在线模型所处状态。
 *
 * @author chao.liu@alibaba-inc.com
 */
public enum OnlineStatus {
  /**
   * 正在部署
   */
  DEPLOYING,

  /**
   * 部署失败
   */
  DEPLOYFAILED,

  /**
   * 正在服务
   */
  SERVING,

  /**
   * 正在更新
   */
  UPDATING,

  /**
   * 正在删除
   */
  DELETING,
  /**
   * 删除失败
   */
  DELETEFAILED
}
