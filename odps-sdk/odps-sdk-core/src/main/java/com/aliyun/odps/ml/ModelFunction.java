package com.aliyun.odps.ml;

/**
 * ModelFunction表示ODPS中支持的模型算法功能分类
 *
 * @author chao.liu@alibaba-inc.com
 */
public enum ModelFunction {
  AssociationRules,
  Sequence,
  Regression,
  Classification,
  Clustering,
  TimeSeries,
  Mixed
}
