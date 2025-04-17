package com.aliyun.odps.type;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public interface NestedTypeInfo extends TypeInfo {
  /**
   * 此方法会将struct类型中，列字段进行转义。适用于拼装 SQL 场景
   * 当 quote = false 时，此方法与 getTypeName() 相同
   */
  String getTypeName(boolean quote);
}
