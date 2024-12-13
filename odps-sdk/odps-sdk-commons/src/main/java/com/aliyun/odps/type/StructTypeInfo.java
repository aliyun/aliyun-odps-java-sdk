package com.aliyun.odps.type;

import java.util.List;

/**
 * Odps struct 接口
 *
 * ODPS 用户请不要自行扩展或实现此接口，否则在未来 ODPS 扩展此接口时会导致错误
 *
 * Created by zhenhong.gzh on 16/7/8.
 */
public interface StructTypeInfo extends TypeInfo {
  List<String> getFieldNames();
  List<TypeInfo> getFieldTypeInfos();
  int getFieldCount();

  /**
   * 此方法会将struct类型中，列字段进行转义。适用于拼装 SQL 场景
   * 当 quote = false 时，此方法与 getTypeName() 相同
   */
  String getTypeName(boolean quote);
}
